package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.flinkpaimon.channel.CollectConversionOutputsOperator;
import io.github.jordepic.icestream.flinkpaimon.channel.ConversionChannelServer;
import io.github.jordepic.icestream.flinkpaimon.channel.ConversionRequest;
import io.github.jordepic.icestream.flinkpaimon.channel.ConversionRequestSource;
import io.github.jordepic.icestream.flinkpaimon.channel.InProcessConversionChannel;
import io.github.jordepic.icestream.flinkpaimon.channel.RemoteConversionChannelClient;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

/**
 * Streaming {@link DeleteConverter}: keeps one long-lived Flink job per iceberg table so the Paimon
 * lookup operator's {@code LocalTableQuery} cache stays warm across conversions — the per-bucket SST
 * download is paid once, not per conversion (a ~5–25x win; see STREAMING_CONVERTER.md). The join
 * runs distributed across the job's slots; the master commits per conversion.
 *
 * <p>{@code create()} submits the conversion's work items to the standing job via
 * {@link InProcessConversionChannel} and blocks for that conversion's {@link TaskOutputs} (delivered
 * at the next checkpoint barrier).
 */
public final class StreamingFlinkDeleteFileCreator implements DeleteConverter, AutoCloseable {

    private final FlinkContext flink;
    private final PaimonIndex paimonIndex;
    private final long checkpointIntervalMs;
    private final long resultTimeoutMs;
    // The driver hosts the channel server; the job's operators (in an in-JVM MiniCluster or on a
    // remote TaskManager) dial it over HTTP at this base URL. There is one transport — the in-JVM
    // case is simply loopback, so it exercises the same code the remote deployment uses.
    private final ConversionChannelServer channelServer;
    private final String channelBaseUrl;
    private final AtomicLong conversionIdSeq = new AtomicLong();
    private final Map<TableIdentifier, RunningJob> jobs = new ConcurrentHashMap<>();

    /** Local/test: bind the channel server on an ephemeral loopback port. */
    public StreamingFlinkDeleteFileCreator(
            FlinkContext flink, PaimonIndex paimonIndex, long checkpointIntervalMs, long resultTimeoutMs) {
        this(flink, paimonIndex, checkpointIntervalMs, resultTimeoutMs, 0, "localhost");
    }

    /**
     * @param channelPort the port the driver's {@link ConversionChannelServer} binds (0 = ephemeral).
     * @param advertisedHost the host the job's operators dial back to — {@code localhost} for an
     *     in-pod MiniCluster, or a routable host/Service DNS when the job runs on a remote cluster.
     */
    public StreamingFlinkDeleteFileCreator(
            FlinkContext flink,
            PaimonIndex paimonIndex,
            long checkpointIntervalMs,
            long resultTimeoutMs,
            int channelPort,
            String advertisedHost) {
        this.flink = flink;
        this.paimonIndex = paimonIndex;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.resultTimeoutMs = resultTimeoutMs;
        try {
            this.channelServer = new ConversionChannelServer(channelPort).start();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start conversion channel server", e);
        }
        this.channelBaseUrl = "http://" + advertisedHost + ":" + channelServer.port();
    }

    @Override
    public CommitPlan create(TableIdentifier id, Table table, EqualityDeleteFileRun run, IcestreamTableConfig config) {
        long startingSnapshotId = table.currentSnapshot().snapshotId();
        if (run.files().isEmpty()) {
            return new CommitPlan(startingSnapshotId, List.of(), List.of(), List.of());
        }
        RunningJob job = jobs.computeIfAbsent(id, k -> startJob(id, table));
        List<EqDeleteWorkItem> workItems = DeleteFileCreatorSupport.buildWorkItems(table, run);
        // The job merges each affected data file's existing deletes into the new file; enumerate the
        // snapshot's delete manifests here (cheap metadata) and ship the refs — the job scans them.
        List<ManifestFile> deleteManifests = ExistingPerFileDeleteLoader.deleteManifests(table);
        long conversionId = conversionIdSeq.incrementAndGet();
        List<TaskOutputs> outputs;
        try {
            outputs = InProcessConversionChannel.submit(
                            job.jobKey, new ConversionRequest(conversionId, workItems, deleteManifests))
                    .get(resultTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Streaming conversion failed for " + id, e);
        }
        return DeleteFileCreatorSupport.assemblePlan(startingSnapshotId, run, outputs);
    }

    private RunningJob startJob(TableIdentifier id, Table table) {
        String jobKey = id + "-" + System.nanoTime();
        InProcessConversionChannel.register(jobKey);
        // Channel state always lives here in the driver JVM (register/submit/await above and below);
        // the operators reach it over HTTP via the driver's channel server (loopback in-JVM, network
        // on a remote TaskManager).
        RemoteConversionChannelClient channelClient = new RemoteConversionChannelClient(channelBaseUrl);
        int formatVersion = DeleteFileCreatorSupport.requireSupportedFormatVersion(table);

        StreamExecutionEnvironment env = flink.newStreamEnv();
        env.enableCheckpointing(checkpointIntervalMs);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        PaimonLookupJoin.registerCatalog(tEnv, paimonIndex);

        var pkFields = PaimonLookupJoin.pkFields(table);
        org.apache.iceberg.Schema pkSchema = new org.apache.iceberg.Schema(pkFields);
        StructType pkStruct = StructType.of(pkFields);

        DataStream<ConversionRequest> requests = env.addSource(new ConversionRequestSource(jobKey, channelClient))
                .returns(TypeInformation.of(ConversionRequest.class))
                .setParallelism(1);
        // Split the request into its eq-delete files on the p1 source side (lightweight, no I/O, so
        // the whole request stays in one checkpoint epoch), then rebalance so the file READS spread
        // across reader subtasks (one file per record).
        DataStream<EqDeleteWorkItem> files = requests.flatMap(new RequestToEqDeleteFiles())
                .returns(TypeInformation.of(EqDeleteWorkItem.class))
                .setParallelism(1)
                .rebalance();
        DataStream<Row> probes = files.flatMap(new EqDeleteSourceFlatMap(table, pkSchema, pkStruct))
                .returns(PaimonLookupJoin.PROBE_TYPE_INFO); // job parallelism: distributed reads feed the lookup join

        // Partition the lookup input by the probe's Paimon bucket (computed by Paimon's own extractor
        // over the index schema), so each lookup subtask owns a disjoint slice of buckets and caches
        // only its slice — instead of every subtask caching every bucket it sees under round-robin.
        TableSchema indexSchema = ((FileStoreTable) paimonIndex.load(id)).schema();
        DataStream<Row> bucketed = probes.keyBy(new PaimonBucketKeySelector(indexSchema));
        DataStream<Row> matches = PaimonLookupJoin.lookupMatches(tEnv, bucketed, paimonIndex, id);

        // Existing-deletes branch: scan the conversion's delete manifests (p1, chained to the source)
        // into (data_file_path, ExistingPerFileDeletes), co-keyed with the matches by data_file_path
        // so the writer merges each affected data file's prior DV / pos-delete into the new file.
        TypeInformation<Tuple2<String, ExistingPerFileDeletes>> existingTypeInfo =
                TypeInformation.of(new TypeHint<>() {});
        DataStream<Tuple2<String, ExistingPerFileDeletes>> existingDeletes = requests
                .flatMap(new RequestToExistingDeletes(table, formatVersion))
                .returns(existingTypeInfo)
                .setParallelism(1);

        SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        // Distribute the WRITE: key both inputs by data_file_path so each writer subtask owns disjoint
        // data files (a positional delete file belongs to exactly one data file → no collisions). The
        // parallelism-1 collector then aggregates all writer subtasks' outputs per conversion (via
        // checkpoint-barrier alignment) and replies to the caller.
        existingDeletes.keyBy(entry -> entry.f0)
                .connect(matches.keyBy(row -> (String) row.getField(2)))
                .transform(
                        "WriteDeleteFiles",
                        TypeInformation.of(TaskOutputs.class),
                        new StreamingWriteDeleteFilesOperator(serializableTable, formatVersion))
                .transform(
                        "CollectOutputs",
                        TypeInformation.of(TaskOutputs.class),
                        new CollectConversionOutputsOperator(jobKey, channelClient))
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>());

        JobClient client;
        try {
            client = env.executeAsync("icestream-streaming-converter:" + id);
        } catch (Exception e) {
            InProcessConversionChannel.unregister(jobKey);
            throw new RuntimeException("Failed to start streaming converter job for " + id, e);
        }
        return new RunningJob(jobKey, client);
    }

    @Override
    public void close() {
        jobs.values().forEach(job -> {
            try {
                job.client.cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // best-effort
            }
            InProcessConversionChannel.unregister(job.jobKey);
        });
        jobs.clear();
        channelServer.close();
    }

    private record RunningJob(String jobKey, JobClient client) {}
}
