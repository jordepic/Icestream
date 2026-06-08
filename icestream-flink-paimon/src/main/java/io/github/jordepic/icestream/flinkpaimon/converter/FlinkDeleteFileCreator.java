package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;

/**
 * Batch Flink + Paimon {@link DeleteConverter}: a fresh batch job per conversion (the reference /
 * benchmark-control implementation; production uses {@link StreamingFlinkDeleteFileCreator}).
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Build {@link EqDeleteWorkItem}s on the master JVM, derive {@code touchedPartitions}, and
 *       enumerate the snapshot's delete-manifest references via
 *       {@link ExistingPerFileDeleteLoader#deleteManifests}. Manifest <em>scan</em> is deferred to
 *       per-task work via {@link ScanDeleteManifestsFlatMap} so the master JVM only does cheap
 *       metadata reads.
 *   <li>Build a fresh Flink batch env with two parallel sources: eq-delete work items →
 *       {@link EqDeleteSourceFlatMap} → probe rows → {@link PaimonLookupJoin} match rows
 *       {@code (spec_id, partition_key, data_file_path, pos)}; and delete manifests →
 *       {@link ScanDeleteManifestsFlatMap} → existing-deletes tuples.
 *   <li>Both streams {@code keyBy(data_file_path)}-ed, {@link DataStream#connect} into a two-input
 *       {@link WriteDeleteFilesOperator}, so existing-deletes and matches for the same path land on
 *       the same writer.
 *   <li>Collect the {@link TaskOutputs} on the master JVM and hand it to
 *       {@link DeleteFileCreatorSupport#assemblePlan} for the {@link CommitPlan}.
 * </ol>
 */
public final class FlinkDeleteFileCreator implements DeleteConverter {

    private final FlinkContext flink;
    private final PaimonIndex paimonIndex;
    private final boolean useLookupJoin;

    public FlinkDeleteFileCreator(FlinkContext flink, PaimonIndex paimonIndex) {
        this(flink, paimonIndex, true);
    }

    /**
     * @param useLookupJoin when {@code true} (the default) the probe is driven by a
     *     {@code FOR SYSTEM_TIME AS OF} temporal join → Paimon's {@code FileStoreLookupFunction}
     *     (indexed point lookups). {@code false} drops the hint for a full-scan regular join — a
     *     benchmark control to isolate the indexed-lookup win; never used in production.
     */
    public FlinkDeleteFileCreator(FlinkContext flink, PaimonIndex paimonIndex, boolean useLookupJoin) {
        this.flink = flink;
        this.paimonIndex = paimonIndex;
        this.useLookupJoin = useLookupJoin;
    }

    @Override
    public CommitPlan create(TableIdentifier id, Table table, EqualityDeleteFileRun run, IcestreamTableConfig config) {
        long startingSnapshotId = table.currentSnapshot().snapshotId();
        if (run.files().isEmpty()) {
            return new CommitPlan(startingSnapshotId, List.of(), List.of(), List.of());
        }
        List<EqDeleteWorkItem> workItems = DeleteFileCreatorSupport.buildWorkItems(table, run);
        Set<PartitionKey> touchedPartitions = DeleteFileCreatorSupport.touchedPartitions(workItems);
        int formatVersion = DeleteFileCreatorSupport.requireSupportedFormatVersion(table);
        List<ManifestFile> deleteManifests = ExistingPerFileDeleteLoader.deleteManifests(table);

        List<TaskOutputs> taskOutputs =
                runJob(id, table, workItems, formatVersion, touchedPartitions, deleteManifests);
        return DeleteFileCreatorSupport.assemblePlan(startingSnapshotId, run, taskOutputs);
    }

    private List<TaskOutputs> runJob(
            TableIdentifier id,
            Table icebergTable,
            List<EqDeleteWorkItem> workItems,
            int formatVersion,
            Set<PartitionKey> touchedPartitions,
            List<ManifestFile> deleteManifests) {
        StreamExecutionEnvironment env = flink.newBatchEnv();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        PaimonLookupJoin.registerCatalog(tEnv, paimonIndex);

        var pkFields = PaimonLookupJoin.pkFields(icebergTable);
        org.apache.iceberg.Schema pkSchema = new org.apache.iceberg.Schema(pkFields);
        StructType pkStruct = StructType.of(pkFields);

        DataStream<Row> probes = env.fromCollection(workItems, TypeInformation.of(EqDeleteWorkItem.class))
                .flatMap(new EqDeleteSourceFlatMap(icebergTable, pkSchema, pkStruct))
                .returns(PaimonLookupJoin.PROBE_TYPE_INFO);
        DataStream<Row> matches = PaimonLookupJoin.lookupMatches(tEnv, probes, paimonIndex, id, useLookupJoin);

        TypeInformation<Tuple2<String, ExistingPerFileDeletes>> existingTypeInfo =
                TypeInformation.of(new TypeHint<>() {});
        DataStream<Tuple2<String, ExistingPerFileDeletes>> existingDeletes = env.fromCollection(
                        deleteManifests, TypeInformation.of(ManifestFile.class))
                .flatMap(new ScanDeleteManifestsFlatMap(icebergTable, formatVersion, touchedPartitions))
                .returns(existingTypeInfo);

        SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(icebergTable);
        WriteDeleteFilesOperator operator = new WriteDeleteFilesOperator(serializableTable, formatVersion);

        DataStream<TaskOutputs> taskOutputsStream = existingDeletes
                .keyBy(entry -> entry.f0)
                .connect(matches.keyBy(row -> (String) row.getField(2)))
                .transform("WriteDeleteFiles", TypeInformation.of(TaskOutputs.class), operator);

        List<TaskOutputs> collected = new ArrayList<>();
        try (CloseableIterator<TaskOutputs> iter = taskOutputsStream.executeAndCollect()) {
            while (iter.hasNext()) {
                collected.add(iter.next());
            }
        } catch (Exception e) {
            throw new RuntimeException("Flink converter job failed for " + id, e);
        }
        return collected;
    }
}
