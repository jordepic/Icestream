package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.flinkpaimon.converter.EqDeleteSourceFlatMap;
import io.github.jordepic.icestream.flinkpaimon.converter.PaimonBucketKeySelector;
import io.github.jordepic.icestream.flinkpaimon.converter.PaimonLookupJoin;
import io.github.jordepic.icestream.flinkpaimon.converter.StreamingWriteDeleteFilesOperator;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.flinkpaimon.indexer.DataFileFlatMap;
import io.github.jordepic.icestream.indexer.FileWorkItem;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

/**
 * Builds and submits the one autonomous, long-lived streaming job per iceberg table that replaces the
 * master loop + RPC channel. A single parallelism-1 {@link IcestreamWalkSource} walks the planner and
 * emits one {@link WorkUnit} per checkpoint epoch; the job then routes it:
 *
 * <ul>
 *   <li><b>DATA</b> → {@link WorkUnitToDataFiles} → {@code rebalance} → {@link DataFileFlatMap} →
 *       Paimon index sink (one bucket-keyed PK upsert per row). Keeps the index writer warm.
 *   <li><b>EQ_DEL</b> → {@link WorkUnitToEqDeleteFiles} → {@code rebalance} →
 *       {@link EqDeleteSourceFlatMap} → bucket {@code keyBy} → warm Paimon lookup join → matches
 *       {@code keyBy(data_file_path)}, co-keyed with {@link WorkUnitToExistingDeletes}, into the
 *       distributed {@link StreamingWriteDeleteFilesOperator}. Keeps the lookup cache warm.
 *   <li>both → {@link RunCommitter} (p1), which commits each run and advances the iceberg watermark
 *       that paces the source.
 * </ul>
 *
 * <p>The whole control plane (plan, gate, commit, watermark) lives in the source and committer; there
 * is no separate driver and no channel. Correctness rests on the single sequential source (strict
 * {@code (seq, kind)} ordering) + at-least-once + idempotent commit.
 */
public final class IcestreamTableJob {

    private IcestreamTableJob() {}

    public static JobClient submit(
            FlinkContext flink,
            PaimonIndex paimonIndex,
            IcebergCatalogSpec catalogSpec,
            PaimonIndexSpec indexSpec,
            TableIdentifier id,
            Table table,
            long checkpointIntervalMs,
            long pollIntervalMs,
            long idleBackoffMs,
            long paimonCommitTimeoutMs) {
        int formatVersion = DeleteFileCreatorSupport.requireSupportedFormatVersion(table);
        SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);

        var pkFields = PaimonLookupJoin.pkFields(table);
        Schema pkSchema = new Schema(pkFields);
        StructType pkStruct = StructType.of(pkFields);
        FileStoreTable indexTable = (FileStoreTable) paimonIndex.load(id);
        TableSchema indexSchema = indexTable.schema();

        StreamExecutionEnvironment env = flink.newStreamEnv();
        env.enableCheckpointing(checkpointIntervalMs);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        PaimonLookupJoin.registerCatalog(tEnv, paimonIndex);

        DataStream<WorkUnit> units = env.addSource(
                        new IcestreamWalkSource(catalogSpec, id, formatVersion, pollIntervalMs, idleBackoffMs))
                .returns(TypeInformation.of(WorkUnit.class))
                .setParallelism(1);

        // DATA branch: read data files distributed and upsert the Paimon PK index (warm writer).
        RowType flinkRowType = LogicalTypeConversion.toLogicalType(indexTable.rowType());
        TypeInformation<RowData> rowDataTypeInfo = InternalTypeInfo.of(flinkRowType);
        DataStream<RowData> indexRows = units.flatMap(new WorkUnitToDataFiles())
                .returns(TypeInformation.of(FileWorkItem.class))
                .setParallelism(1)
                .rebalance()
                .flatMap(new DataFileFlatMap(table, pkSchema, pkStruct))
                .returns(rowDataTypeInfo);
        new FlinkSinkBuilder(indexTable).forRowData(indexRows).build();

        // EQ_DEL branch: distributed eq-delete reads → bucket-local warm lookup join → matches.
        DataStream<EqDeleteWorkItem> eqFiles = units.flatMap(new WorkUnitToEqDeleteFiles())
                .returns(TypeInformation.of(EqDeleteWorkItem.class))
                .setParallelism(1)
                .rebalance();
        DataStream<Row> probes = eqFiles.flatMap(new EqDeleteSourceFlatMap(table, pkSchema, pkStruct))
                .returns(PaimonLookupJoin.PROBE_TYPE_INFO);
        DataStream<Row> bucketed = probes.keyBy(new PaimonBucketKeySelector(indexSchema));
        DataStream<Row> matches = PaimonLookupJoin.lookupMatches(tEnv, bucketed, paimonIndex, id);

        TypeInformation<Tuple2<String, ExistingPerFileDeletes>> existingTypeInfo =
                TypeInformation.of(new TypeHint<>() {});
        DataStream<Tuple2<String, ExistingPerFileDeletes>> existingDeletes = units.flatMap(
                        new WorkUnitToExistingDeletes())
                .returns(existingTypeInfo)
                .setParallelism(1);

        // Distribute the write by data_file_path (a positional delete file belongs to one data file).
        DataStream<TaskOutputs> writerOutputs = existingDeletes
                .keyBy(entry -> entry.f0)
                .connect(matches.keyBy(row -> (String) row.getField(2)))
                .transform(
                        "WriteDeleteFiles",
                        TypeInformation.of(TaskOutputs.class),
                        new StreamingWriteDeleteFilesOperator(serializableTable, formatVersion));

        // Commit + advance watermark in one p1 operator; it sees the control unit and all writer slices.
        units.connect(writerOutputs)
                .transform(
                        "Commit",
                        Types.VOID,
                        new RunCommitter(catalogSpec, id, indexSpec, paimonCommitTimeoutMs))
                .setParallelism(1)
                .sinkTo(new DiscardingSink<>());

        try {
            return env.executeAsync("icestream-" + id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to submit icestream job for " + id, e);
        }
    }
}
