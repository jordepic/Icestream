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
import io.github.jordepic.icestream.flinkpaimon.index.IndexTableSchema;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;

/**
 * Flink + Paimon implementation of {@link DeleteConverter}.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Build {@link EqDeleteWorkItem}s on the master JVM, derive {@code touchedPartitions}, and
 *       enumerate the snapshot's delete-manifest references via
 *       {@link ExistingPerFileDeleteLoader#deleteManifests}. Manifest <em>scan</em> is deferred to
 *       per-task work via {@link ScanDeleteManifestsFlatMap} so the master JVM only does cheap
 *       metadata reads.
 *   <li>Build a fresh Flink batch env with two parallel sources:
 *       <ul>
 *         <li>Eq-delete work items → {@link EqDeleteSourceFlatMap} → probe rows
 *             {@code (spec_id, partition_key, pk)} → registered as a temporary view → SQL
 *             {@code LEFT JOIN ... FOR SYSTEM_TIME AS OF} drives the Paimon lookup join, giving
 *             per-row indexed probes via {@code FileStoreLookupFunction}. Match rows carry
 *             {@code (spec_id, partition_key, data_file_path, pos)}.
 *         <li>Delete manifests → {@link ScanDeleteManifestsFlatMap} → existing-deletes tuples
 *             {@code (data_file_path, ExistingPerFileDeletes)}.
 *       </ul>
 *   <li>Both streams {@code keyBy(data_file_path)}-ed, {@link DataStream#connect} into a two-input
 *       {@link WriteDeleteFilesOperator}. Each task slot owns one
 *       {@link io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter} for the
 *       data files keyed to it; existing-deletes and matches for the same path are
 *       co-partitioned, so registration and writes always land on the same writer.
 *   <li>Collect the {@link TaskOutputs} stream (one record per task slot) on the master JVM and
 *       hand it to {@link DeleteFileCreatorSupport#assemblePlan} for the {@link CommitPlan}.
 * </ol>
 */
public final class FlinkDeleteFileCreator implements DeleteConverter {

    private static final String PAIMON_CATALOG_NAME = "paimon";
    private static final String EQ_DELETES_VIEW = "icestream_eq_deletes";

    private final FlinkContext flink;
    private final PaimonIndex paimonIndex;

    public FlinkDeleteFileCreator(FlinkContext flink, PaimonIndex paimonIndex) {
        this.flink = flink;
        this.paimonIndex = paimonIndex;
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
        registerPaimonCatalog(tEnv);

        org.apache.iceberg.Schema pkSchema = new org.apache.iceberg.Schema(buildPkFields(icebergTable));
        StructType pkStruct = StructType.of(buildPkFields(icebergTable));

        TypeInformation<Row> probeTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
                new String[] {"spec_id", "partition_key", "pk"});
        DataStream<Row> probes = env.fromCollection(workItems, TypeInformation.of(EqDeleteWorkItem.class))
                .flatMap(new EqDeleteSourceFlatMap(icebergTable, pkSchema, pkStruct))
                .returns(probeTypeInfo);

        Schema probeSchema = Schema.newBuilder()
                .column("spec_id", DataTypes.INT())
                .column("partition_key", DataTypes.STRING())
                .column("pk", DataTypes.STRING())
                .columnByExpression("proc", "PROCTIME()")
                .build();
        tEnv.createTemporaryView(EQ_DELETES_VIEW, tEnv.fromDataStream(probes, probeSchema));

        String sql = buildLookupJoinSql(qualifiedIndexFqn(id));
        DataStream<Row> matches = tEnv.toDataStream(tEnv.sqlQuery(sql));

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

    /**
     * SQL the converter runs against its temporary view to drive the lookup join. Package-private
     * so tests can call {@code tEnv.sqlQuery(...).explain()} on this and assert the planner
     * picked Paimon's {@code FileStoreLookupFunction} (a {@code LookupJoin} operator) rather
     * than a regular hash/sort-merge join — which would silently lose the indexed-probe semantics
     * that are the whole point of moving from Spark+Cassandra to Flink+Paimon.
     */
    static String buildLookupJoinSql(String indexFqn) {
        return "SELECT eq.spec_id, eq.partition_key, idx." + IndexTableSchema.COL_DATA_FILE_PATH
                + ", idx." + IndexTableSchema.COL_POS
                + " FROM " + EQ_DELETES_VIEW + " AS eq"
                + " LEFT JOIN " + indexFqn + " FOR SYSTEM_TIME AS OF eq.proc AS idx"
                + " ON eq." + IndexTableSchema.COL_SPEC_ID + " = idx." + IndexTableSchema.COL_SPEC_ID
                + " AND eq." + IndexTableSchema.COL_PARTITION_KEY + " = idx." + IndexTableSchema.COL_PARTITION_KEY
                + " AND eq." + IndexTableSchema.COL_PK + " = idx." + IndexTableSchema.COL_PK
                + " WHERE idx." + IndexTableSchema.COL_DATA_FILE_PATH + " IS NOT NULL";
    }

    String qualifiedIndexFqn(TableIdentifier id) {
        return String.format(
                "`%s`.`%s`.`%s`",
                PAIMON_CATALOG_NAME, paimonIndex.database(), paimonIndex.tableName(id));
    }

    private void registerPaimonCatalog(StreamTableEnvironment tEnv) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "paimon");
        options.putAll(paimonIndex.catalogOptionsForFlink());
        StringBuilder withClause = new StringBuilder();
        options.forEach((k, v) -> withClause.append("'").append(k).append("'='").append(v).append("',"));
        if (withClause.length() > 0) {
            withClause.setLength(withClause.length() - 1);
        }
        tEnv.executeSql("CREATE CATALOG " + PAIMON_CATALOG_NAME + " WITH (" + withClause + ")");
    }

    private static List<org.apache.iceberg.types.Types.NestedField> buildPkFields(Table table) {
        return IcestreamTableConfig.from(table).primaryKey().fields();
    }
}
