package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.converter.DeletionVectorLoader;
import io.github.jordepic.icestream.converter.DvInfo;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.ExistingPosDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.PerTaskDvWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskPosDeleteWriter;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.IndexTableSchema;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * Flink + Paimon implementation of {@link DeleteConverter}.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>Build {@link EqDeleteWorkItem}s on the master JVM and pre-collect existing per-data-file
 *       scoped delete files in the touched partitions ({@link DeletionVectorLoader} for V3,
 *       {@link ExistingPosDeleteLoader} for V2).
 *   <li>Build a fresh Flink batch env, source the work items, flatMap through
 *       {@link EqDeleteSourceFlatMap} into {@code (spec_id, partition_key, pk)} probe rows.
 *   <li>Convert the probe stream into a Table with a {@code PROCTIME} column, register the
 *       Paimon catalog, and run a SQL {@code LEFT JOIN ... FOR SYSTEM_TIME AS OF} against the
 *       index table — Flink's planner picks Paimon's {@code FileStoreLookupFunction}, giving
 *       per-row indexed probes.
 *   <li>Collect matches into the master JVM via {@code executeAndCollect}, group by
 *       {@code data_file_path}, and write per-data-file delete files via
 *       {@link PerTaskDvWriter} / {@link PerTaskPosDeleteWriter} on the driver.
 *   <li>Assemble the {@link CommitPlan} that the {@code TableProcessor} hands to the
 *       conversion committer.
 * </ol>
 *
 * <p>Driver-side write: matches travel through the master JVM rather than being written on Flink
 * TaskManagers. Acceptable because the matches RDD is small relative to the index — eq-delete
 * counts per fileRun are bounded by what the writer emits per checkpoint window. Future
 * optimization: keyed Flink operator wrapping {@link PerTaskDvWriter} for parallel write.
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
        List<EqDeleteWorkItem> workItems = buildWorkItems(table, run);
        Set<PartitionKey> touchedPartitions = workItems.stream()
                .map(item -> new PartitionKey(item.specId(), item.partitionBytes()))
                .collect(Collectors.toSet());
        int formatVersion =
                ((HasTableOperations) table).operations().current().formatVersion();

        Map<String, List<PerPositionMatch>> matchesByDataFile = runLookupJoin(id, table, workItems);
        if (matchesByDataFile.isEmpty()) {
            return new CommitPlan(startingSnapshotId, new ArrayList<>(run.files()), List.of(), List.of());
        }
        List<TaskOutputs> taskOutputs =
                writeDeleteFiles(table, formatVersion, matchesByDataFile, touchedPartitions);
        return assemblePlan(startingSnapshotId, run, taskOutputs);
    }

    private Map<String, List<PerPositionMatch>> runLookupJoin(
            TableIdentifier id, Table icebergTable, List<EqDeleteWorkItem> workItems) {
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

        String indexFqn = String.format(
                "`%s`.`%s`.`%s`",
                PAIMON_CATALOG_NAME, paimonIndex.database(), paimonIndex.tableName(id));
        String sql = "SELECT eq.spec_id, eq.partition_key, idx." + IndexTableSchema.COL_DATA_FILE_PATH
                + ", idx." + IndexTableSchema.COL_POS
                + " FROM " + EQ_DELETES_VIEW + " AS eq"
                + " LEFT JOIN " + indexFqn + " FOR SYSTEM_TIME AS OF eq.proc AS idx"
                + " ON eq." + IndexTableSchema.COL_SPEC_ID + " = idx." + IndexTableSchema.COL_SPEC_ID
                + " AND eq." + IndexTableSchema.COL_PARTITION_KEY + " = idx." + IndexTableSchema.COL_PARTITION_KEY
                + " AND eq." + IndexTableSchema.COL_PK + " = idx." + IndexTableSchema.COL_PK
                + " WHERE idx." + IndexTableSchema.COL_DATA_FILE_PATH + " IS NOT NULL";

        Map<String, List<PerPositionMatch>> matchesByDataFile = new LinkedHashMap<>();
        TableResult result = tEnv.sqlQuery(sql).execute();
        try (CloseableIterator<Row> iter = result.collect()) {
            while (iter.hasNext()) {
                Row row = iter.next();
                int specId = (Integer) row.getField(0);
                byte[] partitionBytes = IndexEncoding.fromHex((String) row.getField(1));
                String dataFilePath = (String) row.getField(2);
                long pos = (Long) row.getField(3);
                matchesByDataFile
                        .computeIfAbsent(dataFilePath, k -> new ArrayList<>())
                        .add(new PerPositionMatch(pos, specId, partitionBytes));
            }
        } catch (Exception e) {
            throw new RuntimeException("Flink converter lookup-join failed for " + id, e);
        }
        return matchesByDataFile;
    }

    private List<TaskOutputs> writeDeleteFiles(
            Table table,
            int formatVersion,
            Map<String, List<PerPositionMatch>> matchesByDataFile,
            Set<PartitionKey> touchedPartitions) {
        SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        return switch (formatVersion) {
            case 2 -> writeV2(serializableTable, table, matchesByDataFile, touchedPartitions);
            case 3 -> writeV3(serializableTable, table, matchesByDataFile, touchedPartitions);
            default -> throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        };
    }

    private List<TaskOutputs> writeV3(
            SerializableTable serializableTable,
            Table table,
            Map<String, List<PerPositionMatch>> matchesByDataFile,
            Set<PartitionKey> touchedPartitions) {
        DeletionVectorLoader.CollectedDvs existing = DeletionVectorLoader.collect(table, touchedPartitions);
        Map<String, DvInfo> existingByPath = existing.serializableDeletesByDataFilePath();
        try (PerTaskDvWriter writer = new PerTaskDvWriter(serializableTable, 0, 0L)) {
            existingByPath.forEach(writer::registerExistingDv);
            for (Map.Entry<String, List<PerPositionMatch>> entry : matchesByDataFile.entrySet()) {
                for (PerPositionMatch match : entry.getValue()) {
                    writer.delete(entry.getKey(), match.pos(), match.specId(), match.partitionBytes());
                }
            }
            return List.of(writer.finishAndClose());
        } catch (IOException e) {
            throw new RuntimeException("Failed writing DV files", e);
        }
    }

    private List<TaskOutputs> writeV2(
            SerializableTable serializableTable,
            Table table,
            Map<String, List<PerPositionMatch>> matchesByDataFile,
            Set<PartitionKey> touchedPartitions) {
        Map<String, List<DeleteFile>> existing = ExistingPosDeleteLoader.collect(table, touchedPartitions);
        try (PerTaskPosDeleteWriter writer = new PerTaskPosDeleteWriter(serializableTable, 0, 0L)) {
            existing.forEach(writer::registerExistingPosDeletes);
            for (Map.Entry<String, List<PerPositionMatch>> entry : matchesByDataFile.entrySet()) {
                for (PerPositionMatch match : entry.getValue()) {
                    writer.delete(entry.getKey(), match.pos(), match.specId(), match.partitionBytes());
                }
            }
            return List.of(writer.finishAndClose());
        } catch (IOException e) {
            throw new RuntimeException("Failed writing pos-delete files", e);
        }
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

    private List<EqDeleteWorkItem> buildWorkItems(Table table, EqualityDeleteFileRun run) {
        List<EqDeleteWorkItem> items = new ArrayList<>(run.files().size());
        for (DeleteFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
            items.add(new EqDeleteWorkItem(file, file.specId(), partitionBytes));
        }
        return items;
    }

    private static List<org.apache.iceberg.types.Types.NestedField> buildPkFields(Table table) {
        // Re-derive pk fields from the iceberg table + IcestreamTableConfig, but config isn't
        // available here; defer: caller reconstructs it identically. Inline-derive from
        // IcestreamTableConfig.from(table).
        return IcestreamTableConfig.from(table).primaryKey().fields();
    }

    private CommitPlan assemblePlan(
            long startingSnapshotId, EqualityDeleteFileRun run, List<TaskOutputs> taskOutputs) {
        List<DeleteFile> newDeletes = new ArrayList<>();
        List<DeleteFile> rewrittenDeletes = new ArrayList<>();
        CharSequenceSet seenRewrittenLocations = CharSequenceSet.empty();
        for (TaskOutputs output : taskOutputs) {
            newDeletes.addAll(output.newDeletes());
            for (DeleteFile rewritten : output.rewrittenDeletes()) {
                if (seenRewrittenLocations.add(rewritten.location())) {
                    rewrittenDeletes.add(rewritten);
                }
            }
        }
        return new CommitPlan(startingSnapshotId, new ArrayList<>(run.files()), rewrittenDeletes, newDeletes);
    }
}
