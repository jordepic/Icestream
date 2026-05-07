package io.github.jordepic.icestream.flinkpaimon.converter;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.flinkpaimon.indexer.FlinkDataFileIndexer;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Behavioral coverage for the Flink+Paimon converter end-to-end. Tests exercise the full
 * pipeline (HadoopCatalog + Paimon FilesystemCatalog + in-process Flink MiniCluster):
 * <ul>
 *   <li>V3 happy path + merge with existing DV.
 *   <li>V2 happy path + merge with existing FILE-scoped pos-delete.
 *   <li>Multi-data-file conversion: one DV per referenced data file.
 *   <li>Partitioned table: hex-encoded {@code partition_key} round-trips through the lookup join
 *       and the new DV/pos-delete carries the right partition.
 *   <li>Empty-run no-op + LookupJoin planner assertion.
 * </ul>
 */
class FlinkDeleteFileCreatorTest {

    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "dept", Types.StringType.get()));

    @TempDir
    Path icebergWarehouse;

    @TempDir
    Path paimonWarehouse;

    private HadoopCatalog icebergCatalog;
    private PaimonIndex paimonIndex;
    private FlinkContext flink;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        icebergCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse.toString());
        paimonIndex = PaimonIndex.create(paimonWarehouse.toUri().toString(), "icestream", Map.of());
        flink = FlinkContext.local(2);
    }

    @AfterEach
    void tearDown() throws IOException {
        flink.close();
        try {
            paimonIndex.catalog().close();
        } catch (Exception ignored) {
        }
        icebergCatalog.close();
    }

    @Test
    void convertsEqDeleteAgainstIndexedPk() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        IcestreamTableConfig config = indexAndCommit(id, table, dataFile);

        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(2L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), config);

        assertThat(plan.eqDeletesToRemove())
                .extracting(DeleteFile::location)
                .containsExactly(eqDelete.location());
        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).referencedDataFile()).isEqualTo(dataFile.location());
        assertThat(plan.existingDeletesToRemove()).isEmpty();
    }

    @Test
    void existingDvOnTargetDataFile_mergedAndScheduledForRemoval() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
        DeleteFile existingDv = writeAndCommitDv(table, dataFile, List.of(0L));
        IcestreamTableConfig config = indexAndCommit(id, table, dataFile);

        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(3L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), config);

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount())
                .as("merged DV should cover both the prior position 0 and the new position 2")
                .isEqualTo(2L);
        assertThat(plan.existingDeletesToRemove())
                .extracting(d -> d.location().toString())
                .containsExactly(existingDv.location());
    }

    @Test
    void eqDeleteSpansMultipleDataFiles_oneDvPerReferencedDataFile() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile first = writeUnpartitionedDataFile(table, List.of(record(1L, "alice")));
        DataFile second = writeUnpartitionedDataFile(table, List.of(record(2L, "bob")));
        IcestreamTableConfig config = indexAndCommit(id, table, first, second);

        DeleteFile eqDelete = writeEqDelete(
                table, FileFormat.PARQUET, null, List.of(record(1L, "x"), record(2L, "y")));
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), config);

        assertThat(plan.deletesToAdd()).hasSize(2);
        assertThat(plan.deletesToAdd())
                .extracting(d -> d.referencedDataFile().toString())
                .containsExactlyInAnyOrder(first.location(), second.location());
        assertThat(plan.deletesToAdd()).extracting(DeleteFile::recordCount).containsOnly(1L);
    }

    @Test
    void partitionedTable_eqDeleteConfinedToOwnPartition() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        PartitionSpec spec =
                PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build();
        Table table = icebergCatalog.createTable(id, PARTITIONED_SCHEMA, spec, v3Props());
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        DataFile salesFile = writePartitionedDataFile(table, "sales", List.of(partitionedRecord(1L, "sales")));
        IcestreamTableConfig config = indexAndCommit(id, table, engFile, salesFile);

        DeleteFile engEqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, "eng")));
        table.newRowDelta().addDeletes(engEqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(3L, List.of(engEqDelete)), config);

        assertThat(plan.deletesToAdd())
                .as("hex-encoded partition_key prunes the lookup join to the eng partition only")
                .hasSize(1);
        DeleteFile newDv = plan.deletesToAdd().get(0);
        assertThat(newDv.referencedDataFile()).isEqualTo(engFile.location());
        assertThat(newDv.specId()).isEqualTo(table.spec().specId());
        assertThat(newDv.partition().get(0, CharSequence.class)).hasToString("eng");
    }

    @Test
    void v2_eqDelete_convertsToFileScopedPosDelete() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        IcestreamTableConfig config = indexAndCommit(id, table, dataFile);

        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(2L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), config);

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile newPosDelete = plan.deletesToAdd().get(0);
        assertThat(newPosDelete.format()).isEqualTo(FileFormat.PARQUET);
        assertThat(ContentFileUtil.referencedDataFile(newPosDelete).toString()).isEqualTo(dataFile.location());
        assertThat(newPosDelete.recordCount()).isEqualTo(1L);
        assertThat(plan.existingDeletesToRemove()).isEmpty();
    }

    @Test
    void v2_mergesWithExistingFileScopedPosDelete() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        DeleteFile existingPosDelete = writeAndCommitFileScopedPosDelete(table, dataFile, List.of(0L));
        IcestreamTableConfig config = indexAndCommit(id, table, dataFile);

        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(3L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), config);

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile merged = plan.deletesToAdd().get(0);
        assertThat(ContentFileUtil.referencedDataFile(merged).toString()).isEqualTo(dataFile.location());
        assertThat(merged.recordCount())
                .as("merged file contains the existing position 0 plus the new position 2")
                .isEqualTo(2L);
        assertThat(plan.existingDeletesToRemove())
                .extracting(DeleteFile::location)
                .containsExactly(existingPosDelete.location());
    }

    @Test
    void lookupJoinPlanUsesPaimonFileStoreLookupFunction() {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        try (FlinkContext ctx = FlinkContext.local(1)) {
            org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = ctx.newBatchEnv();
            org.apache.flink.table.api.bridge.java.StreamTableEnvironment tEnv =
                    org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
            registerPaimonCatalogForTest(tEnv);

            org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.types.Row> empty =
                    env.fromCollection(List.<org.apache.flink.types.Row>of(),
                            new org.apache.flink.api.java.typeutils.RowTypeInfo(
                                    new org.apache.flink.api.common.typeinfo.TypeInformation[] {
                                        org.apache.flink.api.common.typeinfo.Types.INT,
                                        org.apache.flink.api.common.typeinfo.Types.STRING,
                                        org.apache.flink.api.common.typeinfo.Types.STRING
                                    },
                                    new String[] {"spec_id", "partition_key", "pk"}));
            org.apache.flink.table.api.Schema probeSchema = org.apache.flink.table.api.Schema.newBuilder()
                    .column("spec_id", org.apache.flink.table.api.DataTypes.INT())
                    .column("partition_key", org.apache.flink.table.api.DataTypes.STRING())
                    .column("pk", org.apache.flink.table.api.DataTypes.STRING())
                    .columnByExpression("proc", "PROCTIME()")
                    .build();
            tEnv.createTemporaryView("icestream_eq_deletes", tEnv.fromDataStream(empty, probeSchema));

            FlinkDeleteFileCreator creator = new FlinkDeleteFileCreator(ctx, paimonIndex);
            String sql = FlinkDeleteFileCreator.buildLookupJoinSql(creator.qualifiedIndexFqn(id));
            String plan = tEnv.sqlQuery(sql).explain();

            assertThat(plan)
                    .as(
                            "Flink should pick LookupJoin against Paimon's FileStoreLookupFunction; "
                                    + "if the plan picks a regular hash/sort-merge join we silently lose "
                                    + "the indexed-probe semantics.\nPlan was:\n%s",
                            plan)
                    .contains("LookupJoin");
        }
    }

    private void registerPaimonCatalogForTest(org.apache.flink.table.api.bridge.java.StreamTableEnvironment tEnv) {
        StringBuilder withClause = new StringBuilder("'type'='paimon'");
        paimonIndex.catalogOptionsForFlink().forEach((k, v) ->
                withClause.append(",'").append(k).append("'='").append(v).append("'"));
        tEnv.executeSql("CREATE CATALOG paimon WITH (" + withClause + ")");
    }

    @Test
    void emptyEqDeleteRunReturnsNoOpPlan() {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);
        // Drive a snapshot so that table.currentSnapshot() is non-null in create().
        table.newAppend().commit();

        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex)
                .create(id, table, new EqualityDeleteFileRun(1L, List.of()), config);

        assertThat(plan.isNoOp()).isTrue();
    }

    private IcestreamTableConfig indexAndCommit(TableIdentifier id, Table table, DataFile... dataFiles) {
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (DataFile dataFile : dataFiles) {
            append.appendFile(dataFile);
        }
        append.commit();
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);
        new FlinkDataFileIndexer(flink, paimonIndex)
                .index(id, table, new DataFileRun(1L, List.of(dataFiles), Map.of()), config);
        table.refresh();
        return config;
    }

    private DataFile writeUnpartitionedDataFile(Table table, List<Record> rows) throws IOException {
        return writeDataFile(table, null, rows);
    }

    private DataFile writePartitionedDataFile(Table table, String dept, List<Record> rows) throws IOException {
        return writeDataFile(table, partition(dept), rows);
    }

    private DataFile writeDataFile(Table table, StructLike partition, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newFilePath("data", "parquet"));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer = factory.newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, partition);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(Table table, FileFormat format, StructLike partition, List<Record> rows)
            throws IOException {
        Schema deleteSchema = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(newFilePath("eq-delete", format.name().toLowerCase()));
        GenericAppenderFactory factory = new GenericAppenderFactory(
                table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), format, partition);
        try (Closeable toClose = writer) {
            for (Record row : rows) {
                Record projected = GenericRecord.create(deleteSchema);
                projected.setField("id", row.getField("id"));
                writer.write(projected);
            }
        }
        return writer.toDeleteFile();
    }

    private DeleteFile writeAndCommitDv(Table table, DataFile dataFile, List<Long> deletedPositions)
            throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PUFFIN)
                .build();
        DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
        try (DVFileWriter closeable = writer) {
            for (Long pos : deletedPositions) {
                closeable.delete(dataFile.location(), pos, table.specs().get(dataFile.specId()), dataFile.partition());
            }
        }
        DeleteFile dv = Iterables.getOnlyElement(writer.result().deleteFiles());
        table.newRowDelta().addDeletes(dv).commit();
        return dv;
    }

    private DeleteFile writeAndCommitFileScopedPosDelete(Table table, DataFile dataFile, List<Long> positions)
            throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PARQUET)
                .build();
        FanoutPositionOnlyDeleteWriter<Record> writer = new FanoutPositionOnlyDeleteWriter<>(
                new TestPosDeleteWriterFactory(table, FileFormat.PARQUET),
                fileFactory,
                table.io(),
                64L * 1024 * 1024,
                DeleteGranularity.FILE);
        PositionDelete<Record> positionDelete = PositionDelete.create();
        try {
            for (Long pos : positions) {
                positionDelete.set(dataFile.location(), pos, null);
                writer.write(positionDelete, table.specs().get(dataFile.specId()), dataFile.partition());
            }
        } finally {
            writer.close();
        }
        DeleteFile delete = Iterables.getOnlyElement(writer.result().deleteFiles());
        table.newRowDelta().addDeletes(delete).commit();
        return delete;
    }

    private static StructLike partition(String dept) {
        return new StructLike() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass) {
                return javaClass.cast(dept);
            }

            @Override
            public <T> void set(int pos, T value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Record record(long id, String name) {
        Record r = GenericRecord.create(UNPARTITIONED_SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
        return r;
    }

    private Record partitionedRecord(long id, String dept) {
        Record r = GenericRecord.create(PARTITIONED_SCHEMA);
        r.setField("id", id);
        r.setField("dept", dept);
        return r;
    }

    private String newFilePath(String prefix, String ext) {
        return icebergWarehouse
                .resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private static Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "2");
        return props;
    }

    private static Map<String, String> v2Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "2");
        return props;
    }

    private static final class TestPosDeleteWriterFactory implements FileWriterFactory<Record> {
        private final Table table;
        private final FileFormat deleteFormat;
        private final Map<Integer, GenericAppenderFactory> bySpec = new HashMap<>();

        TestPosDeleteWriterFactory(Table table, FileFormat deleteFormat) {
            this.table = table;
            this.deleteFormat = deleteFormat;
        }

        @Override
        public PositionDeleteWriter<Record> newPositionDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            GenericAppenderFactory delegate = bySpec.computeIfAbsent(
                    spec.specId(),
                    id -> new GenericAppenderFactory(
                            table, table.schema(), spec, table.properties(), null, null, null));
            return delegate.newPosDeleteWriter(file, deleteFormat, partition);
        }

        @Override
        public DataWriter<Record> newDataWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }
    }
}
