package io.github.jordepic.icestream.sparkcassandra.converter;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.sparkcassandra.cassandra.CassandraIndex;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.sparkcassandra.indexer.SparkDataFileIndexer;
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
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.PerPositionMatch;

@Testcontainers
class SparkDeleteFileCreatorTest {

    private static final String KEYSPACE = "icestream_converter_test";
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "t");
    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "dept", Types.StringType.get()));
    private static final Schema PK_SCHEMA = new Schema(NestedField.required(1, "id", Types.LongType.get()));

    @Container
    static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:5.0.3");

    private static SparkSession spark;
    private static CqlSession cassandraSession;

    @BeforeAll
    static void startEnvironment() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("converter-test")
                .config("spark.cassandra.connection.host",
                        CASSANDRA.getContactPoint().getAddress().getHostAddress())
                .config("spark.cassandra.connection.port",
                        String.valueOf(CASSANDRA.getContactPoint().getPort()))
                .config("spark.cassandra.connection.localDC", CASSANDRA.getLocalDatacenter())
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        cassandraSession = CqlSession.builder()
                .addContactPoint(CASSANDRA.getContactPoint())
                .withLocalDatacenter(CASSANDRA.getLocalDatacenter())
                .build();
    }

    @AfterAll
    static void stopEnvironment() {
        cassandraSession.close();
        spark.stop();
    }

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private CassandraIndex index;
    private SparkDeleteFileCreator creator;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        cassandraSession.execute("CREATE KEYSPACE " + KEYSPACE
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        index = new CassandraIndex(cassandraSession, KEYSPACE);
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        creator = new SparkDeleteFileCreator(spark, index);
    }

    @AfterEach
    void teardown() throws IOException {
        cassandraSession.execute("DROP KEYSPACE " + KEYSPACE);
        catalog.close();
    }

    @Test
    void eqDeleteHitsSingleIndexedPk_newDvTargetsCorrectPosition() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "alice"), unpartitionedRecord(2L, "bob")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.eqDeletesToRemove()).containsExactly(eqDelete);
        assertThat(plan.existingDeletesToRemove()).isEmpty();
        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile newDv = plan.deletesToAdd().get(0);
        assertThat(newDv.format()).isEqualTo(FileFormat.PUFFIN);
        assertThat(newDv.referencedDataFile()).isEqualTo(dataFile.location());
        assertThat(newDv.recordCount()).isEqualTo(1L);
    }

    @Test
    void eqDeleteHitsAllIndexedPks_newDvHasAllPositions() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(
                table,
                List.of(
                        unpartitionedRecord(1L, "a"),
                        unpartitionedRecord(2L, "b"),
                        unpartitionedRecord(3L, "c")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(
                table,
                FileFormat.PARQUET,
                null,
                List.of(unpartitionedRecord(1L, null), unpartitionedRecord(2L, null), unpartitionedRecord(3L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount()).isEqualTo(3L);
        assertThat(plan.deletesToAdd().get(0).referencedDataFile()).isEqualTo(dataFile.location());
    }

    @Test
    void eqDeleteHitsNoIndexedPk_commitPlanHasEmptyDvList() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(1L, "alice")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(99L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).isEmpty();
        assertThat(plan.existingDeletesToRemove()).isEmpty();
        assertThat(plan.eqDeletesToRemove()).containsExactly(eqDelete);
    }

    @Test
    void eqDeleteSpansMultipleDataFiles_oneDvPerReferencedDataFile() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile first = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(1L, "alice")));
        DataFile second = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(2L, "bob")));
        indexAndCommitDataFile(table, first);
        indexAndCommitDataFile(table, second);
        DeleteFile eqDelete = writeEqDelete(
                table,
                FileFormat.PARQUET,
                null,
                List.of(unpartitionedRecord(1L, null), unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(2);
        assertThat(plan.deletesToAdd())
                .extracting(d -> d.referencedDataFile().toString())
                .containsExactlyInAnyOrder(first.location(), second.location());
        assertThat(plan.deletesToAdd()).extracting(DeleteFile::recordCount).containsOnly(1L);
    }

    @Test
    void existingDvOnTargetDataFile_mergedAndScheduledForRemoval() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(
                table,
                List.of(
                        unpartitionedRecord(1L, "a"),
                        unpartitionedRecord(2L, "b"),
                        unpartitionedRecord(3L, "c")));
        DeleteFile existingDv = writeAndCommitDv(table, dataFile, List.of(0L));
        indexAndCommitDataFile(table, dataFile, existingDv);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(3L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount()).isEqualTo(2L);
        assertThat(plan.existingDeletesToRemove())
                .extracting(d -> d.location().toString())
                .containsExactly(existingDv.location());
    }

    @Test
    void partitionedTable_eqDeleteConfinedToOwnPartition() throws IOException {
        Table table = createPartitionedTable();
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        DataFile salesFile = writePartitionedDataFile(table, "sales", List.of(partitionedRecord(1L, "sales")));
        indexAndCommitDataFile(table, engFile);
        indexAndCommitDataFile(table, salesFile);
        DeleteFile engEqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(3L, List.of(engEqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).referencedDataFile()).isEqualTo(engFile.location());
    }

    @Test
    void partitionedTable_newDvCarriesEqDeletePartitionInManifest() throws IOException {
        Table table = createPartitionedTable();
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        indexAndCommitDataFile(table, engFile);
        DeleteFile eqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile newDv = plan.deletesToAdd().get(0);
        assertThat(newDv.specId()).isEqualTo(table.spec().specId());
        assertThat(newDv.partition().get(0, CharSequence.class)).hasToString("eng");
    }

    @Test
    void avroEqDeleteFormat_readsCorrectly() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "a"), unpartitionedRecord(2L, "b")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.AVRO, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount()).isEqualTo(1L);
        assertThat(plan.deletesToAdd().get(0).referencedDataFile()).isEqualTo(dataFile.location());
    }

    @Test
    void orcEqDeleteFormat_readsCorrectly() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "a"), unpartitionedRecord(2L, "b")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.ORC, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount()).isEqualTo(1L);
        assertThat(plan.deletesToAdd().get(0).referencedDataFile()).isEqualTo(dataFile.location());
    }

    @Test
    void joinPipelineUsesCassandraJoinAndReplicaAwareRepartition() throws IOException {
        Table table = createUnpartitionedTable();
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(1L, "a")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(1L, null)));

        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        EqDeleteWorkItem workItem = new EqDeleteWorkItem(
                eqDelete,
                eqDelete.specId(),
                IndexEncoding.encodeAsAvroBytes(table.specs().get(eqDelete.specId()).partitionType(), eqDelete.partition()));
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Table> tableBroadcast = jsc.broadcast(SerializableTable.copyOf(table));

        org.apache.spark.api.java.JavaPairRDD<String, PerPositionMatch> pipeline =
                creator.buildJoinPipeline(TABLE_ID, List.of(workItem), config, tableBroadcast);

        String lineage = pipeline.toDebugString();
        assertThat(lineage).contains("CassandraJoinRDD");
        assertThat(lineage).contains("RDDFunctions.scala");
    }

    private Table createUnpartitionedTable() {
        Table table = catalog.createTable(TABLE_ID, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        index.createIfAbsent(TABLE_ID);
        return table;
    }

    private Table createPartitionedTable() {
        PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build();
        Table table = catalog.createTable(TABLE_ID, PARTITIONED_SCHEMA, spec, v3Props());
        index.createIfAbsent(TABLE_ID);
        return table;
    }

    private void indexAndCommitDataFile(Table table, DataFile dataFile, DeleteFile... deletes) {
        table.newAppend().appendFile(dataFile).commit();
        Map<String, List<DeleteFile>> deletesMap = deletes.length == 0
                ? Map.of()
                : Map.of(dataFile.location(), List.of(deletes));
        DataFileRun run = new DataFileRun(1L, List.of(dataFile), deletesMap);
        new SparkDataFileIndexer(spark, index).index(TABLE_ID, table, run, IcestreamTableConfig.from(table));
    }

    private DataFile writeUnpartitionedDataFile(Table table, List<Record> rows) throws IOException {
        return writeDataFile(table, null, rows);
    }

    private DataFile writePartitionedDataFile(Table table, String dept, List<Record> rows) throws IOException {
        return writeDataFile(table, partition(dept), rows);
    }

    private DataFile writeDataFile(Table table, StructLike partition, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("data", "parquet"));
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
        OutputFile out = table.io().newOutputFile(newPath("eqdel", format.name().toLowerCase()));
        GenericAppenderFactory factory = new GenericAppenderFactory(
                table.schema(), table.spec(), new int[] {1}, PK_SCHEMA, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), format, partition);
        try (Closeable toClose = writer) {
            for (Record row : rows) {
                Record keyOnly = GenericRecord.create(PK_SCHEMA);
                keyOnly.setField("id", row.getField("id"));
                writer.write(keyOnly);
            }
        }
        return writer.toDeleteFile();
    }

    /** Writes a DV via {@code BaseDVFileWriter} and commits it so it shows up in delete manifests. */
    private DeleteFile writeAndCommitDv(Table table, DataFile dataFile, List<Long> deletedPositions)
            throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PUFFIN)
                .build();
        DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
        try (DVFileWriter closeable = writer) {
            for (Long pos : deletedPositions) {
                closeable.delete(dataFile.location(), pos, table.spec(), null);
            }
        }
        DeleteFile dv = Iterables.getOnlyElement(writer.result().deleteFiles());
        table.newRowDelta().addDeletes(dv).commit();
        return dv;
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

    private Record unpartitionedRecord(long id, String name) {
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

    private String newPath(String prefix, String ext) {
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "4");
        return props;
    }

    private Map<String, String> v2Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "4");
        return props;
    }

    @Test
    void v2_eqDelete_convertsToParquetPosDeleteFile() throws IOException {
        Table table = createUnpartitionedV2Table();
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "alice"), unpartitionedRecord(2L, "bob")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile newPosDelete = plan.deletesToAdd().get(0);
        assertThat(newPosDelete.format()).isEqualTo(FileFormat.PARQUET);
        assertThat(ContentFileUtil.referencedDataFile(newPosDelete).toString()).isEqualTo(dataFile.location());
        assertThat(newPosDelete.recordCount()).isEqualTo(1L);
        assertThat(plan.existingDeletesToRemove()).isEmpty();
    }

    @Test
    void v2_honorsWriteDeleteFormatAvro() throws IOException {
        Map<String, String> props = v2Props();
        props.put(TableProperties.DELETE_DEFAULT_FILE_FORMAT, "avro");
        Table table = catalog.createTable(TABLE_ID, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), props);
        index.createIfAbsent(TABLE_ID);
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(1L, "alice")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(1L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).format()).isEqualTo(FileFormat.AVRO);
    }

    @Test
    void v2_mergesWithExistingFileScopedPosDelete() throws IOException {
        Table table = createUnpartitionedV2Table();
        DataFile dataFile = writeUnpartitionedDataFile(table,
                List.of(unpartitionedRecord(1L, "alice"), unpartitionedRecord(2L, "bob"), unpartitionedRecord(3L, "carol")));
        indexAndCommitDataFile(table, dataFile);

        DeleteFile existingPosDelete = writeAndCommitFileScopedPosDelete(table, dataFile, List.of(0L));
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(3L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(3L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile merged = plan.deletesToAdd().get(0);
        assertThat(merged.format()).isEqualTo(FileFormat.PARQUET);
        assertThat(ContentFileUtil.referencedDataFile(merged).toString()).isEqualTo(dataFile.location());
        assertThat(merged.recordCount()).as("merged file contains the existing position 0 plus the new position 2").isEqualTo(2L);
        assertThat(plan.existingDeletesToRemove())
                .extracting(DeleteFile::location)
                .containsExactly(existingPosDelete.location());
    }

    @Test
    void v2_unscopedExistingPosDelete_isLeftAlone() throws IOException {
        Table table = createUnpartitionedV2Table();
        DataFile dataFileA = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "alice"), unpartitionedRecord(2L, "bob")));
        DataFile dataFileB = writeUnpartitionedDataFile(table, List.of(unpartitionedRecord(3L, "carol")));
        indexAndCommitDataFiles(table, List.of(dataFileA, dataFileB));

        DeleteFile unscopedPosDelete = writeAndCommitUnscopedPosDelete(
                table,
                List.of(new PosDeletePos(dataFileA.location(), 0L), new PosDeletePos(dataFileB.location(), 0L)));
        assertThat(ContentFileUtil.referencedDataFile(unscopedPosDelete))
                .as("baseline: an unscoped pos-delete file references no specific data file")
                .isNull();

        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(4L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(ContentFileUtil.referencedDataFile(plan.deletesToAdd().get(0)).toString())
                .isEqualTo(dataFileA.location());
        assertThat(plan.existingDeletesToRemove())
                .as("unscoped pos-delete files are not absorbed; they remain alongside")
                .extracting(DeleteFile::location)
                .doesNotContain(unscopedPosDelete.location());
    }

    @Test
    void v2_multipleEqDeleteFilesOnOneDataFile_squashedIntoSinglePosDeleteFile() throws IOException {
        Table table = createUnpartitionedV2Table();
        DataFile dataFile = writeUnpartitionedDataFile(
                table,
                List.of(unpartitionedRecord(1L, "a"), unpartitionedRecord(2L, "b"), unpartitionedRecord(3L, "c")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete1 = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(1L, null)));
        DeleteFile eqDelete2 = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(3L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID,
                table,
                new EqualityDeleteFileRun(2L, List.of(eqDelete1, eqDelete2)),
                IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        DeleteFile squashed = plan.deletesToAdd().get(0);
        assertThat(squashed.recordCount())
                .as("two eq-delete files contributing positions 0 and 2 should produce one merged pos-delete with 2 entries")
                .isEqualTo(2L);
        assertThat(ContentFileUtil.referencedDataFile(squashed).toString()).isEqualTo(dataFile.location());
    }

    @Test
    void v2_partitionedTable_posDeleteFileLocationContainsPartitionPath() throws IOException {
        Table table = catalog.createTable(
                TABLE_ID,
                PARTITIONED_SCHEMA,
                PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build(),
                v2Props());
        index.createIfAbsent(TABLE_ID);
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        indexAndCommitDataFile(table, engFile);
        DeleteFile eqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, "eng")));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).location())
                .as("output pos-delete file should be laid out under the data file's partition path")
                .contains("dept=eng");
    }

    @Test
    void v2_writtenDeletesAreFileScoped() throws IOException {
        Table table = createUnpartitionedV2Table();
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(unpartitionedRecord(1L, "alice"), unpartitionedRecord(2L, "bob")));
        indexAndCommitDataFile(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(unpartitionedRecord(2L, null)));

        CommitPlan plan = creator.create(
                TABLE_ID, table, new EqualityDeleteFileRun(2L, List.of(eqDelete)), IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(ContentFileUtil.isFileScoped(plan.deletesToAdd().get(0))).isTrue();
    }

    @Test
    void v2_partitionedTable_emitsPerPartitionPosDeleteFiles() throws IOException {
        Table table = catalog.createTable(TABLE_ID, PARTITIONED_SCHEMA,
                PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build(), v2Props());
        index.createIfAbsent(TABLE_ID);
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        DataFile salesFile = writePartitionedDataFile(table, "sales", List.of(partitionedRecord(2L, "sales")));
        indexAndCommitDataFiles(table, List.of(engFile, salesFile));

        DeleteFile engEqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, "eng")));
        DeleteFile salesEqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("sales"), List.of(partitionedRecord(2L, "sales")));

        CommitPlan plan = creator.create(
                TABLE_ID, table,
                new EqualityDeleteFileRun(3L, List.of(engEqDelete, salesEqDelete)),
                IcestreamTableConfig.from(table));

        assertThat(plan.deletesToAdd()).hasSize(2);
        assertThat(plan.deletesToAdd())
                .extracting(d -> ContentFileUtil.referencedDataFile(d) == null
                        ? null
                        : ContentFileUtil.referencedDataFile(d).toString())
                .containsExactlyInAnyOrder(engFile.location(), salesFile.location());
    }

    private Table createUnpartitionedV2Table() {
        Table table = catalog.createTable(TABLE_ID, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        index.createIfAbsent(TABLE_ID);
        return table;
    }

    /** Writes a parquet pos-delete file with referencedDataFile set (FILE-scoped) and commits it. */
    private DeleteFile writeAndCommitFileScopedPosDelete(Table table, DataFile dataFile, List<Long> positions)
            throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PARQUET)
                .build();
        FanoutPositionOnlyDeleteWriter<Record> writer = new FanoutPositionOnlyDeleteWriter<>(
                new TestPosDeleteWriterFactory(table, FileFormat.PARQUET),
                fileFactory, table.io(), 64L * 1024 * 1024,
                org.apache.iceberg.deletes.DeleteGranularity.FILE);
        org.apache.iceberg.deletes.PositionDelete<Record> positionDelete =
                org.apache.iceberg.deletes.PositionDelete.create();
        try {
            for (Long pos : positions) {
                positionDelete.set(dataFile.location(), pos, null);
                writer.write(positionDelete, table.spec(), null);
            }
        } finally {
            writer.close();
        }
        DeleteFile delete = Iterables.getOnlyElement(writer.result().deleteFiles());
        table.newRowDelta().addDeletes(delete).commit();
        return delete;
    }

    /** A pos-delete with referencedDataFile == null (PARTITION granularity), referencing N data files. */
    private DeleteFile writeAndCommitUnscopedPosDelete(Table table, List<PosDeletePos> positions) throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PARQUET)
                .build();
        FanoutPositionOnlyDeleteWriter<Record> writer = new FanoutPositionOnlyDeleteWriter<>(
                new TestPosDeleteWriterFactory(table, FileFormat.PARQUET),
                fileFactory, table.io(), 64L * 1024 * 1024,
                org.apache.iceberg.deletes.DeleteGranularity.PARTITION);
        org.apache.iceberg.deletes.PositionDelete<Record> positionDelete =
                org.apache.iceberg.deletes.PositionDelete.create();
        try {
            for (PosDeletePos p : positions) {
                positionDelete.set(p.dataFilePath(), p.pos(), null);
                writer.write(positionDelete, table.spec(), null);
            }
        } finally {
            writer.close();
        }
        DeleteFile delete = Iterables.getOnlyElement(writer.result().deleteFiles());
        table.newRowDelta().addDeletes(delete).commit();
        return delete;
    }

    private record PosDeletePos(String dataFilePath, long pos) {}

    private void indexAndCommitDataFiles(Table table, List<DataFile> dataFiles) {
        org.apache.iceberg.AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();
        DataFileRun run = new DataFileRun(1L, dataFiles, Map.of());
        new SparkDataFileIndexer(spark, index).index(TABLE_ID, table, run, IcestreamTableConfig.from(table));
    }

    /** Local FileWriterFactory adapter — matches PerTaskPosDeleteWriter's inner class but lives in tests. */
    private static final class TestPosDeleteWriterFactory implements org.apache.iceberg.io.FileWriterFactory<Record> {
        private final Table table;
        private final FileFormat deleteFormat;
        private final Map<Integer, GenericAppenderFactory> bySpec = new HashMap<>();

        TestPosDeleteWriterFactory(Table table, FileFormat deleteFormat) {
            this.table = table;
            this.deleteFormat = deleteFormat;
        }

        @Override
        public org.apache.iceberg.deletes.PositionDeleteWriter<Record> newPositionDeleteWriter(
                org.apache.iceberg.encryption.EncryptedOutputFile file,
                PartitionSpec spec, StructLike partition) {
            GenericAppenderFactory delegate = bySpec.computeIfAbsent(
                    spec.specId(),
                    id -> new GenericAppenderFactory(
                            table, table.schema(), spec, table.properties(), null, null, null));
            return delegate.newPosDeleteWriter(file, deleteFormat, partition);
        }

        @Override
        public DataWriter<Record> newDataWriter(
                org.apache.iceberg.encryption.EncryptedOutputFile file,
                PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                org.apache.iceberg.encryption.EncryptedOutputFile file,
                PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }
    }
}
