package io.github.jordepic.icestream.master;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.converter.DeleteFileCreator;
import io.github.jordepic.icestream.indexer.DataFileIndexer;
import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import io.github.jordepic.icestream.schema.IcestreamProperties;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
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

@Testcontainers
class TableProcessorTest {

    private static final String KEYSPACE = "icestream_processor_test";
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "t");
    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PK_SCHEMA = new Schema(NestedField.required(1, "id", Types.LongType.get()));

    @Container
    static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:5.0.3");

    private static SparkSession spark;
    private static CqlSession cassandraSession;

    @BeforeAll
    static void startEnvironment() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("processor-test")
                .config(
                        "spark.cassandra.connection.host",
                        CASSANDRA.getContactPoint().getAddress().getHostAddress())
                .config(
                        "spark.cassandra.connection.port",
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
    private TableProcessor processor;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        cassandraSession.execute("CREATE KEYSPACE " + KEYSPACE
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        index = new CassandraIndex(cassandraSession, KEYSPACE);
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        processor = new TableProcessor(
                new SnapshotPlanner(),
                new DataFileIndexer(spark, index),
                new DeleteFileCreator(spark, index),
                index,
                IcestreamMetrics.NOOP);
    }

    @AfterEach
    void teardown() throws IOException {
        cassandraSession.execute("DROP KEYSPACE " + KEYSPACE);
        catalog.close();
    }

    @Test
    void dataRunIndexesAndAdvancesState() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        table.newAppend().appendFile(file).commit();

        boolean didWork = processor.processNextRun(TABLE_ID, table);

        assertThat(didWork).isTrue();
        assertThat(rowCount()).isEqualTo(2);
        Table refreshed = catalog.loadTable(TABLE_ID);
        assertThat(refreshed.properties().get(IcestreamProperties.LAST_PROCESSED_SEQUENCE))
                .isEqualTo("1");
        assertThat(refreshed.properties().get(IcestreamProperties.LAST_PROCESSED_KIND))
                .isEqualTo(FileKind.DATA.name());
    }

    @Test
    void eqDeleteRunConvertsAfterDataIndexed() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        table.newAppend().appendFile(file).commit();
        DeleteFile eqDelete = writeEqDelete(table, List.of(record(2L, null)));
        table.newRowDelta().addDeletes(eqDelete).commit();

        assertThat(processor.processNextRun(TABLE_ID, catalog.loadTable(TABLE_ID))).isTrue();
        assertThat(processor.processNextRun(TABLE_ID, catalog.loadTable(TABLE_ID))).isTrue();

        Table refreshed = catalog.loadTable(TABLE_ID);
        assertThat(refreshed.properties().get(IcestreamProperties.LAST_PROCESSED_SEQUENCE))
                .isEqualTo("2");
        assertThat(refreshed.properties().get(IcestreamProperties.LAST_PROCESSED_KIND))
                .isEqualTo(FileKind.EQ_DEL.name());
        assertThat(refreshed.currentSnapshot().addedDeleteFiles(refreshed.io())).hasSize(1);
    }

    @Test
    void emptyTableReturnsFalse() {
        Table table = createTable();

        boolean didWork = processor.processNextRun(TABLE_ID, table);

        assertThat(didWork).isFalse();
        assertThat(catalog.loadTable(TABLE_ID).properties())
                .doesNotContainKey(IcestreamProperties.LAST_PROCESSED_SEQUENCE);
    }

    @Test
    void noNewWorkReturnsFalse() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice")));
        table.newAppend().appendFile(file).commit();
        processor.processNextRun(TABLE_ID, table);

        boolean didWork = processor.processNextRun(TABLE_ID, catalog.loadTable(TABLE_ID));

        assertThat(didWork).isFalse();
    }

    @Test
    void firstSuccessfulRunPinsCurrentConfig() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice")));
        table.newAppend().appendFile(file).commit();

        processor.processNextRun(TABLE_ID, table);

        Map<String, String> props = catalog.loadTable(TABLE_ID).properties();
        assertThat(props.get(IcestreamProperties.PINNED_PRIMARY_KEYS)).isEqualTo("id");
        assertThat(props.get(IcestreamProperties.PINNED_CASSANDRA_BUCKETS)).isEqualTo("4");
    }

    @Test
    void tableWithoutPrimaryKeysReturnsFalse() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        Table table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), props);

        boolean didWork = processor.processNextRun(TABLE_ID, table);

        assertThat(didWork).isFalse();
    }

    @Test
    void primaryKeyDriftAfterPinningThrows() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice")));
        table.newAppend().appendFile(file).commit();
        processor.processNextRun(TABLE_ID, table);
        Table afterFirst = catalog.loadTable(TABLE_ID);
        afterFirst.updateProperties()
                .set(IcestreamProperties.PRIMARY_KEYS, "name")
                .commit();

        assertThatThrownBy(() -> processor.processNextRun(TABLE_ID, catalog.loadTable(TABLE_ID)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary-keys");
    }

    @Test
    void validationExceptionFromConverterPropagatesAndStateNotAdvanced() throws IOException {
        Table table = createTable();
        DataFile file = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        table.newAppend().appendFile(file).commit();
        processor.processNextRun(TABLE_ID, table);

        Table afterIndex = catalog.loadTable(TABLE_ID);
        DeleteFile eqDelete = writeEqDelete(afterIndex, List.of(record(2L, null)));
        afterIndex.newRowDelta().addDeletes(eqDelete).commit();
        Table beforeConvert = catalog.loadTable(TABLE_ID);
        Table racer = catalog.loadTable(TABLE_ID);
        DeleteFile blockingDv = writeDvForDataFile(racer, file, List.of(0L));
        racer.newRowDelta().addDeletes(blockingDv).removeDeletes(eqDelete).commit();

        assertThatThrownBy(() -> processor.processNextRun(TABLE_ID, beforeConvert))
                .isInstanceOf(ValidationException.class);

        assertThat(catalog.loadTable(TABLE_ID).properties().get(IcestreamProperties.LAST_PROCESSED_SEQUENCE))
                .isEqualTo("1");
    }

    private Table createTable() {
        return catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), v3Props());
    }

    private Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.CASSANDRA_BUCKETS, "4");
        return props;
    }

    private DataFile writeDataFile(Table table, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("data", "parquet"));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(Table table, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("eqdel", "parquet"));
        GenericAppenderFactory factory =
                new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, PK_SCHEMA, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            for (Record row : rows) {
                Record keyOnly = GenericRecord.create(PK_SCHEMA);
                keyOnly.setField("id", row.getField("id"));
                writer.write(keyOnly);
            }
        }
        return writer.toDeleteFile();
    }

    private DeleteFile writeDvForDataFile(Table table, DataFile dataFile, List<Long> positions) throws IOException {
        OutputFileFactory factory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PUFFIN)
                .build();
        DVFileWriter dv = new BaseDVFileWriter(factory, p -> null);
        try (DVFileWriter closeable = dv) {
            for (Long pos : positions) {
                closeable.delete(dataFile.location(), pos, table.spec(), null);
            }
        }
        return Iterables.getOnlyElement(dv.result().deleteFiles());
    }

    private Record record(long id, String name) {
        Record r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
        return r;
    }

    private String newPath(String prefix, String ext) {
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private long rowCount() {
        return cassandraSession
                .execute("SELECT COUNT(*) FROM " + index.qualifiedTableName(TABLE_ID))
                .one()
                .getLong(0);
    }
}
