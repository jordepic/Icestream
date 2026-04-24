package io.github.jordepic.icestream.indexer;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;
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
import org.apache.iceberg.encryption.EncryptedFiles;
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
class DataFileIndexerTest {

    private static final String KEYSPACE = "icestream_test";
    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "dept", Types.StringType.get()));

    @Container
    static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:5.0.3");

    private static SparkSession spark;
    private static CqlSession cassandraSession;

    @BeforeAll
    static void startEnvironment() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("indexer-test")
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
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        cassandraSession.execute("CREATE KEYSPACE " + KEYSPACE
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        index = new CassandraIndex(cassandraSession, KEYSPACE);
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    @AfterEach
    void teardown() throws IOException {
        cassandraSession.execute("DROP KEYSPACE " + KEYSPACE);
        catalog.close();
    }

    @Test
    void indexesAllRowsWhenNoDeletes() throws IOException {
        TableIdentifier id = TableIdentifier.of("db", "t");
        Table table = catalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeDataFile(table, null, List.of(record(1L, "alice"), record(2L, "bob")));
        index.createIfAbsent(id);

        DataFileRun run = new DataFileRun(1L, List.of(dataFile), Map.of());
        new DataFileIndexer(spark, index).index(id, table, run, IcestreamTableConfig.from(table));

        List<Row> rows = fetchIndexRows(id);
        assertThat(rows).hasSize(2);
        assertThat(rows.stream().map(r -> r.getByteBuffer("pk")).distinct()).hasSize(2);
        assertThat(rows).extracting(r -> r.getInt("spec_id")).containsOnly(0);
        assertThat(rows).extracting(r -> r.getByteBuffer("partition_key")).containsOnly(ByteBuffer.wrap(new byte[0]));
        assertThat(rows).extracting(r -> r.getString("data_file_path")).containsOnly(dataFile.location());
    }

    @Test
    void deletionVectorFiltersRowsFromIndex() throws IOException {
        TableIdentifier id = TableIdentifier.of("db", "t");
        Table table = catalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile =
                writeDataFile(table, null, List.of(record(0L, "alice"), record(1L, "bob"), record(2L, "carol")));
        DeleteFile dv = writeDV(table, dataFile, List.of(1L));
        index.createIfAbsent(id);

        DataFileRun run = new DataFileRun(1L, List.of(dataFile), Map.of(dataFile.location(), List.of(dv)));
        new DataFileIndexer(spark, index).index(id, table, run, IcestreamTableConfig.from(table));

        List<Row> rows = fetchIndexRows(id);
        assertThat(rows).hasSize(2);
        assertThat(rows).extracting(r -> r.getLong("pos")).containsExactlyInAnyOrder(0L, 2L);
        assertThat(rows).extracting(r -> r.getString("data_file_path")).containsOnly(dataFile.location());
    }

    @Test
    void reindexingSamePkOverwritesPreviousRow() throws IOException {
        TableIdentifier id = TableIdentifier.of("db", "t");
        Table table = catalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile first = writeDataFile(table, null, List.of(record(1L, "alice-v1")));
        DataFile second = writeDataFile(table, null, List.of(record(1L, "alice-v2")));
        index.createIfAbsent(id);
        IcestreamTableConfig config = IcestreamTableConfig.from(table);

        new DataFileIndexer(spark, index).index(id, table, new DataFileRun(1L, List.of(first), Map.of()), config);
        new DataFileIndexer(spark, index).index(id, table, new DataFileRun(2L, List.of(second), Map.of()), config);

        List<Row> rows = fetchIndexRows(id);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("data_file_path")).isEqualTo(second.location());
    }

    @Test
    void partitionedTable_encodesDistinctPartitionKeysPerFile() throws IOException {
        TableIdentifier id = TableIdentifier.of("db", "t");
        PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build();
        Table table = catalog.createTable(id, PARTITIONED_SCHEMA, spec, v3Props());
        DataFile eng = writeDataFile(table, partition("eng"), List.of(partitionedRecord(1L, "eng")));
        DataFile sales = writeDataFile(table, partition("sales"), List.of(partitionedRecord(2L, "sales")));
        index.createIfAbsent(id);

        DataFileRun run = new DataFileRun(1L, List.of(eng, sales), Map.of());
        new DataFileIndexer(spark, index).index(id, table, run, IcestreamTableConfig.from(table));

        List<Row> rows = fetchIndexRows(id);
        assertThat(rows).hasSize(2);
        assertThat(rows.stream().map(r -> r.getByteBuffer("partition_key")).distinct()).hasSize(2);
        assertThat(rows)
                .extracting(r -> r.getString("data_file_path"))
                .containsExactlyInAnyOrder(eng.location(), sales.location());
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

    private DeleteFile writeDV(Table table, DataFile dataFile, List<Long> positions) throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                .format(FileFormat.PUFFIN)
                .build();
        DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
        try (DVFileWriter closeable = writer) {
            for (Long pos : positions) {
                closeable.delete(dataFile.location(), pos, table.spec(), null);
            }
        }
        return Iterables.getOnlyElement(writer.result().deleteFiles());
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
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.CASSANDRA_BUCKETS, "4");
        return props;
    }

    private List<Row> fetchIndexRows(TableIdentifier id) {
        return StreamSupport.stream(
                        cassandraSession
                                .execute("SELECT * FROM " + index.qualifiedTableName(id))
                                .spliterator(),
                        false)
                .toList();
    }
}
