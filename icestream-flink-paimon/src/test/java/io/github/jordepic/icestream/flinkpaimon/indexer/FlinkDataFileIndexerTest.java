package io.github.jordepic.icestream.flinkpaimon.indexer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonCatalogFactory;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.planner.DataFileRun;
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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test exercising the Flink+Paimon indexer end-to-end against a real iceberg HadoopCatalog,
 * a real Paimon FilesystemCatalog, and an in-process Flink MiniCluster. Verifies the cross-module
 * wiring: work-item serialization, the iceberg-side data-file read inside a Flink TaskManager,
 * the Paimon sink, and that the index table receives a snapshot.
 */
class FlinkDataFileIndexerTest {

    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));

    @TempDir
    Path icebergWarehouse;

    @TempDir
    Path paimonWarehouse;

    private HadoopCatalog icebergCatalog;
    private org.apache.paimon.catalog.Catalog paimonCatalog;
    private PaimonIndex paimonIndex;
    private FlinkContext flink;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        icebergCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse.toString());
        paimonCatalog = PaimonCatalogFactory.create(paimonWarehouse.toUri().toString(), Map.of());
        paimonIndex = new PaimonIndex(paimonCatalog, "icestream");
        flink = FlinkContext.local(2);
    }

    @AfterEach
    void tearDown() throws IOException {
        flink.close();
        try {
            paimonCatalog.close();
        } catch (Exception ignored) {
        }
        icebergCatalog.close();
    }

    @Test
    void index_writesRowsAndProducesPaimonSnapshot() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        DataFileRun run = new DataFileRun(1L, List.of(dataFile), Map.of());
        new FlinkDataFileIndexer(flink, paimonIndex).index(id, table, run, config);

        FileStoreTable indexTable = (FileStoreTable)
                paimonCatalog.getTable(Identifier.create("icestream", "db_events"));
        assertThat(indexTable.snapshotManager().latestSnapshot()).isNotNull();
        assertThat(indexTable.snapshotManager().latestSnapshot().id()).isPositive();
    }

    private DataFile writeDataFile(Table table, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newFilePath("data", "parquet"));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer = factory.newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private Record record(long id, String name) {
        Record r = GenericRecord.create(UNPARTITIONED_SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
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
}
