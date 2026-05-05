package io.github.jordepic.icestream.flinkpaimon.converter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonCatalogFactory;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Smoke test for the Flink+Paimon converter: indexes a data file, then converts an eq-delete
 * file targeting one of the indexed pks, and asserts the resulting {@link CommitPlan} carries
 * the right shape.
 */
class FlinkDeleteFileCreatorTest {

    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));

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
        DataFile dataFile = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        table.newAppend().appendFile(dataFile).commit();
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        new FlinkDataFileIndexer(flink, paimonIndex)
                .index(id, table, new DataFileRun(1L, List.of(dataFile), Map.of()), config);

        DeleteFile eqDelete = writeEqDelete(table, List.of(record(2L, "ignored")));
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

    private DeleteFile writeEqDelete(Table table, List<Record> rows) throws IOException {
        Schema deleteSchema = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(newFilePath("eq-delete", "parquet"));
        GenericAppenderFactory factory = new GenericAppenderFactory(
                table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
        EqualityDeleteWriter<Record> writer = factory.newEqDeleteWriter(
                EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            for (Record row : rows) {
                Record projected = GenericRecord.create(deleteSchema);
                projected.setField("id", row.getField("id"));
                writer.write(projected);
            }
        }
        return writer.toDeleteFile();
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
