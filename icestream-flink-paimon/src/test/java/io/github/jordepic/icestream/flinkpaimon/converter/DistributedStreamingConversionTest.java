package io.github.jordepic.icestream.flinkpaimon.converter;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the <b>distributed</b> streaming convert pipeline: parallel eq-delete reads (rebalanced
 * across reader subtasks) and parallel positional-delete writes (match rows keyed by data_file_path
 * across writer subtasks), with the parallelism-1 collector aggregating every subtask's slice for the
 * conversion via checkpoint-barrier alignment.
 *
 * <p>The setup forces fan-out on both ends: 4 data files (so the keyed writer spreads delete-file
 * writing across subtasks) and 2 eq-delete files (so the rebalanced reader spreads file reads), run
 * at parallelism 4. The assertion is end-to-end correctness — every affected data file gets exactly
 * its deleted positions back in the assembled {@link CommitPlan}, regardless of how the work spread.
 */
class DistributedStreamingConversionTest {

    private static final Schema SCHEMA = new Schema(
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
        flink = FlinkContext.local(4); // parallelism 4 → reads + writes actually fan out
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flink != null) {
            flink.close();
        }
        try {
            paimonIndex.catalog().close();
        } catch (Exception ignored) {
            // best-effort
        }
        icebergCatalog.close();
    }

    @Test
    void distributedConversionAcrossFilesAndDeleteFiles() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Schema schemaWithId = new Schema(SCHEMA.columns(), Set.of(SCHEMA.findField("id").fieldId()));
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "4");
        Table table = icebergCatalog.createTable(id, schemaWithId, PartitionSpec.unpartitioned(), props);
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        // 4 data files, 100 ids each: f0=[0,100) f1=[100,200) f2=[200,300) f3=[300,400)
        List<DataFile> dataFiles = new ArrayList<>();
        for (int f = 0; f < 4; f++) {
            dataFiles.add(writeDataFile(table, f * 100L, 100));
        }
        table.newAppend().appendFile(dataFiles.get(0)).appendFile(dataFiles.get(1))
                .appendFile(dataFiles.get(2)).appendFile(dataFiles.get(3)).commit();
        table.refresh();

        new FlinkDataFileIndexer(flink, paimonIndex)
                .index(id, table, new DataFileRun(table.currentSnapshot().sequenceNumber(), dataFiles, Map.of()), config);
        table.refresh();

        // 2 eq-delete files; deleted ids span all 4 data files.
        DeleteFile eqA = writeEqDelete(table, Set.of(1L, 2L, 101L));   // f0:{1,2}, f1:{101}
        DeleteFile eqB = writeEqDelete(table, Set.of(250L, 350L, 351L)); // f2:{250}, f3:{350,351}
        EqualityDeleteFileRun run =
                new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqA, eqB));

        CommitPlan plan;
        try (StreamingFlinkDeleteFileCreator converter =
                new StreamingFlinkDeleteFileCreator(flink, paimonIndex, 500, 120_000)) {
            plan = converter.create(id, table, run, config);
        }

        // Total positions across all produced delete files == number of deleted ids (6).
        long totalPositions = plan.deletesToAdd().stream().mapToLong(DeleteFile::recordCount).sum();
        assertThat(totalPositions).as("all deleted ids convert to positions").isEqualTo(6L);

        // Each affected data file is referenced, with the expected per-file count.
        Map<String, Long> perFile = new HashMap<>();
        for (DeleteFile df : plan.deletesToAdd()) {
            String ref = ContentFileUtil.referencedDataFile(df).toString();
            perFile.merge(ref, df.recordCount(), Long::sum);
        }
        assertThat(perFile.get(dataFiles.get(0).location())).isEqualTo(2L);
        assertThat(perFile.get(dataFiles.get(1).location())).isEqualTo(1L);
        assertThat(perFile.get(dataFiles.get(2).location())).isEqualTo(1L);
        assertThat(perFile.get(dataFiles.get(3).location())).isEqualTo(2L);
        assertThat(perFile.keySet()).as("exactly the 4 data files are touched").hasSize(4);
    }

    private DataFile writeDataFile(Table table, long startId, int count) throws IOException {
        OutputFile out = table.io().newOutputFile(
                icebergWarehouse.resolve("data-" + pathCounter.incrementAndGet() + ".parquet").toString());
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            for (long i = 0; i < count; i++) {
                writer.write(record(startId + i, "name-" + (startId + i)));
            }
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(Table table, Set<Long> ids) throws IOException {
        Schema deleteSchema = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(
                icebergWarehouse.resolve("eq-" + pathCounter.incrementAndGet() + ".parquet").toString());
        GenericAppenderFactory factory =
                new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            for (long pk : new LinkedHashSet<>(ids)) {
                Record projected = GenericRecord.create(deleteSchema);
                projected.setField("id", pk);
                writer.write(projected);
            }
        }
        return writer.toDeleteFile();
    }

    private Record record(long id, String name) {
        Record r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
        return r;
    }
}
