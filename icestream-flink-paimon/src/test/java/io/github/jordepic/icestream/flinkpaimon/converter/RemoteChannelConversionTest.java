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
 * Focused end-to-end proof of the <b>channel transport</b>: the standing streaming job's source and
 * writer reach the driver's channel state over HTTP via {@link RemoteConversionChannelClient}, so
 * {@code /channel/poll}, {@code /channel/inflight} and {@code /channel/complete} — plus the
 * over-the-wire serialization of {@code ConversionRequest} and {@code TaskOutputs} — are all
 * exercised exactly as they would be on a remote session cluster. The converter hosts its own channel
 * server (the only transport; there is no in-process shortcut).
 *
 * <p>The Flink job still runs in a local MiniCluster (operators in this JVM), so this isn't physical
 * process separation; but the transport code path is identical, and for a same-host standalone
 * cluster the per-conversion networking IS loopback, so this is a faithful proxy.
 */
class RemoteChannelConversionTest {

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
    private StreamingFlinkDeleteFileCreator converter;
    private FlinkDataFileIndexer indexer;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        icebergCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse.toString());
        paimonIndex = PaimonIndex.create(paimonWarehouse.toUri().toString(), "icestream", Map.of());
        flink = FlinkContext.local(2);
        // Converter hosts its own channel server on an ephemeral loopback port; the standing job's
        // operators reach it over HTTP exactly as a remote cluster's TaskManagers would.
        converter = new StreamingFlinkDeleteFileCreator(flink, paimonIndex, 500, 120_000);
        indexer = new FlinkDataFileIndexer(flink, paimonIndex);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (converter != null) {
            converter.close();
        }
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
    void convertOverRemoteHttpChannel() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Schema schemaWithId = new Schema(SCHEMA.columns(), java.util.Set.of(SCHEMA.findField("id").fieldId()));
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "2");
        Table table = icebergCatalog.createTable(id, schemaWithId, PartitionSpec.unpartitioned(), props);
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        DataFile dataFile = writeDataFile(table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        table.newAppend().appendFile(dataFile).commit();
        table.refresh();

        indexer.index(
                id, table, new DataFileRun(table.currentSnapshot().sequenceNumber(), List.of(dataFile), Map.of()), config);

        // The conversion's probes flow to the standing job's source over /channel/poll, and the
        // resulting TaskOutputs come back over /channel/complete — both across the HTTP boundary.
        DeleteFile eqDelete = writeEqDelete(table, List.of(record(2L, "ignored")));
        CommitPlan plan = converter.create(
                id, table, new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete)), config);

        assertThat(plan.deletesToAdd()).hasSize(1);
        assertThat(plan.deletesToAdd().get(0).recordCount()).isEqualTo(1L);
        assertThat(org.apache.iceberg.util.ContentFileUtil.referencedDataFile(plan.deletesToAdd().get(0)).toString())
                .isEqualTo(dataFile.location());
    }

    private DataFile writeDataFile(Table table, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(
                icebergWarehouse.resolve("data-" + pathCounter.incrementAndGet() + ".parquet").toString());
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(Table table, List<Record> rows) throws IOException {
        Schema deleteSchema = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(
                icebergWarehouse.resolve("eq-" + pathCounter.incrementAndGet() + ".parquet").toString());
        GenericAppenderFactory factory =
                new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
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
        Record r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("name", name);
        return r;
    }
}
