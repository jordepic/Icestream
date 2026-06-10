package io.github.jordepic.icestream.flinkpaimon.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.converter.ConversionCommitter;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.master.IcestreamWatermark;
import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.State;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.execution.JobClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
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
import org.apache.iceberg.data.IcebergGenerics;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.paimon.table.query.LocalTableQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end behavioral coverage for the autonomous {@link IcestreamTableJob} (HadoopCatalog + Paimon
 * FilesystemCatalog + in-process Flink MiniCluster). Each test sets up an iceberg table with data and
 * eq-delete files, submits the long-lived job, waits for its self-driven walk to reach the eq-delete
 * position (the committer-advanced watermark), then asserts the project's core invariant: <b>the
 * visible row set is identical before and after</b> the conversion — the eq-delete and the
 * positional-delete/DV it converts to delete exactly the same rows. The {@code icestream-converted}
 * tag and the removal of the eq-delete are checked alongside.
 *
 * <p>Unlike the old per-{@code create()} tests, there is no driver call to inspect a {@code CommitPlan}:
 * the job indexes the data run and converts the eq-delete run on its own, committing to iceberg.
 */
class IcestreamTableJobBehaviorTest {

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
    void v3_convertsEqDeletePreservingVisibleRows() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        appendData(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(2L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();

        assertConversionPreservesRows(id, table, new State(2L, FileKind.EQ_DEL));
    }

    @Test
    void v3_eqDeleteSpanningMultipleDataFiles() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile first = writeUnpartitionedDataFile(table, List.of(record(1L, "alice")));
        DataFile second = writeUnpartitionedDataFile(table, List.of(record(2L, "bob")));
        appendData(table, first, second);
        DeleteFile eqDelete =
                writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(1L, "x"), record(2L, "y")));
        table.newRowDelta().addDeletes(eqDelete).commit();

        assertConversionPreservesRows(id, table, new State(2L, FileKind.EQ_DEL));
    }

    @Test
    void v3_partitionedConfinedToOwnPartition() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        PartitionSpec spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build();
        Table table = icebergCatalog.createTable(id, PARTITIONED_SCHEMA, spec, v3Props());
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        DataFile salesFile = writePartitionedDataFile(table, "sales", List.of(partitionedRecord(1L, "sales")));
        appendData(table, engFile, salesFile);
        DeleteFile engEqDelete = writeEqDelete(
                table, FileFormat.PARQUET, partition("eng"), List.of(partitionedRecord(1L, "eng")));
        table.newRowDelta().addDeletes(engEqDelete).commit();

        // Only the eng row (id=1 in eng) should be deleted; the sales row (id=1 in sales) survives.
        assertConversionPreservesRows(id, table, new State(2L, FileKind.EQ_DEL));
    }

    @Test
    void v3_mergesWithExistingDvOnSameDataFile() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
        appendData(table, dataFile);
        writeAndCommitDv(table, dataFile, List.of(0L)); // existing DV removes id=1
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(3L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();

        // After conversion the merged DV must remove both position 0 (id=1) and id=3 → only id=2 visible.
        assertConversionPreservesRows(id, table, new State(3L, FileKind.EQ_DEL));
        assertMergedIntoOneDelete(table, dataFile, 2L);
    }

    @Test
    void v2_convertsToFileScopedPosDelete() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        appendData(table, dataFile);
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(2L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();

        assertConversionPreservesRows(id, table, new State(2L, FileKind.EQ_DEL));
    }

    @Test
    void v2_mergesWithExistingFileScopedPosDelete() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "alice"), record(2L, "bob"), record(3L, "carol")));
        appendData(table, dataFile);
        writeAndCommitFileScopedPosDelete(table, dataFile, List.of(0L)); // existing pos-delete removes id=1
        DeleteFile eqDelete = writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(3L, "ignored")));
        table.newRowDelta().addDeletes(eqDelete).commit();

        assertConversionPreservesRows(id, table, new State(3L, FileKind.EQ_DEL));
        assertMergedIntoOneDelete(table, dataFile, 2L);
    }

    /** Assert the data file ends up with exactly one positional delete / DV covering {@code recordCount} rows. */
    private void assertMergedIntoOneDelete(Table table, DataFile dataFile, long expectedRecordCount) {
        List<DeleteFile> referencing = currentDeletes(table).stream()
                .filter(d -> {
                    CharSequence ref = ContentFileUtil.referencedDataFile(d);
                    return ref != null && dataFile.location().contentEquals(ref);
                })
                .toList();
        assertThat(referencing)
                .as("prior and converted deletes for the data file must merge into a single delete file")
                .hasSize(1);
        assertThat(referencing.get(0).recordCount())
                .as("merged delete must cover both the prior position and the converted row")
                .isEqualTo(expectedRecordCount);
    }

    /**
     * The point of the long-lived job: across two conversions touching the same Paimon bucket with no
     * intervening index change, the lookup operator must reuse its cached (warm) lookup files rather
     * than rebuild them. We drive ONE job through two separate conversions and assert the cold
     * per-bucket build counter ({@link LocalTableQuery#COLD_BUCKET_LOADS}) does not move on the second.
     * The second eq-delete is committed only after the first conversion finishes, so the planner sees
     * two distinct EQ_DEL runs and nothing re-indexes between them.
     */
    @Test
    void warmJobReusesLookupFilesAcrossConversions() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Map<String, String> props = baseProps("3");
        props.put(IcestreamProperties.INDEX_BUCKETS, "1"); // one bucket → both conversions hit the same bucket
        Table table = icebergCatalog.createTable(id, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), props);
        DataFile dataFile = writeUnpartitionedDataFile(
                table, List.of(record(1L, "a"), record(2L, "b"), record(3L, "c"), record(4L, "d")));
        appendData(table, dataFile);
        table.newRowDelta()
                .addDeletes(writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(1L, "x"))))
                .commit();

        LocalTableQuery.COLD_BUCKET_LOADS.set(0);
        JobClient client = submitJob(id, table);
        try {
            // First conversion (cold): the lookup operator builds the bucket's lookup levels once.
            waitForWatermark(table, new State(2L, FileKind.EQ_DEL));
            long coldAfterFirst = LocalTableQuery.COLD_BUCKET_LOADS.get();
            assertThat(coldAfterFirst)
                    .as("first conversion should cold-build the touched bucket's lookup files")
                    .isGreaterThanOrEqualTo(1L);

            // Second conversion on the SAME bucket, no DATA run between → index unchanged.
            table.refresh();
            table.newRowDelta()
                    .addDeletes(writeEqDelete(table, FileFormat.PARQUET, null, List.of(record(2L, "y"))))
                    .commit();
            waitForWatermark(table, new State(3L, FileKind.EQ_DEL));

            assertThat(LocalTableQuery.COLD_BUCKET_LOADS.get())
                    .as("warm job must reuse the cached lookup files on the 2nd conversion, not rebuild them")
                    .isEqualTo(coldAfterFirst);
        } finally {
            cancel(client);
        }
    }

    /**
     * The heart of every scenario: snapshot the visible rows, run the autonomous job until it has
     * converted the eq-delete at {@code target}, then assert the visible rows are unchanged, the
     * conversion was tagged, and the eq-delete file is gone.
     */
    private void assertConversionPreservesRows(TableIdentifier id, Table table, State target) {
        List<Long> before = visibleIds(table);
        DeleteFile eqDeleteFile = currentEqDeletes(table).get(0);

        runJobUntil(id, table, target);

        table.refresh();
        assertThat(visibleIds(table))
                .as("eq-delete and its converted positional-delete/DV must delete the same rows")
                .containsExactlyInAnyOrderElementsOf(before);
        assertThat(table.currentSnapshot().summary())
                .containsEntry(ConversionCommitter.ICESTREAM_CONVERTED_SUMMARY_KEY, "true");
        assertThat(currentEqDeletes(table))
                .as("the converted eq-delete must be removed")
                .extracting(DeleteFile::location)
                .doesNotContain(eqDeleteFile.location());
    }

    private void runJobUntil(TableIdentifier id, Table table, State target) {
        JobClient client = submitJob(id, table);
        try {
            waitForWatermark(table, target);
        } finally {
            cancel(client);
        }
    }

    private JobClient submitJob(TableIdentifier id, Table table) {
        paimonIndex.initializeForTable(id, IcestreamTableConfig.from(table));
        IcebergCatalogSpec catalogSpec = new IcebergCatalogSpec(
                "org.apache.iceberg.hadoop.HadoopCatalog",
                "test",
                Map.of(CatalogProperties.WAREHOUSE_LOCATION, icebergWarehouse.toString()));
        PaimonIndexSpec indexSpec = new PaimonIndexSpec(paimonWarehouse.toUri().toString(), "icestream", Map.of());
        return IcestreamTableJob.submit(flink, paimonIndex, catalogSpec, indexSpec, id, table, 500, 200, 200, 30_000);
    }

    private void waitForWatermark(Table table, State target) {
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (true) {
                table.refresh();
                if (IcestreamWatermark.read(table.properties()).compareTo(target) >= 0) {
                    return;
                }
                if (System.nanoTime() >= deadline) {
                    throw new AssertionError("watermark did not reach " + target + " within 90s");
                }
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for watermark", e);
        }
    }

    private static void cancel(JobClient client) {
        try {
            client.cancel().get();
        } catch (Exception ignored) {
            // best-effort
        }
    }

    private List<Long> visibleIds(Table table) {
        table.refresh();
        List<Long> ids = new ArrayList<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record r : rows) {
                ids.add((Long) r.getField("id"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ids;
    }

    private List<DeleteFile> currentEqDeletes(Table table) {
        return currentDeletes(table).stream()
                .filter(d -> d.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES)
                .toList();
    }

    private List<DeleteFile> currentDeletes(Table table) {
        table.refresh();
        List<DeleteFile> out = new ArrayList<>();
        if (table.currentSnapshot() == null) {
            return out;
        }
        for (org.apache.iceberg.ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
            try (CloseableIterable<DeleteFile> iter =
                    org.apache.iceberg.ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                for (DeleteFile f : iter) {
                    out.add(f.copy());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return out;
    }

    private void appendData(Table table, DataFile... dataFiles) {
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (DataFile dataFile : dataFiles) {
            append.appendFile(dataFile);
        }
        append.commit();
        table.refresh();
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
        table.refresh();
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
        table.refresh();
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
        return baseProps("3");
    }

    private static Map<String, String> v2Props() {
        return baseProps("2");
    }

    private static Map<String, String> baseProps(String formatVersion) {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, formatVersion);
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
                    sid -> new GenericAppenderFactory(
                            table, table.schema(), spec, table.properties(), null, null, null));
            return delegate.newPosDeleteWriter(file, deleteFormat, partition);
        }

        @Override
        public DataWriter<Record> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }
    }
}
