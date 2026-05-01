package io.github.jordepic.icestream.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConversionCommitterTest {

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "t");
    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PK_SCHEMA = new Schema(NestedField.required(1, "id", Types.LongType.get()));

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private Table table;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), v3Props());
    }

    @AfterEach
    void teardown() throws IOException {
        catalog.close();
    }

    @Test
    void commit_removesEqDeleteAndAddsNewDv() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a"), record(2L, "b")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(1L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile newDv = writeDvOutOfBand(dataFile, List.of(0L));

        ConversionCommitter.commit(
                table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of(newDv)));

        table.refresh();
        List<DeleteFile> liveDeletes = liveDeleteFiles();
        assertThat(liveDeletes).extracting(DeleteFile::location).containsExactly(newDv.location());
        assertThat(liveDeletes).extracting(DeleteFile::format).containsExactly(FileFormat.PUFFIN);
    }

    @Test
    void commit_setsIcestreamConvertedSummary() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(1L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile newDv = writeDvOutOfBand(dataFile, List.of(0L));

        ConversionCommitter.commit(
                table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of(newDv)));

        table.refresh();
        assertThat(table.currentSnapshot().summary())
                .containsEntry(ConversionCommitter.ICESTREAM_CONVERTED_SUMMARY_KEY, "true");
    }

    @Test
    void commit_removesExistingDvAndAddsMergedDv() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
        table.newAppend().appendFile(dataFile).commit();
        DeleteFile existingDv = writeAndCommitDv(dataFile, List.of(0L));
        DeleteFile eqDelete = writeEqDelete(List.of(record(2L, null)));
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile mergedDv = writeDvOutOfBand(dataFile, List.of(0L, 1L));

        ConversionCommitter.commit(
                table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(existingDv), List.of(mergedDv)));

        table.refresh();
        List<DeleteFile> liveDeletes = liveDeleteFiles();
        assertThat(liveDeletes).extracting(DeleteFile::location).containsExactly(mergedDv.location());
    }

    @Test
    void nothingToConvert_stillCommitsEqDeleteRemoval() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(99L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();

        ConversionCommitter.commit(
                table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of()));

        table.refresh();
        assertThat(liveDeleteFiles()).isEmpty();
    }

    @Test
    void noOpPlan_isShortCircuited() {
        long snapshotId = table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();

        ConversionCommitter.commit(table, new CommitPlan(snapshotId, List.of(), List.of(), List.of()));

        table.refresh();
        long after = table.currentSnapshot() == null ? -1 : table.currentSnapshot().snapshotId();
        assertThat(after).isEqualTo(snapshotId);
    }

    @Test
    void eqDeleteRemovedConcurrently_commitFailsWithValidationException() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(1L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile newDv = writeDvOutOfBand(dataFile, List.of(0L));

        table.newRewrite().deleteFile(eqDelete).commit();

        assertThatThrownBy(() -> ConversionCommitter.commit(
                        table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of(newDv))))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void existingDvRemovedConcurrently_commitFailsWithValidationException() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
        table.newAppend().appendFile(dataFile).commit();
        DeleteFile existingDv = writeAndCommitDv(dataFile, List.of(0L));
        DeleteFile eqDelete = writeEqDelete(List.of(record(2L, null)));
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile mergedDv = writeDvOutOfBand(dataFile, List.of(0L, 1L));

        table.newRewrite().deleteFile(existingDv).commit();

        assertThatThrownBy(() -> ConversionCommitter.commit(
                        table,
                        new CommitPlan(planSnapshot, List.of(eqDelete), List.of(existingDv), List.of(mergedDv))))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void conflictingDvAddedConcurrently_commitFailsWithValidationException() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a"), record(2L, "b")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(1L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile newDv = writeDvOutOfBand(dataFile, List.of(0L));

        writeAndCommitDv(dataFile, List.of(1L));

        assertThatThrownBy(() -> ConversionCommitter.commit(
                        table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of(newDv))))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void dataFileRemovedConcurrently_commitStillSucceeds() throws IOException {
        DataFile dataFile = writeDataFile(List.of(record(1L, "a")));
        DeleteFile eqDelete = writeEqDelete(List.of(record(1L, null)));
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(eqDelete).commit();
        long planSnapshot = table.currentSnapshot().snapshotId();
        DeleteFile newDv = writeDvOutOfBand(dataFile, List.of(0L));

        table.newDelete().deleteFile(dataFile).commit();

        ConversionCommitter.commit(
                table, new CommitPlan(planSnapshot, List.of(eqDelete), List.of(), List.of(newDv)));

        table.refresh();
        assertThat(table.newScan().planFiles()).isEmpty();
    }

    private List<DeleteFile> liveDeleteFiles() {
        return StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
                .flatMap(t -> t.deletes().stream())
                .toList();
    }

    private DataFile writeDataFile(List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("data", "parquet"));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("eqdel", "parquet"));
        GenericAppenderFactory factory =
                new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, PK_SCHEMA, null);
        EqualityDeleteWriter<Record> writer = factory.newEqDeleteWriter(
                EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        try (Closeable toClose = writer) {
            for (Record row : rows) {
                Record key = GenericRecord.create(PK_SCHEMA);
                key.setField("id", row.getField("id"));
                writer.write(key);
            }
        }
        return writer.toDeleteFile();
    }

    /** Writes a DV without committing — used to stand in for a DV that {@code DeleteFileCreator} would have produced. */
    private DeleteFile writeDvOutOfBand(DataFile dataFile, List<Long> positions) throws IOException {
        return buildDv(dataFile, positions);
    }

    private DeleteFile writeAndCommitDv(DataFile dataFile, List<Long> positions) throws IOException {
        DeleteFile dv = buildDv(dataFile, positions);
        table.newRowDelta().addDeletes(dv).commit();
        return dv;
    }

    private DeleteFile buildDv(DataFile dataFile, List<Long> positions) throws IOException {
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

    private Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        return props;
    }
}
