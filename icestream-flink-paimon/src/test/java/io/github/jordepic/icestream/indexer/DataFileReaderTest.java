package io.github.jordepic.icestream.indexer;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
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
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import io.github.jordepic.icestream.indexer.DataFileReader;

class DataFileReaderTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");
    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private Table table;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        table = catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), v3Properties());
    }

    @AfterEach
    void teardown() throws IOException {
        catalog.close();
    }

    @ParameterizedTest
    @EnumSource(
            value = FileFormat.class,
            names = {"PARQUET", "AVRO", "ORC"})
    void noDeletes_returnsAllRows(FileFormat format) throws IOException {
        DataFile dataFile = writeDataFile(format, List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));

        List<Record> rows = readRecords(dataFile, List.of());

        assertThat(rows).extracting(r -> r.getField("id"), r -> r.getField("name"))
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1L, "a"),
                        org.assertj.core.groups.Tuple.tuple(2L, "b"),
                        org.assertj.core.groups.Tuple.tuple(3L, "c"));
    }

    @ParameterizedTest
    @EnumSource(
            value = FileFormat.class,
            names = {"PARQUET", "AVRO", "ORC"})
    void deletionVectorFiltersReferencedPositions(FileFormat format) throws IOException {
        DataFile dataFile = writeDataFile(format, List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
        DeleteFile dv = writeDV(dataFile, List.of(1L));

        List<Record> rows = readRecords(dataFile, List.of(dv));

        assertThat(rows).extracting(r -> r.getField("id"), r -> r.getField("name"))
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1L, "a"),
                        org.assertj.core.groups.Tuple.tuple(3L, "c"));
    }

    @ParameterizedTest
    @EnumSource(
            value = FileFormat.class,
            names = {"PARQUET", "AVRO", "ORC"})
    void multipleOverlappingPosDeleteFilesMergeCorrectly(FileFormat posDeleteFormat) throws IOException {
        DataFile dataFile = writeDataFile(List.of(
                record(10L, "a"),
                record(20L, "b"),
                record(30L, "c"),
                record(40L, "d"),
                record(50L, "e"),
                record(60L, "f")));
        DeleteFile posDel1 = writePosDeleteFile(posDeleteFormat, dataFile, List.of(0L, 2L));
        DeleteFile posDel2 = writePosDeleteFile(posDeleteFormat, dataFile, List.of(2L, 4L));
        DeleteFile posDel3 = writePosDeleteFile(posDeleteFormat, dataFile, List.of(4L, 5L));

        List<Record> rows = readRecords(dataFile, List.of(posDel1, posDel2, posDel3));

        assertThat(rows).extracting(r -> r.getField("id"), r -> r.getField("name"))
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(20L, "b"),
                        org.assertj.core.groups.Tuple.tuple(40L, "d"));
    }

    private List<Record> readRecords(DataFile dataFile, List<DeleteFile> deletes) throws IOException {
        List<Record> rows = new ArrayList<>();
        try (var iter = DataFileReader.read(table.io(), dataFile, deletes, SCHEMA)) {
            iter.forEach(rows::add);
        }
        return rows;
    }

    private DataFile writeDataFile(List<Record> rows) throws IOException {
        return writeDataFile(FileFormat.PARQUET, rows);
    }

    private DataFile writeDataFile(FileFormat format, List<Record> rows) throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("data", format.name().toLowerCase()));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), format, null);
        try (Closeable toClose = writer) {
            writer.write(rows);
        }
        return writer.toDataFile();
    }

    private DeleteFile writeDV(DataFile dataFile, List<Long> deletedPositions) throws IOException {
        OutputFileFactory fileFactory =
                OutputFileFactory.builderFor(table, 1, pathCounter.incrementAndGet())
                        .format(FileFormat.PUFFIN)
                        .build();
        DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
        try (DVFileWriter closeableWriter = writer) {
            for (Long pos : deletedPositions) {
                closeableWriter.delete(dataFile.location(), pos, table.spec(), null);
            }
        }
        return Iterables.getOnlyElement(writer.result().deleteFiles());
    }

    private DeleteFile writePosDeleteFile(FileFormat format, DataFile dataFile, List<Long> deletedPositions)
            throws IOException {
        OutputFile out = table.io().newOutputFile(newPath("posdel", format.name().toLowerCase()));
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        PositionDeleteWriter<Record> writer =
                factory.newPosDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), format, null);
        PositionDelete<Record> delete = PositionDelete.create();
        try (Closeable toClose = writer) {
            for (Long pos : deletedPositions) {
                writer.write(delete.set(dataFile.location(), pos));
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

    private String newPath(String prefix, String ext) {
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private Map<String, String> v3Properties() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        return props;
    }
}
