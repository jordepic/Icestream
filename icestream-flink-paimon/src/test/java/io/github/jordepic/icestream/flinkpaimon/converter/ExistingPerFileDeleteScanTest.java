package io.github.jordepic.icestream.flinkpaimon.converter;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
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
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.StructLike;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Direct filter-logic coverage for
 * {@link io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader#scanManifest} — the scan
 * {@link RequestToExistingDeletes} drives per conversion. Exercises the predicate matrix without a
 * Flink runtime: build a real iceberg snapshot, scan its delete manifests, and inspect what's emitted.
 */
class ExistingPerFileDeleteScanTest {

    private static final Schema UNPARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));
    private static final Schema PARTITIONED_SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "dept", Types.StringType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        catalog.close();
    }

    @Test
    void v3_emitsV3EntryForPuffinDvInTouchedPartition() throws Exception {
        Table table = catalog.createTable(TABLE_ID, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v3Props());
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        table.newAppend().appendFile(dataFile).commit();
        writeAndCommitDv(table, dataFile, List.of(0L));

        List<Tuple2<String, ExistingPerFileDeletes>> emitted =
                runScan(table, 3, Set.of(unpartitionedKey(table)));

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).f0).isEqualTo(dataFile.location());
        assertThat(emitted.get(0).f1).isInstanceOf(ExistingPerFileDeletes.V3.class);
        assertThat(((ExistingPerFileDeletes.V3) emitted.get(0).f1).dv().referencedDataFile())
                .isEqualTo(dataFile.location());
    }

    @Test
    void v2_emitsV2EntryForFileScopedPosDeleteInTouchedPartition() throws Exception {
        Table table = catalog.createTable(TABLE_ID, UNPARTITIONED_SCHEMA, PartitionSpec.unpartitioned(), v2Props());
        DataFile dataFile = writeUnpartitionedDataFile(table, List.of(record(1L, "alice"), record(2L, "bob")));
        table.newAppend().appendFile(dataFile).commit();
        DeleteFile committed = writeAndCommitFileScopedPosDelete(table, dataFile, List.of(0L));

        List<Tuple2<String, ExistingPerFileDeletes>> emitted =
                runScan(table, 2, Set.of(unpartitionedKey(table)));

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).f0).isEqualTo(dataFile.location());
        assertThat(emitted.get(0).f1).isInstanceOf(ExistingPerFileDeletes.V2.class);
        ExistingPerFileDeletes.V2 v2 = (ExistingPerFileDeletes.V2) emitted.get(0).f1;
        assertThat(v2.posDeletes())
                .extracting(DeleteFile::location)
                .containsExactly(committed.location());
    }

    @Test
    void skipsScopedDeletesOutsideTouchedPartitions() throws Exception {
        Table table = catalog.createTable(
                TABLE_ID,
                PARTITIONED_SCHEMA,
                PartitionSpec.builderFor(PARTITIONED_SCHEMA).identity("dept").build(),
                v3Props());
        DataFile engFile = writePartitionedDataFile(table, "eng", List.of(partitionedRecord(1L, "eng")));
        DataFile salesFile = writePartitionedDataFile(table, "sales", List.of(partitionedRecord(2L, "sales")));
        table.newAppend().appendFile(engFile).appendFile(salesFile).commit();
        writeAndCommitDv(table, engFile, List.of(0L));
        writeAndCommitDv(table, salesFile, List.of(0L));

        List<Tuple2<String, ExistingPerFileDeletes>> emitted =
                runScan(table, 3, Set.of(partitionKey(table, "eng")));

        assertThat(emitted)
                .as("only the eng-partition DV passes; the sales partition isn't touched")
                .hasSize(1);
        assertThat(emitted.get(0).f0).isEqualTo(engFile.location());
    }

    private List<Tuple2<String, ExistingPerFileDeletes>> runScan(
            Table table, int formatVersion, Set<PartitionKey> touchedPartitions) {
        SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        List<Tuple2<String, ExistingPerFileDeletes>> emitted = new ArrayList<>();
        for (ManifestFile manifest : ExistingPerFileDeleteLoader.deleteManifests(table)) {
            ExistingPerFileDeleteLoader.scanManifest(
                    serializableTable,
                    formatVersion,
                    touchedPartitions,
                    manifest,
                    (path, entry) -> emitted.add(Tuple2.of(path, entry)));
        }
        return emitted;
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
        assertThat(ContentFileUtil.referencedDataFile(delete)).isNotNull();
        table.newRowDelta().addDeletes(delete).commit();
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

    private PartitionKey unpartitionedKey(Table table) {
        PartitionSpec spec = table.spec();
        byte[] encoded = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), null);
        return new PartitionKey(spec.specId(), encoded);
    }

    private PartitionKey partitionKey(Table table, String dept) {
        PartitionSpec spec = table.spec();
        byte[] encoded = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), partition(dept));
        return new PartitionKey(spec.specId(), encoded);
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

    private String newPath(String prefix, String ext) {
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private static Map<String, String> v3Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        return props;
    }

    private static Map<String, String> v2Props() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
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
                    id -> new GenericAppenderFactory(
                            table, table.schema(), spec, table.properties(), null, null, null));
            return delegate.newPosDeleteWriter(file, deleteFormat, partition);
        }

        @Override
        public DataWriter<Record> newDataWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException();
        }
    }
}
