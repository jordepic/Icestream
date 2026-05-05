package io.github.jordepic.icestream.sparkcassandra.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.FileRun;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import io.github.jordepic.icestream.planner.State;

class SnapshotPlannerTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("db", "t");
    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()));

    @TempDir
    Path warehouse;

    private HadoopCatalog catalog;
    private final SnapshotPlanner planner = new SnapshotPlanner();
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setup() {
        catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
    }

    @AfterEach
    void teardown() throws IOException {
        catalog.close();
    }

    @Test
    void emptyTable_returnsNoBlocks() {
        Table table = createV3Table();

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).isEmpty();
    }

    @Test
    void onlyDataFilesAcrossSeqs_returnsSingleDataBlock() {
        Table table = createV3Table();
        appendData(table, dataFile());
        appendData(table, dataFile());
        appendData(table, dataFile());

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, b -> {
            assertThat(b.files()).hasSize(3);
            assertThat(b.maxSeq()).isEqualTo(3L);
        });
    }

    @Test
    void onlyConvertibleEqDeletes_returnsSingleDeleteBlock() {
        Table table = createV3Table();
        appendDeletes(table, eqDeleteWithFieldIds(1));
        appendDeletes(table, eqDeleteWithFieldIds(1));

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(EqualityDeleteFileRun.class, b -> {
            assertThat(b.files()).hasSize(2);
            assertThat(b.maxSeq()).isEqualTo(2L);
        });
    }

    @Test
    void dataRunFollowedByDeleteRun_returnsTwoBlocks() {
        Table table = createV3Table();
        appendData(table, dataFile());
        appendData(table, dataFile());
        appendData(table, dataFile());
        appendDeletes(table, eqDeleteWithFieldIds(1));
        appendDeletes(table, eqDeleteWithFieldIds(1));
        appendDeletes(table, eqDeleteWithFieldIds(1));

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).hasSize(2);
        assertThat(fileRuns.get(0)).isInstanceOfSatisfying(DataFileRun.class, b -> {
            assertThat(b.files()).hasSize(3);
            assertThat(b.maxSeq()).isEqualTo(3L);
        });
        assertThat(fileRuns.get(1)).isInstanceOfSatisfying(EqualityDeleteFileRun.class, b -> {
            assertThat(b.files()).hasSize(3);
            assertThat(b.maxSeq()).isEqualTo(6L);
        });
    }

    @Test
    void interleavedKinds_returnsAlternatingSingletons() {
        Table table = createV3Table();
        appendData(table, dataFile());
        appendDeletes(table, eqDeleteWithFieldIds(1));
        appendData(table, dataFile());
        appendDeletes(table, eqDeleteWithFieldIds(1));

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).hasSize(4);
        assertThat(fileRuns).extracting(FileRun::kind).containsExactly(FileKind.DATA, FileKind.EQ_DEL, FileKind.DATA, FileKind.EQ_DEL);
        assertThat(fileRuns).extracting(FileRun::maxSeq).containsExactly(1L, 2L, 3L, 4L);
    }

    @Test
    void sameSeqHasDeleteBeforeData() {
        Table table = createV3Table();
        table.newRowDelta()
                .addRows(dataFile())
                .addDeletes(eqDeleteWithFieldIds(1))
                .commit();

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).hasSize(2);
        assertThat(fileRuns.get(0).kind()).isEqualTo(FileKind.EQ_DEL);
        assertThat(fileRuns.get(0).maxSeq()).isEqualTo(1L);
        assertThat(fileRuns.get(1).kind()).isEqualTo(FileKind.DATA);
        assertThat(fileRuns.get(1).maxSeq()).isEqualTo(1L);
    }

    @Test
    void resumingFromDelState_includesSameSeqData() {
        Table table = createV3Table();
        table.newRowDelta()
                .addRows(dataFile())
                .addDeletes(eqDeleteWithFieldIds(1))
                .commit();

        List<FileRun> fileRuns = planner.plan(table, new State(1L, FileKind.EQ_DEL));

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, b -> {
            assertThat(b.maxSeq()).isEqualTo(1L);
            assertThat(b.files()).hasSize(1);
        });
    }

    @Test
    void resumingFromDataState_excludesSameSeqWalkEntries() {
        Table table = createV3Table();
        table.newRowDelta()
                .addRows(dataFile())
                .addDeletes(eqDeleteWithFieldIds(1))
                .commit();

        List<FileRun> fileRuns = planner.plan(table, new State(1L, FileKind.DATA));

        assertThat(fileRuns).isEmpty();
    }

    @Test
    void eqDeleteWithMismatchedSchema_filteredDoesNotSplitDataBlock() {
        Table table = createV3Table();
        appendData(table, dataFile());
        appendDeletes(table, eqDeleteWithFieldIds(2));
        appendData(table, dataFile());

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, b -> {
            assertThat(b.files()).hasSize(2);
            assertThat(b.maxSeq()).isEqualTo(3L);
        });
    }

    @Test
    void deletionVector_notInWalk_doesNotSplitDataBlock() {
        Table table = createV3Table();
        DataFile first = dataFile();
        appendData(table, first);
        appendDeletes(table, deletionVectorFor(first.path().toString()));
        appendData(table, dataFile());

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, b -> {
            assertThat(b.files()).hasSize(2);
            assertThat(b.maxSeq()).isEqualTo(3L);
        });
    }

    @Test
    void deletionVectorInRunWindow_attachedToReferencedDataFile() {
        Table table = createV3Table();
        DataFile a = dataFile();
        DataFile b = dataFile();
        DeleteFile dvForA = deletionVectorFor(a.location());
        table.newRowDelta().addRows(a).addRows(b).addDeletes(dvForA).commit();

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, run -> {
            assertThat(locationsOf(run, a)).containsExactly(dvForA.location());
            assertThat(locationsOf(run, b)).isEmpty();
        });
    }

    @Test
    void deletionVectorAfterRunWindow_notAttached() {
        Table table = createV3Table();
        DataFile a = dataFile();
        appendData(table, a);
        appendDeletes(table, eqDeleteWithFieldIds(1));
        appendDeletes(table, deletionVectorFor(a.location()));

        List<FileRun> fileRuns = planner.plan(table, State.INITIAL);

        assertThat(fileRuns).hasSize(2);
        assertThat(fileRuns.get(0)).isInstanceOfSatisfying(DataFileRun.class, run -> {
            assertThat(run.maxSeq()).isEqualTo(1L);
            assertThat(locationsOf(run, a)).isEmpty();
        });
    }

    @Test
    void deletionVectorBeforeResumePoint_notAttached() {
        Table table = createV3Table();
        DataFile a = dataFile();
        DeleteFile dvForA = deletionVectorFor(a.location());
        table.newRowDelta().addRows(a).addDeletes(dvForA).commit();
        DataFile b = dataFile();
        appendData(table, b);

        List<FileRun> fileRuns = planner.plan(table, new State(1L, FileKind.DATA));

        assertThat(fileRuns).singleElement().isInstanceOfSatisfying(DataFileRun.class, run -> {
            assertThat(run.files()).extracting(DataFile::location).containsExactly(b.location());
            assertThat(locationsOf(run, b)).isEmpty();
        });
    }

    private static List<String> locationsOf(DataFileRun run, DataFile file) {
        return run.deletesFor(file).stream().map(DeleteFile::location).toList();
    }

    @Test
    void v1Table_failsLoud() {
        Map<String, String> props = defaultProps();
        props.put(TableProperties.FORMAT_VERSION, "1");
        Table table = catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), props);

        assertThatThrownBy(() -> planner.plan(table, State.INITIAL))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("format-version");
    }

    @Test
    void pkChangedAfterPinning_failsLoud() {
        Table table = createV3Table();
        pinCurrentConfig(table);
        table.updateProperties()
                .set(IcestreamProperties.PRIMARY_KEYS, "name")
                .commit();

        assertThatThrownBy(() -> planner.plan(table, State.INITIAL))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary-keys");
    }

    @Test
    void bucketsChangedAfterPinning_failsLoud() {
        Table table = createV3Table();
        pinCurrentConfig(table);
        table.updateProperties()
                .set(IcestreamProperties.INDEX_BUCKETS, "8")
                .commit();

        assertThatThrownBy(() -> planner.plan(table, State.INITIAL))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("index-buckets");
    }

    @Test
    void planNextRun_returnsFirstRunInOrder() {
        Table table = createV3Table();
        appendData(table, dataFile());
        appendDeletes(table, eqDeleteWithFieldIds(1));

        Optional<FileRun> next = planner.planNextRun(table, State.INITIAL);

        assertThat(next).hasValueSatisfying(run -> {
            assertThat(run.kind()).isEqualTo(FileKind.DATA);
            assertThat(run.maxSeq()).isEqualTo(1L);
        });
    }

    @Test
    void planNextRun_emptyWhenNoWork() {
        Table table = createV3Table();

        Optional<FileRun> next = planner.planNextRun(table, State.INITIAL);

        assertThat(next).isEmpty();
    }

    private Table createV3Table() {
        return catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), defaultProps());
    }

    private Map<String, String> defaultProps() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "4");
        return props;
    }

    private void pinCurrentConfig(Table table) {
        table.updateProperties()
                .set(IcestreamProperties.PINNED_PRIMARY_KEYS, table.properties().get(IcestreamProperties.PRIMARY_KEYS))
                .set(
                        IcestreamProperties.PINNED_INDEX_BUCKETS,
                        table.properties().get(IcestreamProperties.INDEX_BUCKETS))
                .commit();
    }

    private DataFile dataFile() {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(newPath("data"))
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1L)
                .withRecordCount(1L)
                .build();
    }

    private DeleteFile eqDeleteWithFieldIds(int... fieldIds) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(fieldIds)
                .withPath(newPath("eqdel"))
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1L)
                .withRecordCount(1L)
                .build();
    }

    private DeleteFile deletionVectorFor(String referencedDataFilePath) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(newPath("dv") + ".puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(1L)
                .withRecordCount(1L)
                .withReferencedDataFile(referencedDataFilePath)
                .withContentOffset(0L)
                .withContentSizeInBytes(1L)
                .build();
    }

    private String newPath(String prefix) {
        return warehouse.resolve(prefix + "-" + pathCounter.incrementAndGet() + ".parquet")
                .toString();
    }

    private void appendData(Table table, DataFile... files) {
        AppendFiles append = table.newAppend();
        for (DataFile f : files) {
            append.appendFile(f);
        }
        append.commit();
    }

    private void appendDeletes(Table table, DeleteFile... files) {
        RowDelta delta = table.newRowDelta();
        for (DeleteFile f : files) {
            delta.addDeletes(f);
        }
        delta.commit();
    }
}
