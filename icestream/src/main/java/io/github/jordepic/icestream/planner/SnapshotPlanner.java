package io.github.jordepic.icestream.planner;

import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import io.github.jordepic.icestream.schema.PrimaryKeySchema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

public final class SnapshotPlanner {

    private static final int MINIMUM_REQUIRED_FORMAT_VERSION = 2;

    public Optional<FileRun> planNextRun(Table table, State lastProcessed) {
        return plan(table, lastProcessed).stream().findFirst();
    }

    public List<FileRun> plan(Table table, State lastProcessed) {
        validateFormatVersion(table);
        validateStability(table);
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return List.of();
        }
        CollectedFiles files = collectFiles(table, snapshot, config.primaryKey());
        return groupIntoRuns(buildWalk(files, lastProcessed), files.positionDeletes, lastProcessed);
    }

    private void validateFormatVersion(Table table) {
        int version = ((HasTableOperations) table).operations().current().formatVersion();
        if (version < MINIMUM_REQUIRED_FORMAT_VERSION) {
            throw new IllegalStateException(
                    "Table " + table.name() + " format-version is " + version + "; icestream requires v2 or higher");
        }
    }

    private void validateStability(Table table) {
        String pinnedPk = table.properties().get(IcestreamProperties.PINNED_PRIMARY_KEYS);
        String currentPk = table.properties().get(IcestreamProperties.PRIMARY_KEYS);
        if (pinnedPk != null && !pinnedPk.equals(currentPk)) {
            throw new IllegalStateException(
                    "Table " + table.name() + " primary-keys changed from '" + pinnedPk + "' to '" + currentPk + "'");
        }
        String pinnedBuckets = table.properties().get(IcestreamProperties.PINNED_CASSANDRA_BUCKETS);
        String currentBuckets = table.properties().get(IcestreamProperties.CASSANDRA_BUCKETS);
        if (pinnedBuckets != null && !pinnedBuckets.equals(currentBuckets)) {
            throw new IllegalStateException("Table " + table.name() + " cassandra-buckets changed from '" + pinnedBuckets
                    + "' to '" + currentBuckets + "'");
        }
    }

    private CollectedFiles collectFiles(Table table, Snapshot snapshot, PrimaryKeySchema pk) {
        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> eqDeleteFiles = new ArrayList<>();
        List<DeleteFile> positionDeleteFiles = new ArrayList<>();
        boolean tablePartitioned = table.spec().isPartitioned();
        Set<Integer> pkFieldIds = pk.fieldIds();
        try {
            for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
                try (CloseableIterable<DataFile> iter = ManifestFiles.read(manifest, table.io(), table.specs())) {
                    for (DataFile f : iter) {
                        dataFiles.add(f.copy());
                    }
                }
            }
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                try (CloseableIterable<DeleteFile> iter =
                        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                    for (DeleteFile f : iter) {
                        if (isConvertibleEqDelete(f, table, pkFieldIds, tablePartitioned)) {
                            eqDeleteFiles.add(f.copy());
                        } else if (f.content() == FileContent.POSITION_DELETES) {
                            positionDeleteFiles.add(f.copy());
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading manifests for " + table.name(), e);
        }
        return new CollectedFiles(dataFiles, eqDeleteFiles, positionDeleteFiles);
    }

    // Only convert primary key equality deletes that are partition scoped
    private boolean isConvertibleEqDelete(
            DeleteFile file, Table table, Set<Integer> pkFieldIds, boolean tablePartitioned) {
        if (file.content() != FileContent.EQUALITY_DELETES) {
            return false;
        }
        if (!Set.copyOf(file.equalityFieldIds()).equals(pkFieldIds)) {
            return false;
        }
        boolean filePartitioned = table.specs().get(file.specId()).isPartitioned();
        return filePartitioned == tablePartitioned;
    }

    private List<WalkEntry> buildWalk(CollectedFiles files, State lastProcessed) {
        Stream<WalkEntry> dataEntries = files.data.stream()
                .map(f -> new DataWalkEntry(
                        new State(Objects.requireNonNull(f.dataSequenceNumber()), FileKind.DATA), f));
        Stream<WalkEntry> deleteEntries = files.eqDeletes.stream()
                .map(f -> new EqualityDeleteWalkEntry(
                        new State(Objects.requireNonNull(f.dataSequenceNumber()), FileKind.EQ_DEL), f));
        return Stream.concat(dataEntries, deleteEntries)
                .filter(e -> e.position().compareTo(lastProcessed) > 0)
                .sorted(Comparator.comparing(WalkEntry::position))
                .toList();
    }

    private List<FileRun> groupIntoRuns(
            List<WalkEntry> entries, List<DeleteFile> positionDeletes, State lastProcessed) {
        if (entries.isEmpty()) {
            return List.of();
        }
        List<FileRun> runs = new ArrayList<>();
        long prevMaxSeq = lastProcessed.sequenceNumber();
        int runStart = 0;
        while (runStart < entries.size()) {
            int runEnd = runStart + 1;
            while (runEnd < entries.size() && entries.get(runEnd).getClass() == entries.get(runStart).getClass()) {
                runEnd++;
            }
            List<WalkEntry> run = entries.subList(runStart, runEnd);
            long maxSeq = run.get(run.size() - 1).position().sequenceNumber();
            runs.add(toFileRun(run, prevMaxSeq, maxSeq, positionDeletes));
            prevMaxSeq = maxSeq;
            runStart = runEnd;
        }
        return runs;
    }

    private FileRun toFileRun(
            List<WalkEntry> run, long prevMaxSeq, long maxSeq, List<DeleteFile> positionDeletes) {
        if (run.get(0) instanceof DataWalkEntry) {
            List<DataFile> files = run.stream()
                    .map(e -> ((DataWalkEntry) e).file())
                    .toList();
            return new DataFileRun(maxSeq, files, matchDeletesForRun(files, positionDeletes, prevMaxSeq, maxSeq));
        }
        List<DeleteFile> files = run.stream()
                .map(e -> ((EqualityDeleteWalkEntry) e).file())
                .toList();
        return new EqualityDeleteFileRun(maxSeq, files);
    }

    private Map<String, List<DeleteFile>> matchDeletesForRun(
            List<DataFile> dataFiles, List<DeleteFile> positionDeletes, long lowExclusive, long highInclusive) {
        Set<String> runFilePaths = dataFiles.stream().map(DataFile::location).collect(Collectors.toSet());
        Map<String, List<DeleteFile>> byReferencedFile = positionDeletes.stream()
                .filter(del -> {
                    long seq = Objects.requireNonNull(del.dataSequenceNumber());
                    return seq > lowExclusive && seq <= highInclusive;
                })
                .filter(del -> del.referencedDataFile() != null)
                .collect(Collectors.groupingBy(DeleteFile::referencedDataFile));
        byReferencedFile.keySet().retainAll(runFilePaths);
        return byReferencedFile;
    }

    // positionDeletes includes positional deletes and deletion vectors
    private record CollectedFiles(
            List<DataFile> data, List<DeleteFile> eqDeletes, List<DeleteFile> positionDeletes) {}

    private sealed interface WalkEntry permits DataWalkEntry, EqualityDeleteWalkEntry {
        State position();
    }

    private record DataWalkEntry(State position, DataFile file) implements WalkEntry {}

    private record EqualityDeleteWalkEntry(State position, DeleteFile file) implements WalkEntry {}
}
