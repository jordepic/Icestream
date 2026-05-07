package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Locates per-data-file scoped delete files (V3 puffin DVs or V2 FILE-scoped pos-delete files)
 * whose {@code (specId, partition)} encoding falls inside {@code touchedPartitions}.
 *
 * <p>Two entry-points:
 * <ul>
 *   <li>{@link #collect} — driver/master-side, walks every delete manifest in one pass and returns
 *       the full {@code Map<String, ExistingPerFileDeletes>}. Used by the Spark path which
 *       distributes via {@code parallelizePairs} + {@code cogroup}.
 *   <li>{@link #deleteManifests} + {@link #scanManifest} — splits the work so the master only
 *       enumerates manifest references (cheap metadata read) and per-task code does the actual
 *       manifest scan + filter. Used by the Flink path so the I/O parallelizes across the cluster
 *       instead of running on the master JVM.
 * </ul>
 *
 * <p>Out-of-scope (deliberately ignored):
 * <ul>
 *   <li>V3: legacy positional-delete parquet files — a v3 reader applies them additively
 *       alongside our new DV.
 *   <li>V2: PARTITION-granularity pos-delete files (no {@code referencedDataFile}). Knowing which
 *       data files they touch would require a content scan; they remain alongside our output and
 *       are merged by readers.
 * </ul>
 */
public final class ExistingPerFileDeleteLoader {

    private ExistingPerFileDeleteLoader() {}

    public static Map<String, ExistingPerFileDeletes> collect(
            Table table, int formatVersion, Set<PartitionKey> touchedPartitions) {
        Map<String, ExistingPerFileDeletes> result = new HashMap<>();
        if (table.currentSnapshot() == null || touchedPartitions.isEmpty()) {
            return result;
        }
        for (ManifestFile manifest : deleteManifests(table)) {
            scanManifest(
                    table,
                    formatVersion,
                    touchedPartitions,
                    manifest,
                    (path, entry) -> mergeInto(result, path, entry));
        }
        return result;
    }

    /** Master-side: just the manifest references for the current snapshot. Cheap metadata read. */
    public static List<ManifestFile> deleteManifests(Table table) {
        if (table.currentSnapshot() == null) {
            return List.of();
        }
        return table.currentSnapshot().deleteManifests(table.io());
    }

    /**
     * Per-task: open one manifest, filter to scoped deletes within {@code touchedPartitions} that
     * match the format-version's content predicate, and emit one
     * {@code (data_file_path, ExistingPerFileDeletes)} per matching entry. V2 may emit several
     * entries for the same data file path (multiple FILE-scoped pos-delete files merged later by
     * the writer's {@code registerExisting}).
     */
    public static void scanManifest(
            Table table,
            int formatVersion,
            Set<PartitionKey> touchedPartitions,
            ManifestFile manifest,
            BiConsumer<String, ExistingPerFileDeletes> emit) {
        Predicate<DeleteFile> contentFilter = contentFilterFor(formatVersion);
        try (CloseableIterable<DeleteFile> entries =
                ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
            for (DeleteFile file : entries) {
                if (!contentFilter.test(file)) {
                    continue;
                }
                PartitionSpec spec = table.specs().get(file.specId());
                byte[] encoded = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
                if (!touchedPartitions.contains(new PartitionKey(file.specId(), encoded))) {
                    continue;
                }
                CharSequence referenced = ContentFileUtil.referencedDataFile(file);
                if (referenced == null) {
                    continue;
                }
                emit.accept(referenced.toString(), toExisting(formatVersion, file));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Predicate<DeleteFile> contentFilterFor(int formatVersion) {
        return switch (formatVersion) {
            case 3 -> file -> file.format() == FileFormat.PUFFIN;
            case 2 -> file -> file.content() == FileContent.POSITION_DELETES
                    && ContentFileUtil.referencedDataFile(file) != null;
            default -> throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        };
    }

    private static ExistingPerFileDeletes toExisting(int formatVersion, DeleteFile file) {
        return switch (formatVersion) {
            case 3 -> new ExistingPerFileDeletes.V3(toDvInfo(file));
            case 2 -> new ExistingPerFileDeletes.V2(List.of(file.copy()));
            default -> throw new IllegalArgumentException("unreachable");
        };
    }

    private static void mergeInto(
            Map<String, ExistingPerFileDeletes> target, String path, ExistingPerFileDeletes entry) {
        ExistingPerFileDeletes existing = target.get(path);
        if (existing == null) {
            target.put(path, entry);
            return;
        }
        if (entry instanceof ExistingPerFileDeletes.V2 incoming
                && existing instanceof ExistingPerFileDeletes.V2 current) {
            List<DeleteFile> merged = new ArrayList<>(current.posDeletes());
            merged.addAll(incoming.posDeletes());
            target.put(path, new ExistingPerFileDeletes.V2(merged));
            return;
        }
        // V3 invariant: at most one DV per data file. Last writer wins; in practice this branch
        // shouldn't fire because the manifest scan only emits one V3 entry per path.
        target.put(path, entry);
    }

    private static DvInfo toDvInfo(DeleteFile file) {
        DeleteFile copied = file.copy();
        return new DvInfo(
                copied.location(),
                copied.contentOffset(),
                copied.contentSizeInBytes(),
                copied.recordCount(),
                copied.referencedDataFile());
    }
}
