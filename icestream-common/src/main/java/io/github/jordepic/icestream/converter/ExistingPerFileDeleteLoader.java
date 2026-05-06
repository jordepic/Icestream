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
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Driver-side: walks the snapshot's delete manifests once for per-data-file scoped delete files
 * (V3 puffin DVs or V2 FILE-scoped pos-delete files) whose {@code (specId, partition)} encoding
 * falls inside {@code touchedPartitions}, returning a single map keyed by referenced data file
 * path. The format-version dispatch happens here so callers above don't see it.
 *
 * <p>Pruning by partition keeps the manifest scan and the resulting map bounded to the eq-delete
 * run's footprint instead of the whole table.
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
        switch (formatVersion) {
            case 3 -> walkScopedDeletes(
                    table,
                    touchedPartitions,
                    file -> file.format() == FileFormat.PUFFIN,
                    (path, file) -> result.put(path, new ExistingPerFileDeletes.V3(toDvInfo(file))));
            case 2 -> walkScopedDeletes(
                    table,
                    touchedPartitions,
                    file -> file.content() == FileContent.POSITION_DELETES
                            && ContentFileUtil.referencedDataFile(file) != null,
                    (path, file) -> {
                        ExistingPerFileDeletes existing = result.get(path);
                        List<DeleteFile> list = existing instanceof ExistingPerFileDeletes.V2 v2
                                ? new ArrayList<>(v2.posDeletes())
                                : new ArrayList<>();
                        list.add(file.copy());
                        result.put(path, new ExistingPerFileDeletes.V2(list));
                    });
            default -> throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        }
        return result;
    }

    private static void walkScopedDeletes(
            Table table,
            Set<PartitionKey> touchedPartitions,
            Predicate<DeleteFile> contentFilter,
            BiConsumer<String, DeleteFile> recordEntry) {
        FileIO io = table.io();
        table.currentSnapshot().deleteManifests(io).forEach(manifest -> {
            try (CloseableIterable<DeleteFile> entries =
                    ManifestFiles.readDeleteManifest(manifest, io, table.specs())) {
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
                    recordEntry.accept(referenced.toString(), file);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
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
