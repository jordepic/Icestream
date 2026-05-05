package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.index.IndexEncoding;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Driver-side V2 analog of {@link DeletionVectorLoader}: walks the snapshot's delete manifests
 * once for FILE-scoped position-delete files (where {@code referencedDataFile() != null}) whose
 * {@code (specId, partition)} encoding falls inside {@code touchedPartitions}, returning a map
 * of data-file-path → list of pos-delete files referencing it.
 *
 * <p>FILE-scoped pos-delete files are what streaming writers (Flink, RisingWave) produce in
 * upsert mode. Multiple such files can accumulate over many commits for the same data file; the
 * V2 writer merges all of them into a single new pos-delete file in this run via
 * {@link org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter}'s {@code loadPreviousDeletes}.
 *
 * <p>Unscoped (PARTITION-granularity) pos-delete files are deliberately ignored — those would
 * require a content scan to know which data files they touch. They remain in the table state
 * alongside our output and are merged by readers.
 */
public final class ExistingPosDeleteLoader {

    private ExistingPosDeleteLoader() {}

    public static Map<String, List<DeleteFile>> collect(Table table, Set<PartitionKey> touchedPartitions) {
        Map<String, List<DeleteFile>> byDataFile = new HashMap<>();
        if (table.currentSnapshot() == null || touchedPartitions.isEmpty()) {
            return byDataFile;
        }
        FileIO io = table.io();
        table.currentSnapshot().deleteManifests(io).forEach(manifest -> {
            try (CloseableIterable<DeleteFile> entries =
                    ManifestFiles.readDeleteManifest(manifest, io, table.specs())) {
                for (DeleteFile file : entries) {
                    if (file.content() != FileContent.POSITION_DELETES) {
                        continue;
                    }
                    CharSequence referenced = ContentFileUtil.referencedDataFile(file);
                    if (referenced == null) {
                        continue;
                    }
                    String referencedFile = referenced.toString();
                    PartitionSpec spec = table.specs().get(file.specId());
                    byte[] encoded = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
                    if (!touchedPartitions.contains(new PartitionKey(file.specId(), encoded))) {
                        continue;
                    }
                    byDataFile.computeIfAbsent(referencedFile, k -> new ArrayList<>()).add(file.copy());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return byDataFile;
    }
}
