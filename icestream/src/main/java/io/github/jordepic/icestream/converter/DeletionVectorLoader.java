package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.cassandra.IndexEncoding;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Driver-side: walks the snapshot's delete manifests once for puffin DVs whose {@code (specId,
 * partition)} encoding falls inside {@code touchedPartitions}, returning both the original
 * {@link DeleteFile} objects (for {@code RowDelta.removeDeletes}) and a small broadcast-friendly
 * {@link DvInfo} map (for the executor-side merge via {@link DeletionVectorReader}).
 *
 * <p>Pruning by partition keeps the manifest scan and the broadcast bounded to the eq-delete run's
 * footprint instead of the whole table.
 *
 * <p>Scope: PUFFIN files only. Legacy positional-delete parquet files are deliberately ignored; a
 * v3 reader applies them additively alongside our new DV.
 */
public final class DeletionVectorLoader {

    private DeletionVectorLoader() {}

    public record CollectedDvs(Map<String, DeleteFile> deletes, Map<String, DvInfo> serializableDeletes) {}

    public static CollectedDvs collect(Table table, Set<PartitionKey> touchedPartitions) {
        Map<String, DeleteFile> originals = new HashMap<>();
        Map<String, DvInfo> broadcastable = new HashMap<>();
        if (table.currentSnapshot() == null || touchedPartitions.isEmpty()) {
            return new CollectedDvs(originals, broadcastable);
        }
        FileIO io = table.io();
        table.currentSnapshot().deleteManifests(io).forEach(manifest -> {
            try (CloseableIterable<DeleteFile> entries =
                    ManifestFiles.readDeleteManifest(manifest, io, table.specs())) {
                for (DeleteFile file : entries) {
                    if (file.format() != FileFormat.PUFFIN) {
                        continue;
                    }
                    PartitionSpec spec = table.specs().get(file.specId());
                    byte[] encoded = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
                    if (!touchedPartitions.contains(new PartitionKey(file.specId(), encoded))) {
                        continue;
                    }
                    String referencedFile = file.referencedDataFile();
                    DeleteFile copied = file.copy();
                    originals.put(referencedFile, copied);
                    broadcastable.put(
                            referencedFile,
                            new DvInfo(
                                    copied.location(),
                                    copied.contentOffset(),
                                    copied.contentSizeInBytes(),
                                    copied.recordCount(),
                                    referencedFile));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        return new CollectedDvs(originals, broadcastable);
    }
}
