package io.github.jordepic.icestream.flinkpaimon.channel;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.ManifestFile;

/**
 * One conversion submitted to the standing streaming job via {@link InProcessConversionChannel}:
 * the eq-delete files to read, join against the index, and convert to positional deletes, plus the
 * snapshot's delete-manifest references — the job scans them to merge each affected data file's
 * existing deletes (V3 DV / V2 file-scoped pos-delete) into the new delete file it writes.
 * {@code conversionId} correlates the result back to the waiting caller.
 */
public record ConversionRequest(
        long conversionId, List<EqDeleteWorkItem> workItems, List<ManifestFile> deleteManifests)
        implements Serializable {

    private static final long serialVersionUID = 2L;
}
