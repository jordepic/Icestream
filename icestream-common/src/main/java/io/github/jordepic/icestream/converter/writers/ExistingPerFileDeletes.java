package io.github.jordepic.icestream.converter.writers;

import io.github.jordepic.icestream.converter.DvInfo;
import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.DeleteFile;

/**
 * Per-data-file scoped delete files that already exist for some data file in the table at the
 * snapshot we're converting against. The format-version-specific writer folds these into its new
 * output and surfaces them as {@code rewrittenDeletes} in {@link
 * io.github.jordepic.icestream.converter.TaskOutputs}.
 *
 * <p>Both engines treat this as an opaque carrier — only the variant-matching writer ever
 * downcasts. This is the single seam that hides the V2/V3 difference everywhere above the writer.
 */
public sealed interface ExistingPerFileDeletes extends Serializable {

    record V3(DvInfo dv) implements ExistingPerFileDeletes {}

    record V2(List<DeleteFile> posDeletes) implements ExistingPerFileDeletes {}
}
