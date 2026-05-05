package io.github.jordepic.icestream.converter;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.DeleteFile;

/**
 * Per-spark-task delete-file output: the new delete files this task wrote and the existing
 * file-scoped delete files it absorbed (rewrote) into them.
 *
 * <p>Mirrors the shape of {@code org.apache.iceberg.io.DeleteWriteResult} but is serializable so
 * it can travel back to the driver as a Spark partition output. {@code rewrittenDeletes} comes
 * straight from {@code BaseDVFileWriter.result().rewrittenDeleteFiles()} (V3) or
 * {@code SortingPositionOnlyDeleteWriter.result().rewrittenDeleteFiles()} (V2) — both writers
 * track this for us based on the previous-deletes loader's output.
 */
public record TaskOutputs(List<DeleteFile> newDeletes, List<DeleteFile> rewrittenDeletes) implements Serializable {

    private static final long serialVersionUID = 1L;
}
