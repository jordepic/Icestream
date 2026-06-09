package io.github.jordepic.icestream.converter;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.DeleteFile;

/**
 * Output of {@link DeleteFileCreator}: the file-level mutation that {@link ConversionCommitter}
 * applies in a single {@code RowDelta}.
 *
 * <p>{@code startingSnapshotId} is the snapshot the planner read; the committer passes it to
 * {@code RowDelta.validateFromSnapshot} so conflict-detection windows are bounded to changes
 * landed *after* planning rather than the whole table history.
 */
public record CommitPlan(
        long startingSnapshotId,
        List<DeleteFile> eqDeletesToRemove,
        List<DeleteFile> existingDeletesToRemove,
        List<DeleteFile> deletesToAdd)
        implements Serializable {

    public boolean isNoOp() {
        return eqDeletesToRemove.isEmpty() && existingDeletesToRemove.isEmpty() && deletesToAdd.isEmpty();
    }
}
