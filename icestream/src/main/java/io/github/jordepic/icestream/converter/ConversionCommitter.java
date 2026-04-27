package io.github.jordepic.icestream.converter;

import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;

/**
 * Commits a {@link CommitPlan} to a table in a single {@link RowDelta}.
 *
 * <p>Validation strategy:
 * <ul>
 *   <li>{@code validateFromSnapshot(plan.startingSnapshotId())} — bounds conflict-detection
 *       history to commits landed after the planner read.
 *   <li>{@code validateDeletedFiles()} — every eq-delete and existing DV we're removing must
 *       still be in the snapshot.
 *   <li>Concurrent-DV-on-same-data-file detection comes from iceberg automatically:
 *       {@code BaseRowDelta.validate} unconditionally calls {@code validateAddedDVs}, which
 *       walks new delete manifests since {@code startingSnapshotId} and throws if any live DV
 *       references a data file we're attaching a new DV to. No flag required, and it's
 *       precise — only DVs on our target data files trigger it (vs.
 *       {@code validateNoConflictingDeleteFiles()} which fires on any new delete anywhere).
 * </ul>
 *
 * <p>We deliberately do NOT call {@code validateDataFilesExist}: a concurrent compaction or
 * expire-snapshots that drops a data file we attached a DV to is fine — the new DV simply
 * references a file that's no longer in the snapshot, which iceberg tolerates. Forcing this
 * check would make us crash on legitimate background maintenance. We can then remove those
 * orphaned files.
 *
 * <p>{@code ValidationException} propagates; the master loop is responsible for re-planning.
 */
public final class ConversionCommitter {

    private ConversionCommitter() {}

    public static void commit(Table table, CommitPlan plan) {
        if (plan.isNoOp()) {
            return;
        }
        RowDelta rowDelta = table.newRowDelta()
                .validateFromSnapshot(plan.startingSnapshotId())
                .validateDeletedFiles();
        plan.newDvsToAdd().forEach(rowDelta::addDeletes);
        plan.eqDeletesToRemove().forEach(rowDelta::removeDeletes);
        plan.existingDvsToRemove().forEach(rowDelta::removeDeletes);
        rowDelta.commit();
    }
}
