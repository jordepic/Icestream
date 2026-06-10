package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.indexer.FileWorkItem;
import io.github.jordepic.icestream.planner.FileKind;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;

/**
 * One unit of the {@link io.github.jordepic.icestream.planner.SnapshotPlanner} walk, emitted by
 * {@link IcestreamWalkSource} (one per checkpoint epoch). The source — the only operator with a live
 * iceberg table, the driver-equivalent — does <em>all</em> table-dependent work up front and packs
 * the results here, so every downstream operator is pure compute and the unit travels as
 * Flink-serializable value types only (no Avro-backed {@code ManifestFile}/{@code FileRun} on the
 * wire):
 *
 * <ul>
 *   <li><b>DATA</b>: {@code dataItems} — one per data file, each with the run's positional-deletes/DVs
 *       to apply while reading.
 *   <li><b>EQ_DEL</b>: {@code eqDeleteItems} (the files to read for pk values), {@code existingDeletes}
 *       (prior per-data-file DVs / pos-deletes to merge, already scanned + partition-filtered), and
 *       {@code eqDeletesToRemove} (the eq-delete files the committer removes in the {@code RowDelta}).
 * </ul>
 */
public record WorkUnit(
        FileKind kind,
        long maxSeq,
        long startingSnapshotId,
        List<EqDeleteWorkItem> eqDeleteItems,
        List<FileWorkItem> dataItems,
        Map<String, ExistingPerFileDeletes> existingDeletes,
        List<DeleteFile> eqDeletesToRemove)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public boolean isEqDelete() {
        return kind == FileKind.EQ_DEL;
    }
}
