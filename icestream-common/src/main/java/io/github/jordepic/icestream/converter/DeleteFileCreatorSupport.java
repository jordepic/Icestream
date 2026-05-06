package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * Engine-agnostic helpers shared by every {@link DeleteConverter} implementation. The pieces that
 * differ across engines (the join pipeline, the per-task writer dispatch) stay in each impl;
 * everything else lives here.
 */
public final class DeleteFileCreatorSupport {

    private static final int MIN_SUPPORTED_FORMAT_VERSION = 2;
    private static final int MAX_SUPPORTED_FORMAT_VERSION = 3;

    private DeleteFileCreatorSupport() {}

    /**
     * Build the per-eq-delete work items: each input {@link DeleteFile} paired with its
     * driver-precomputed {@code (spec_id, encoded partition bytes)}. The work items are shipped
     * to compute tasks where the eq-delete file is read for its pk values.
     */
    public static List<EqDeleteWorkItem> buildWorkItems(Table table, EqualityDeleteFileRun run) {
        List<EqDeleteWorkItem> items = new ArrayList<>(run.files().size());
        for (DeleteFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
            items.add(new EqDeleteWorkItem(file, file.specId(), partitionBytes));
        }
        return items;
    }

    /** Distinct {@code (spec_id, partition_bytes)} keys touched by a list of work items. */
    public static Set<PartitionKey> touchedPartitions(List<EqDeleteWorkItem> workItems) {
        return workItems.stream()
                .map(item -> new PartitionKey(item.specId(), item.partitionBytes()))
                .collect(Collectors.toSet());
    }

    /**
     * Reduce the per-task {@link TaskOutputs} into a single {@link CommitPlan}. New deletes
     * concatenate; rewritten deletes are deduplicated by location (multiple tasks can each touch
     * the same existing per-data-file scoped delete file).
     */
    public static CommitPlan assemblePlan(
            long startingSnapshotId, EqualityDeleteFileRun run, List<TaskOutputs> taskOutputs) {
        List<DeleteFile> newDeletes = new ArrayList<>();
        List<DeleteFile> rewrittenDeletes = new ArrayList<>();
        CharSequenceSet seenRewrittenLocations = CharSequenceSet.empty();
        for (TaskOutputs output : taskOutputs) {
            newDeletes.addAll(output.newDeletes());
            for (DeleteFile rewritten : output.rewrittenDeletes()) {
                if (seenRewrittenLocations.add(rewritten.location())) {
                    rewrittenDeletes.add(rewritten);
                }
            }
        }
        return new CommitPlan(startingSnapshotId, new ArrayList<>(run.files()), rewrittenDeletes, newDeletes);
    }

    /**
     * Read and validate the iceberg table's format version. Throws {@link IllegalArgumentException}
     * if outside the supported range so the engine-specific strategy switch never sees a bad value.
     */
    public static int requireSupportedFormatVersion(Table table) {
        int formatVersion =
                ((HasTableOperations) table).operations().current().formatVersion();
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION || formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        }
        return formatVersion;
    }
}
