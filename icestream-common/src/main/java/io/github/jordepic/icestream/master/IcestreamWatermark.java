package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.State;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.util.Map;
import org.apache.iceberg.Table;

/**
 * Reads and advances the per-table icestream watermark — the {@code (sequence, kind)} position in
 * the {@link io.github.jordepic.icestream.planner.SnapshotPlanner} walk that has been durably
 * processed — stored as iceberg table properties.
 *
 * <p>The watermark is the only coordination state between the planner-driven walk and the work that
 * applies it. The Spark backend advances it from {@link TableProcessor} after each synchronous run;
 * the Flink+Paimon backend advances it from the in-job committer after each run commits, and its
 * autonomous source paces itself off it. Advancing also re-pins the primary-key / bucket config so a
 * concurrent property edit is caught by the next plan ({@code SnapshotPlanner.validateStability}).
 */
public final class IcestreamWatermark {

    private IcestreamWatermark() {}

    /** The processed position recorded in {@code props}, defaulting to {@link State#INITIAL}. */
    public static State read(Map<String, String> props) {
        long seq = Long.parseLong(props.getOrDefault(IcestreamProperties.LAST_PROCESSED_SEQUENCE, "0"));
        FileKind kind =
                FileKind.valueOf(props.getOrDefault(IcestreamProperties.LAST_PROCESSED_KIND, FileKind.DATA.name()));
        return new State(seq, kind);
    }

    /**
     * Advance the watermark to {@code state} and re-pin the stability config captured from
     * {@code propsAtStart}, in a single iceberg property commit.
     */
    public static void commit(Table table, State state, Map<String, String> propsAtStart) {
        table.updateProperties()
                .set(IcestreamProperties.LAST_PROCESSED_SEQUENCE, Long.toString(state.sequenceNumber()))
                .set(IcestreamProperties.LAST_PROCESSED_KIND, state.fileKind().name())
                .set(IcestreamProperties.PINNED_PRIMARY_KEYS, propsAtStart.get(IcestreamProperties.PRIMARY_KEYS))
                .set(
                        IcestreamProperties.PINNED_INDEX_BUCKETS,
                        propsAtStart.getOrDefault(IcestreamProperties.INDEX_BUCKETS, "1"))
                .commit();
    }
}
