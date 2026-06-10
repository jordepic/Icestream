package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.indexer.FileWorkItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Emits a DATA {@link WorkUnit}'s pre-built index work items, one record each, so a downstream
 * {@code rebalance} spreads the data-file reads across reader subtasks. EQ_DEL units emit nothing.
 * Parallelism 1 (chained to the source); metadata only.
 */
public final class WorkUnitToDataFiles implements FlatMapFunction<WorkUnit, FileWorkItem> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(WorkUnit unit, Collector<FileWorkItem> out) {
        if (!unit.isEqDelete()) {
            unit.dataItems().forEach(out::collect);
        }
    }
}
