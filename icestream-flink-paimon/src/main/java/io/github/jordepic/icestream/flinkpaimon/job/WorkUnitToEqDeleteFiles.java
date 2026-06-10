package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Emits an EQ_DEL {@link WorkUnit}'s pre-built eq-delete work items, one record each, so a downstream
 * {@code rebalance} spreads the file reads across reader subtasks. DATA units emit nothing. Runs at
 * parallelism 1 (chained to the source); metadata only, so the whole unit stays in one epoch.
 */
public final class WorkUnitToEqDeleteFiles implements FlatMapFunction<WorkUnit, EqDeleteWorkItem> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(WorkUnit unit, Collector<EqDeleteWorkItem> out) {
        if (unit.isEqDelete()) {
            unit.eqDeleteItems().forEach(out::collect);
        }
    }
}
