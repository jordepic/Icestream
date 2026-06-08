package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.flinkpaimon.channel.ConversionRequest;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Splits a {@link ConversionRequest} into its eq-delete files, one record each, so a downstream
 * {@code rebalance} can spread the file reads across reader subtasks. Runs at parallelism 1 (chained
 * to the p1 source) and only emits lightweight file descriptors — no I/O — so the whole request
 * stays within one checkpoint epoch.
 */
public final class RequestToEqDeleteFiles implements FlatMapFunction<ConversionRequest, EqDeleteWorkItem> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(ConversionRequest request, Collector<EqDeleteWorkItem> out) {
        for (EqDeleteWorkItem item : request.workItems()) {
            out.collect(item);
        }
    }
}
