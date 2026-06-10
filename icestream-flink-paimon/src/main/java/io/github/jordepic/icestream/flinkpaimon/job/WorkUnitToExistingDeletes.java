package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Emits an EQ_DEL {@link WorkUnit}'s pre-scanned existing per-data-file deletes as
 * {@code (data_file_path, ExistingPerFileDeletes)} pairs, co-keyed downstream with the lookup matches
 * by {@code data_file_path} so each writer subtask merges the prior DV / file-scoped pos-delete into
 * the new delete file it writes. DATA units emit nothing. Parallelism 1 (chained to the source).
 */
public final class WorkUnitToExistingDeletes
        implements FlatMapFunction<WorkUnit, Tuple2<String, ExistingPerFileDeletes>> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(WorkUnit unit, Collector<Tuple2<String, ExistingPerFileDeletes>> out) {
        if (!unit.isEqDelete()) {
            return;
        }
        for (Map.Entry<String, ExistingPerFileDeletes> entry : unit.existingDeletes().entrySet()) {
            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }
}
