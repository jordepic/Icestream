package io.github.jordepic.icestream.sparkcassandra.converter;

import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriters;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Spark FlatMapFunction wrapper around {@link PerTaskDeleteFileWriter}. Per spark partition build
 * one writer (V2 or V3 picked once via {@link PerTaskDeleteFileWriters#forTable} from
 * {@link #formatVersion}), stream cogroup tuples through it — register-existing then delete-row
 * per match — and emit one {@link TaskOutputs}.
 */
public final class WriteDeleteFiles
        implements FlatMapFunction<
                Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<ExistingPerFileDeletes>>>>,
                TaskOutputs> {

    private static final long serialVersionUID = 1L;

    private final Broadcast<Table> tableBroadcast;
    private final int formatVersion;

    public WriteDeleteFiles(Broadcast<Table> tableBroadcast, int formatVersion) {
        this.tableBroadcast = tableBroadcast;
        this.formatVersion = formatVersion;
    }

    @Override
    public Iterator<TaskOutputs> call(
            Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<ExistingPerFileDeletes>>>> input)
            throws IOException {
        if (!input.hasNext()) {
            return List.<TaskOutputs>of().iterator();
        }
        Table table = tableBroadcast.value();
        TaskContext ctx = TaskContext.get();
        int partitionId = ctx != null ? ctx.partitionId() : 0;
        long taskId = ctx != null ? ctx.taskAttemptId() : 0L;

        PerTaskDeleteFileWriter writer = PerTaskDeleteFileWriters.forTable(table, formatVersion, partitionId, taskId);
        while (input.hasNext()) {
            Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<ExistingPerFileDeletes>>> entry = input.next();
            String dataFilePath = entry._1;
            Iterator<ExistingPerFileDeletes> existingIter = entry._2._2.iterator();
            if (existingIter.hasNext()) {
                writer.registerExisting(dataFilePath, existingIter.next());
            }
            for (PerPositionMatch match : entry._2._1) {
                writer.delete(dataFilePath, match.pos(), match.specId(), match.partitionBytes());
            }
        }
        return List.of(writer.finishAndClose()).iterator();
    }
}
