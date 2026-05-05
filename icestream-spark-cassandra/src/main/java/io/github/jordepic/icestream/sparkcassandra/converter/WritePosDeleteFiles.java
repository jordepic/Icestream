package io.github.jordepic.icestream.sparkcassandra.converter;

import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.PerTaskPosDeleteWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Spark FlatMapFunction wrapper around {@link PerTaskPosDeleteWriter}. Per spark partition,
 * build one underlying writer, route every cogroup tuple's matches + existing pos-delete files
 * through it, and emit one {@link TaskOutputs}.
 */
public final class WritePosDeleteFiles
        implements FlatMapFunction<
                Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>>>, TaskOutputs> {

    private static final long serialVersionUID = 1L;

    private final Broadcast<Table> tableBroadcast;

    public WritePosDeleteFiles(Broadcast<Table> tableBroadcast) {
        this.tableBroadcast = tableBroadcast;
    }

    @Override
    public Iterator<TaskOutputs> call(
            Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>>> input)
            throws IOException {
        if (!input.hasNext()) {
            return List.<TaskOutputs>of().iterator();
        }
        Table table = tableBroadcast.value();
        TaskContext ctx = TaskContext.get();
        int partitionId = ctx != null ? ctx.partitionId() : 0;
        long taskId = ctx != null ? ctx.taskAttemptId() : 0L;

        PerTaskPosDeleteWriter writer = new PerTaskPosDeleteWriter(table, partitionId, taskId);
        while (input.hasNext()) {
            Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>> entry = input.next();
            String dataFilePath = entry._1;
            List<DeleteFile> existingForPath = new ArrayList<>();
            entry._2._2.forEach(existingForPath::add);
            writer.registerExistingPosDeletes(dataFilePath, existingForPath);
            for (PerPositionMatch match : entry._2._1) {
                writer.delete(dataFilePath, match.pos(), match.specId(), match.partitionBytes());
            }
        }
        return List.of(writer.finishAndClose()).iterator();
    }
}
