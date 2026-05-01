package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.cassandra.IndexEncoding;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Executor-side V3 writer: per spark partition, build one {@link BaseDVFileWriter}, accept all
 * {@code (data_file_path, (matches, existingDv))} cogroup tuples assigned to this task, and emit
 * one {@link TaskOutputs} carrying the new puffin DVs and any pre-existing file-scoped DVs that
 * were folded in.
 *
 * <p>The cogroup co-locates each data file's matches with its (at most one) existing
 * {@link DvInfo}. We populate a per-task local {@code Map<String, DvInfo>} as we iterate and hand
 * it to the {@link DeletionVectorReader} loader by reference; {@code BaseDVFileWriter.close()}
 * calls the loader once per touched data file, by which time the map has every entry it'll
 * query. {@code result().rewrittenDeleteFiles()} traces back to the {@code DeleteFile} that
 * {@code DeletionVectorReader} reconstructs and passes to
 * {@code PositionDeleteIndex.deserialize}, so the driver-visible rewritten list is the V3
 * analog of V2's pos-delete rewrite tracking.
 */
public final class WriteDvFiles
        implements FlatMapFunction<
                Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DvInfo>>>>, TaskOutputs> {

    private static final long serialVersionUID = 1L;

    private final Broadcast<Table> tableBroadcast;

    public WriteDvFiles(Broadcast<Table> tableBroadcast) {
        this.tableBroadcast = tableBroadcast;
    }

    @Override
    public Iterator<TaskOutputs> call(
            Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DvInfo>>>> input) throws IOException {
        if (!input.hasNext()) {
            return List.<TaskOutputs>of().iterator();
        }
        Table table = tableBroadcast.value();
        TaskContext ctx = TaskContext.get();
        int partitionId = ctx != null ? ctx.partitionId() : 0;
        long taskId = ctx != null ? ctx.taskAttemptId() : 0L;

        OutputFileFactory factory = OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(FileFormat.PUFFIN)
                .build();
        Map<String, DvInfo> localExistingByDataFilePath = new HashMap<>();
        DeletionVectorReader loader = new DeletionVectorReader(localExistingByDataFilePath, table::io);
        BaseDVFileWriter writer = new BaseDVFileWriter(factory, loader);
        Map<PartitionKey, StructLike> partitionCache = new HashMap<>();

        try (writer) {
            while (input.hasNext()) {
                Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DvInfo>>> entry = input.next();
                String dataFilePath = entry._1;
                Iterator<DvInfo> existingIter = entry._2._2.iterator();
                if (existingIter.hasNext()) {
                    localExistingByDataFilePath.put(dataFilePath, existingIter.next());
                }
                for (PerPositionMatch match : entry._2._1) {
                    PartitionSpec spec = table.specs().get(match.specId());
                    StructLike partition = partitionCache.computeIfAbsent(
                            new PartitionKey(match.specId(), match.partitionBytes()),
                            key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
                    writer.delete(dataFilePath, match.pos(), spec, partition);
                }
            }
        }
        TaskOutputs outputs = new TaskOutputs(
                List.copyOf(writer.result().deleteFiles()),
                List.copyOf(writer.result().rewrittenDeleteFiles()));
        return List.of(outputs).iterator();
    }
}
