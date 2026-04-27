package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.cassandra.IndexEncoding;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
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
 * Executor-side: for each spark partition, build one {@link BaseDVFileWriter}, accept all
 * {@code (data_file_path, matches)} groups assigned to this task, and emit one {@link DeleteFile}
 * per data file with new puffin metadata.
 *
 * <p>Each {@code PerPositionMatch} carries the {@code (specId, partitionBytes)} that produced
 * the join. We decode the avro bytes back to a {@link StructLike} once per group and pass it to
 * {@code writer.delete(path, pos, spec, partition)} so the resulting {@code DeleteFile} carries
 * the correct partition tuple in the manifest. The puffin file location itself is still flat —
 * {@code BaseDVFileWriter.newWriter()} hardcodes the no-arg {@code OutputFileFactory.newOutputFile()}
 * — but every iceberg DV writer (Spark MERGE/UPDATE/DELETE, ConvertEqualityDeleteFiles, Flink
 * BaseDeltaTaskWriter) lays DVs out the same way, so we follow precedent.
 *
 * <p>Existing DVs (if any) for each data file are merged via {@link DeletionVectorReader}.
 */
public final class WriteDvs
        implements FlatMapFunction<Iterator<Tuple2<String, Iterable<PerPositionMatch>>>, DeleteFile> {

    private static final long serialVersionUID = 1L;

    private final Broadcast<Table> tableBroadcast;
    private final Broadcast<Map<String, DvInfo>> dvInfoBroadcast;

    public WriteDvs(Broadcast<Table> tableBroadcast, Broadcast<Map<String, DvInfo>> dvInfoBroadcast) {
        this.tableBroadcast = tableBroadcast;
        this.dvInfoBroadcast = dvInfoBroadcast;
    }

    @Override
    public Iterator<DeleteFile> call(Iterator<Tuple2<String, Iterable<PerPositionMatch>>> input) throws IOException {
        if (!input.hasNext()) {
            return List.<DeleteFile>of().iterator();
        }
        Table table = tableBroadcast.value();
        TaskContext ctx = TaskContext.get();
        int partitionId = ctx != null ? ctx.partitionId() : 0;
        long taskId = ctx != null ? ctx.taskAttemptId() : 0L;

        OutputFileFactory factory = OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(FileFormat.PUFFIN)
                .build();
        DeletionVectorReader loader = new DeletionVectorReader(dvInfoBroadcast.value(), table::io);
        Map<PartitionKey, StructLike> partitionCache = new HashMap<>();

        BaseDVFileWriter writer = new BaseDVFileWriter(factory, loader);
        while (input.hasNext()) {
            Tuple2<String, Iterable<PerPositionMatch>> tup = input.next();
            String dataFilePath = tup._1;
            for (PerPositionMatch match : tup._2) {
                PartitionSpec spec = table.specs().get(match.specId());
                StructLike partition = partitionCache.computeIfAbsent(
                        new PartitionKey(match.specId(), match.partitionBytes()),
                        key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
                writer.delete(dataFilePath, match.pos(), spec, partition);
            }
        }
        writer.close();
        return writer.result().deleteFiles().iterator();
    }
}
