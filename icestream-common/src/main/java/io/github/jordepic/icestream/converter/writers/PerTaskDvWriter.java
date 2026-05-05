package io.github.jordepic.icestream.converter.writers;

import io.github.jordepic.icestream.converter.DeletionVectorReader;
import io.github.jordepic.icestream.converter.DvInfo;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.OutputFileFactory;

/**
 * Engine-agnostic V3 per-task writer. Wraps {@link BaseDVFileWriter} with the bookkeeping the
 * upstream callers need:
 * <ul>
 *   <li>A local {@code Map<String, DvInfo>} that {@link DeletionVectorReader} probes when
 *       {@link BaseDVFileWriter#close()} loads previous DVs to merge into the new puffin output.
 *       Callers register existing DVs via {@link #registerExistingDv} before issuing the first
 *       {@link #delete} for that data file.
 *   <li>A {@link PartitionKey} → {@link StructLike} cache so the avro-encoded partition bytes are
 *       decoded once per partition rather than per row.
 * </ul>
 *
 * <p>Both Spark and Flink wrappers create one of these per task and forward {@code (path, pos,
 * specId, partitionBytes)} tuples into {@link #delete}. Lifecycle: construct → optional
 * {@link #registerExistingDv} calls → many {@link #delete} calls → {@link #finishAndClose}.
 */
public final class PerTaskDvWriter implements AutoCloseable {

    private final Table table;
    private final BaseDVFileWriter writer;
    private final Map<String, DvInfo> existingByDataFilePath;
    private final Map<PartitionKey, StructLike> partitionCache;
    private boolean closed;

    public PerTaskDvWriter(Table table, int partitionId, long taskAttemptId) {
        this.table = table;
        OutputFileFactory factory = OutputFileFactory.builderFor(table, partitionId, taskAttemptId)
                .format(FileFormat.PUFFIN)
                .build();
        this.existingByDataFilePath = new HashMap<>();
        DeletionVectorReader loader = new DeletionVectorReader(existingByDataFilePath, table::io);
        this.writer = new BaseDVFileWriter(factory, loader);
        this.partitionCache = new HashMap<>();
    }

    public void registerExistingDv(String dataFilePath, DvInfo dvInfo) {
        existingByDataFilePath.put(dataFilePath, dvInfo);
    }

    public void delete(String dataFilePath, long pos, int specId, byte[] partitionBytes) {
        PartitionSpec spec = table.specs().get(specId);
        StructLike partition = partitionCache.computeIfAbsent(
                new PartitionKey(specId, partitionBytes),
                key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
        writer.delete(dataFilePath, pos, spec, partition);
    }

    public TaskOutputs finishAndClose() throws IOException {
        close();
        return new TaskOutputs(
                List.copyOf(writer.result().deleteFiles()), List.copyOf(writer.result().rewrittenDeleteFiles()));
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            writer.close();
            closed = true;
        }
    }
}
