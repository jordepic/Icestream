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
 * V3 per-task writer. Wraps {@link BaseDVFileWriter} with a partition-bytes decode cache and a
 * mutable lookup map the underlying writer probes at close time to merge previous DVs into the
 * new puffin output. Existing DVs land via {@link #registerExisting} as upstream encounters them.
 */
public final class PerTaskDvWriter implements PerTaskDeleteFileWriter {

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

    @Override
    public void registerExisting(String dataFilePath, ExistingPerFileDeletes existing) {
        if (existing instanceof ExistingPerFileDeletes.V3 v3) {
            existingByDataFilePath.put(dataFilePath, v3.dv());
        }
    }

    @Override
    public void delete(String dataFilePath, long pos, int specId, byte[] partitionBytes) {
        PartitionSpec spec = table.specs().get(specId);
        StructLike partition = partitionCache.computeIfAbsent(
                new PartitionKey(specId, partitionBytes),
                key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
        writer.delete(dataFilePath, pos, spec, partition);
    }

    @Override
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
