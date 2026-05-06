package io.github.jordepic.icestream.converter.writers;

import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;

/**
 * V2 per-task writer. Wraps a {@link FanoutPositionOnlyDeleteWriter} configured with FILE
 * granularity, plus a partition-bytes decode cache and a mutable lookup map the underlying writer
 * probes at close time to merge existing FILE-scoped pos-delete files into the new output.
 * Existing pos-deletes land via {@link #registerExisting} as upstream encounters them.
 *
 * <p>Output file format honors {@code write.delete.format.default} (falling back to
 * {@code write.format.default}, then iceberg's parquet default).
 */
public final class PerTaskPosDeleteWriter implements PerTaskDeleteFileWriter {

    private static final long TARGET_FILE_SIZE_BYTES = 512L * 1024 * 1024;

    private final Table table;
    private final FanoutPositionOnlyDeleteWriter<Record> writer;
    private final Map<String, List<DeleteFile>> existingByDataFilePath;
    private final Map<PartitionKey, StructLike> partitionCache;
    private final PositionDelete<Record> positionDelete;
    private boolean closed;

    public PerTaskPosDeleteWriter(Table table, int partitionId, long taskAttemptId) {
        this.table = table;
        FileFormat deleteFormat = resolveDeleteFormat(table);
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskAttemptId)
                .format(deleteFormat)
                .build();
        FileWriterFactory<Record> fileWriterFactory = new GenericPosDeleteWriterFactory(table, deleteFormat);
        this.existingByDataFilePath = new HashMap<>();
        BaseDeleteLoader baseLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
        Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes = path -> {
            List<DeleteFile> existing = existingByDataFilePath.get(path.toString());
            if (existing == null || existing.isEmpty()) {
                return null;
            }
            return baseLoader.loadPositionDeletes(existing, path);
        };
        this.writer = new FanoutPositionOnlyDeleteWriter<>(
                fileWriterFactory,
                outputFileFactory,
                table.io(),
                TARGET_FILE_SIZE_BYTES,
                DeleteGranularity.FILE,
                loadPreviousDeletes);
        this.partitionCache = new HashMap<>();
        this.positionDelete = PositionDelete.create();
    }

    @Override
    public void registerExisting(String dataFilePath, ExistingPerFileDeletes existing) {
        if (existing instanceof ExistingPerFileDeletes.V2 v2 && !v2.posDeletes().isEmpty()) {
            existingByDataFilePath.put(dataFilePath, v2.posDeletes());
        }
    }

    @Override
    public void delete(String dataFilePath, long pos, int specId, byte[] partitionBytes) {
        PartitionSpec spec = table.specs().get(specId);
        StructLike partition = partitionCache.computeIfAbsent(
                new PartitionKey(specId, partitionBytes),
                key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
        positionDelete.set(dataFilePath, pos, null);
        writer.write(positionDelete, spec, partition);
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

    private static FileFormat resolveDeleteFormat(Table table) {
        Map<String, String> props = table.properties();
        String dataFormat =
                props.getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.fromString(props.getOrDefault(TableProperties.DELETE_DEFAULT_FILE_FORMAT, dataFormat));
    }

    /**
     * Adapts {@link GenericAppenderFactory} (the public 1.10.1 entry-point) to the
     * {@link FileWriterFactory} interface that {@link FanoutPositionOnlyDeleteWriter} requires.
     * Only {@code newPositionDeleteWriter} is implemented; data and eq-delete paths throw since
     * the fanout writer never calls them. Memoizes one delegate per spec so a multi-spec table
     * writes pos-delete files tagged with the right spec id.
     */
    private static final class GenericPosDeleteWriterFactory implements FileWriterFactory<Record>, Serializable {

        private static final long serialVersionUID = 1L;

        private final Table table;
        private final FileFormat deleteFormat;
        private final transient Map<Integer, GenericAppenderFactory> bySpecId = new HashMap<>();

        GenericPosDeleteWriterFactory(Table table, FileFormat deleteFormat) {
            this.table = table;
            this.deleteFormat = deleteFormat;
        }

        @Override
        public PositionDeleteWriter<Record> newPositionDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            GenericAppenderFactory delegate = bySpecId.computeIfAbsent(
                    spec.specId(),
                    id -> new GenericAppenderFactory(
                            table, table.schema(), spec, table.properties(), null, null, null));
            return delegate.newPosDeleteWriter(file, deleteFormat, partition);
        }

        @Override
        public DataWriter<Record> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException("PerTaskPosDeleteWriter only writes pos-delete files");
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException("PerTaskPosDeleteWriter only writes pos-delete files");
        }
    }
}
