package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.cassandra.IndexEncoding;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Executor-side V2 writer: per spark partition, build one {@link FanoutPositionOnlyDeleteWriter}
 * with {@link DeleteGranularity#FILE}, accept all {@code (data_file_path, (matches, existing))}
 * cogroup tuples assigned to this task, and emit one {@link TaskOutputs} carrying the new
 * pos-delete files and any pre-existing FILE-scoped pos-delete files that were folded in.
 *
 * <p>The cogroup co-locates each data file's matches with its (zero or more) existing
 * FILE-scoped pos-delete files. We populate a per-task local map as we iterate; the writer's
 * {@code loadPreviousDeletes} callback queries this map by data file path at close time. The
 * output file format honors {@code write.delete.format.default} (falling back to the table's
 * {@code write.format.default}, then iceberg's default of parquet).
 */
public final class WritePosDeleteFiles
        implements FlatMapFunction<
                Iterator<Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>>>, TaskOutputs> {

    private static final long serialVersionUID = 1L;
    private static final long TARGET_FILE_SIZE_BYTES = 512L * 1024 * 1024;

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

        FileFormat deleteFormat = resolveDeleteFormat(table);
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(deleteFormat)
                .build();
        FileWriterFactory<Record> fileWriterFactory = new GenericPosDeleteWriterFactory(table, deleteFormat);

        Map<String, List<DeleteFile>> localExistingByDataFilePath = new HashMap<>();
        BaseDeleteLoader baseLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
        Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes = path -> {
            List<DeleteFile> existing = localExistingByDataFilePath.get(path.toString());
            if (existing == null || existing.isEmpty()) {
                return null;
            }
            return baseLoader.loadPositionDeletes(existing, path);
        };

        FanoutPositionOnlyDeleteWriter<Record> writer = new FanoutPositionOnlyDeleteWriter<>(
                fileWriterFactory, outputFileFactory, table.io(), TARGET_FILE_SIZE_BYTES,
                DeleteGranularity.FILE, loadPreviousDeletes);

        try (writer) {
            Map<PartitionKey, StructLike> partitionCache = new HashMap<>();
            PositionDelete<Record> positionDelete = PositionDelete.create();
            while (input.hasNext()) {
                Tuple2<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>> entry = input.next();
                String dataFilePath = entry._1;
                List<DeleteFile> existingForPath = new ArrayList<>();
                entry._2._2.forEach(existingForPath::add);
                if (!existingForPath.isEmpty()) {
                    localExistingByDataFilePath.put(dataFilePath, existingForPath);
                }
                for (PerPositionMatch match : entry._2._1) {
                    PartitionSpec spec = table.specs().get(match.specId());
                    StructLike partition = partitionCache.computeIfAbsent(
                            new PartitionKey(match.specId(), match.partitionBytes()),
                            key -> IndexEncoding.decodeFromAvroBytes(spec.partitionType(), key.partitionBytes()));
                    positionDelete.set(dataFilePath, match.pos(), null);
                    writer.write(positionDelete, spec, partition);
                }
            }
        }
        TaskOutputs outputs = new TaskOutputs(
                List.copyOf(writer.result().deleteFiles()),
                List.copyOf(writer.result().rewrittenDeleteFiles()));
        return List.of(outputs).iterator();
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
     * Only {@code newPositionDeleteWriter} is implemented; the data and eq-delete paths throw
     * since the fanout writer never calls them. Memoizes one delegate per spec so a multi-spec
     * table writes pos-delete files tagged with the right spec id.
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
            throw new UnsupportedOperationException("WritePosDeleteFiles only writes pos-delete files");
        }

        @Override
        public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
            throw new UnsupportedOperationException("WritePosDeleteFiles only writes pos-delete files");
        }
    }
}
