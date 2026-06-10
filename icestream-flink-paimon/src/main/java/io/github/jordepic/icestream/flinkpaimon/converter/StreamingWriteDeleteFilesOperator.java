package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriters;
import io.github.jordepic.icestream.index.IndexEncoding;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.SerializableTable;

/**
 * Distributed writer for the standing converter job. Runs at the job's parallelism, fed two streams
 * both <b>keyed by {@code data_file_path}</b>, so existing-deletes and matches for the same path land
 * on the same subtask — and a positional delete file belongs to exactly one data file, so two
 * subtasks never write the same one.
 *
 * <ul>
 *   <li>Input 1: {@code (data_file_path, ExistingPerFileDeletes)} from
 *       {@code WorkUnitToExistingDeletes} — registered so the underlying writer merges the prior DV /
 *       file-scoped pos-delete into the new file it writes (and schedules the prior one for removal).
 *   <li>Input 2: {@code Row(spec_id, partition_key_hex, data_file_path, pos)} — match rows from the
 *       warm Paimon lookup join.
 * </ul>
 *
 * <p>Unlike the bounded batch writer, this one is long-lived: it flushes at the <b>checkpoint
 * barrier</b> ({@link #prepareSnapshotPreBarrier}) and <b>emits its slice's {@link TaskOutputs}
 * downstream</b> to the parallelism-1 {@code RunCommitter}, which aggregates all subtasks' slices for
 * the epoch (barrier alignment guarantees it has received every subtask's slice) and commits the
 * conversion. One run per epoch (the source emits one per epoch), so
 * every existing-delete and match preceding the barrier belongs to that conversion; the order in
 * which the two inputs interleave does not matter because the merge happens at flush. The writer is
 * re-created for the next conversion (unique file names via {@code subtask} + an incrementing flush
 * sequence). A data file that gets existing deletes but no new match this epoch produces no output —
 * the writer only emits files for data files it actually deleted into.
 */
public final class StreamingWriteDeleteFilesOperator extends AbstractStreamOperator<TaskOutputs>
        implements TwoInputStreamOperator<Tuple2<String, ExistingPerFileDeletes>, Row, TaskOutputs> {

    private static final long serialVersionUID = 2L;

    private final SerializableTable serializableTable;
    private final int formatVersion;

    private transient PerTaskDeleteFileWriter writer;
    private transient int subtask;
    private transient long flushSeq;

    public StreamingWriteDeleteFilesOperator(SerializableTable serializableTable, int formatVersion) {
        this.serializableTable = serializableTable;
        this.formatVersion = formatVersion;
    }

    @Override
    public void open() throws Exception {
        super.open();
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        flushSeq = 0;
        writer = PerTaskDeleteFileWriters.forTable(serializableTable, formatVersion, subtask, flushSeq);
    }

    @Override
    public void processElement1(StreamRecord<Tuple2<String, ExistingPerFileDeletes>> element) {
        Tuple2<String, ExistingPerFileDeletes> entry = element.getValue();
        writer.registerExisting(entry.f0, entry.f1);
    }

    @Override
    public void processElement2(StreamRecord<Row> element) {
        Row row = element.getValue();
        int specId = (Integer) row.getField(0);
        byte[] partitionBytes = IndexEncoding.fromHex((String) row.getField(1));
        String dataFilePath = (String) row.getField(2);
        long pos = (Long) row.getField(3);
        writer.delete(dataFilePath, pos, specId, partitionBytes);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        // All existing-deletes and matches for this epoch's conversion have been processed (barrier
        // alignment across both inputs). Flush the merged slice downstream before the barrier.
        output.collect(new StreamRecord<>(writer.finishAndClose()));
        writer = PerTaskDeleteFileWriters.forTable(serializableTable, formatVersion, subtask, ++flushSeq);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
        }
    }
}
