package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriters;
import io.github.jordepic.icestream.index.IndexEncoding;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.SerializableTable;

/**
 * Distributed writer for the standing converter job. Runs at the job's parallelism, fed a stream
 * <b>keyed by {@code data_file_path}</b>, so each subtask owns a disjoint set of data files — and a
 * positional delete file belongs to exactly one data file, so two subtasks never write the same one.
 *
 * <p>Input: match rows {@code Row(spec_id, partition_key_hex, data_file_path, pos)} from the warm
 * Paimon lookup join. It writes per-data-file delete files into a {@link PerTaskDeleteFileWriter},
 * then flushes at the <b>checkpoint barrier</b> ({@link #prepareSnapshotPreBarrier}) and <b>emits its
 * slice's {@link TaskOutputs} downstream</b> to the parallelism-1
 * {@link io.github.jordepic.icestream.flinkpaimon.channel.CollectConversionOutputsOperator},
 * which aggregates all subtasks' outputs for the epoch (barrier alignment guarantees it has received
 * every subtask's slice before its own barrier) and replies to the waiting caller. By barrier
 * ordering every match for the in-flight conversion precedes the barrier, so each flush is exactly
 * that conversion's slice. The writer is then re-created for the next conversion (unique file names
 * via {@code subtask} + an incrementing flush sequence).
 *
 * <p>P1 scope: existing-delete merge is deferred, so this operator only writes new deletes from the
 * lookup matches (see STREAMING_CONVERTER.md).
 */
public final class StreamingWriteDeleteFilesOperator extends AbstractStreamOperator<TaskOutputs>
        implements OneInputStreamOperator<Row, TaskOutputs> {

    private static final long serialVersionUID = 1L;

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
    public void processElement(StreamRecord<Row> element) {
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
        TaskOutputs outputs = writer.finishAndClose();
        // Emit this subtask's slice downstream (before the barrier) for the collector to aggregate.
        output.collect(new StreamRecord<>(outputs));
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
