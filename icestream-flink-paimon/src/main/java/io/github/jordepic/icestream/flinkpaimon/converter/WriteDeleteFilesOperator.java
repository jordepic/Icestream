package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriters;
import io.github.jordepic.icestream.index.IndexEncoding;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.SerializableTable;

/**
 * Two-input per-task operator that writes per-data-file delete files (V3 puffin DVs or V2
 * pos-delete files) on TaskManagers. Wraps a {@link PerTaskDeleteFileWriter} chosen once at
 * {@link #open}; the V2/V3 difference does not appear in this class.
 *
 * <p>Inputs are co-partitioned by {@code data_file_path} via upstream {@code keyBy(...)} on both
 * sides, so existing-deletes and matches for the same path always land on the same task slot:
 * <ul>
 *   <li>Input 1: {@code (data_file_path, ExistingPerFileDeletes)} — emitted by
 *       {@link ScanDeleteManifestsFlatMap} as it scans the snapshot's delete manifests.
 *   <li>Input 2: {@code Row(spec_id, partition_key_hex, data_file_path, pos)} — match rows
 *       emitted by the Paimon lookup-join.
 * </ul>
 *
 * <p>Both inputs are bounded; {@link BoundedMultiInput#endInput(int)} fires once per input.
 * After both fire, the writer is flushed and a single {@link TaskOutputs} is emitted to the
 * downstream collect-sink, where the master JVM aggregates them into a {@link
 * io.github.jordepic.icestream.converter.CommitPlan}.
 */
public final class WriteDeleteFilesOperator extends AbstractStreamOperator<TaskOutputs>
        implements TwoInputStreamOperator<Tuple2<String, ExistingPerFileDeletes>, Row, TaskOutputs>,
                BoundedMultiInput {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final int formatVersion;

    private transient PerTaskDeleteFileWriter writer;
    private transient int finishedInputs;

    public WriteDeleteFilesOperator(SerializableTable serializableTable, int formatVersion) {
        this.serializableTable = serializableTable;
        this.formatVersion = formatVersion;
    }

    @Override
    public void open() throws Exception {
        super.open();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        long attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
        writer = PerTaskDeleteFileWriters.forTable(serializableTable, formatVersion, subtask, attempt);
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
    public void endInput(int inputId) throws Exception {
        finishedInputs++;
        if (finishedInputs == 2) {
            output.collect(new StreamRecord<>(writer.finishAndClose()));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
        }
    }
}
