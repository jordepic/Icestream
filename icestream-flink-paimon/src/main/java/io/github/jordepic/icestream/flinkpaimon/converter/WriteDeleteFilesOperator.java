package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskDeleteFileWriters;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.SerializableTable;

/**
 * Per-task Flink operator that writes per-data-file delete files (V3 puffin DVs or V2 pos-delete
 * files) on TaskManagers — not the master JVM. Wraps a {@link PerTaskDeleteFileWriter} chosen
 * once at {@link #open()}; the V2/V3 difference does not appear in this class.
 *
 * <p>End-of-input (Flink batch) triggers {@link #endInput} which flushes the writer and emits
 * exactly one {@link TaskOutputs} per task slot to the downstream collect-sink, where the master
 * JVM aggregates them into a {@link io.github.jordepic.icestream.converter.CommitPlan}.
 *
 * <p>Existing per-data-file scoped delete files are pre-collected on the driver via
 * {@link io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader} and shipped as
 * {@link #existingByDataFile} — replayed once into the writer at {@code open()} so subsequent
 * {@link #processElement} calls only write deletes.
 */
public final class WriteDeleteFilesOperator extends AbstractStreamOperator<TaskOutputs>
        implements OneInputStreamOperator<Row, TaskOutputs>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final int formatVersion;
    private final Map<String, ExistingPerFileDeletes> existingByDataFile;

    private transient PerTaskDeleteFileWriter writer;

    public WriteDeleteFilesOperator(
            SerializableTable serializableTable,
            int formatVersion,
            Map<String, ExistingPerFileDeletes> existingByDataFile) {
        this.serializableTable = serializableTable;
        this.formatVersion = formatVersion;
        this.existingByDataFile = existingByDataFile;
    }

    @Override
    public void open() throws Exception {
        super.open();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        long attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
        writer = PerTaskDeleteFileWriters.forTable(serializableTable, formatVersion, subtask, attempt);
        existingByDataFile.forEach(writer::registerExisting);
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
    public void endInput() throws Exception {
        output.collect(new StreamRecord<>(writer.finishAndClose()));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close();
        }
    }
}
