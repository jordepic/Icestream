package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.DvInfo;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.PerTaskDvWriter;
import io.github.jordepic.icestream.converter.writers.PerTaskPosDeleteWriter;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.SerializableTable;

/**
 * Per-task Flink operator that writes per-data-file delete files (V3 puffin DVs or V2 pos-delete
 * files) on TaskManagers — not the master JVM. Wraps {@link PerTaskDvWriter} /
 * {@link PerTaskPosDeleteWriter}; runs after the lookup-join produces matched
 * {@code (spec_id, partition_key, data_file_path, pos)} rows, with a {@code keyBy(data_file_path)}
 * upstream so all matches for a given data file land on the same TM.
 *
 * <p>End-of-input (Flink batch) triggers {@link #endInput} which flushes the writer and emits
 * exactly one {@link TaskOutputs} per task slot to the downstream collect-sink, where the master
 * JVM aggregates them into a {@link io.github.jordepic.icestream.converter.CommitPlan}.
 *
 * <p>Existing per-data-file scoped delete files (DVs in V3, FILE-scoped pos-deletes in V2) are
 * pre-collected on the driver and shipped as {@link #existingDvs} or {@link #existingPosDeletes}
 * — both small driver-collected maps, registered with the writer the first time a matched row
 * for that data file arrives.
 */
public final class WriteDeleteFilesOperator extends AbstractStreamOperator<TaskOutputs>
        implements OneInputStreamOperator<Row, TaskOutputs>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final int formatVersion;
    private final Map<String, DvInfo> existingDvs;
    private final Map<String, List<DeleteFile>> existingPosDeletes;

    private transient PerTaskDvWriter dvWriter;
    private transient PerTaskPosDeleteWriter posDeleteWriter;
    private transient Set<String> registeredPaths;

    public WriteDeleteFilesOperator(
            SerializableTable serializableTable,
            int formatVersion,
            Map<String, DvInfo> existingDvs,
            Map<String, List<DeleteFile>> existingPosDeletes) {
        this.serializableTable = serializableTable;
        this.formatVersion = formatVersion;
        this.existingDvs = existingDvs;
        this.existingPosDeletes = existingPosDeletes;
    }

    @Override
    public void open() throws Exception {
        super.open();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        long attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
        registeredPaths = new HashSet<>();
        if (formatVersion == 3) {
            dvWriter = new PerTaskDvWriter(serializableTable, subtask, attempt);
        } else {
            posDeleteWriter = new PerTaskPosDeleteWriter(serializableTable, subtask, attempt);
        }
    }

    @Override
    public void processElement(StreamRecord<Row> element) {
        Row row = element.getValue();
        int specId = (Integer) row.getField(0);
        byte[] partitionBytes = IndexEncoding.fromHex((String) row.getField(1));
        String dataFilePath = (String) row.getField(2);
        long pos = (Long) row.getField(3);

        if (registeredPaths.add(dataFilePath)) {
            if (formatVersion == 3) {
                DvInfo existing = existingDvs.get(dataFilePath);
                if (existing != null) {
                    dvWriter.registerExistingDv(dataFilePath, existing);
                }
            } else {
                List<DeleteFile> existing = existingPosDeletes.getOrDefault(dataFilePath, List.of());
                posDeleteWriter.registerExistingPosDeletes(dataFilePath, existing);
            }
        }
        if (formatVersion == 3) {
            dvWriter.delete(dataFilePath, pos, specId, partitionBytes);
        } else {
            posDeleteWriter.delete(dataFilePath, pos, specId, partitionBytes);
        }
    }

    @Override
    public void endInput() throws Exception {
        TaskOutputs result = formatVersion == 3 ? dvWriter.finishAndClose() : posDeleteWriter.finishAndClose();
        output.collect(new StreamRecord<>(result));
    }

    @Override
    public void close() throws Exception {
        super.close();
        // finishAndClose already closed the underlying writers; defensive close in case endInput
        // never fired (e.g. job failure before reaching end-of-input).
        if (dvWriter != null) {
            dvWriter.close();
        }
        if (posDeleteWriter != null) {
            posDeleteWriter.close();
        }
    }

    /** Pre-collected existing-deletes wired through the operator's serializable state. */
    static Map<String, DvInfo> serializableDvMap(Map<String, DvInfo> source) {
        return new HashMap<>(source);
    }

    static Map<String, List<DeleteFile>> serializablePosDeleteMap(Map<String, List<DeleteFile>> source) {
        return new HashMap<>(source);
    }
}
