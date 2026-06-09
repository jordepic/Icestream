package io.github.jordepic.icestream.flinkpaimon.channel;

import io.github.jordepic.icestream.converter.TaskOutputs;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallelism-1 tail of the standing converter job: gathers every distributed writer subtask's
 * {@link TaskOutputs} for a conversion and hands the assembled list back to the waiting caller via
 * {@link RemoteConversionChannelClient#complete}.
 *
 * <p>How it knows a conversion is complete across N parallel writers: <b>aligned checkpoint
 * barriers</b>. Each writer emits its slice during its own {@code prepareSnapshotPreBarrier(K)},
 * just before forwarding barrier K. This operator has all N writers as input channels, so by the
 * time barrier K is aligned here — i.e. when this operator's {@link #prepareSnapshotPreBarrier}
 * runs — it has already {@code processElement}'d all N slices for epoch K. So the barrier alignment
 * itself is the "end of request" signal across subtasks; no explicit end-of-request marker is needed.
 *
 * <p>Serial dispatch (one conversion in flight) means at most one conversion's slices arrive per
 * epoch. {@code complete} pops the oldest in-flight conversion and completes its future; on an idle
 * epoch (no conversion) the in-flight queue is empty and {@code complete} is a no-op, so calling it
 * every barrier is safe. Requires <b>aligned</b> checkpoints (the default) — unaligned barriers would
 * break the "all slices precede the barrier" guarantee.
 */
public final class CollectConversionOutputsOperator extends AbstractStreamOperator<TaskOutputs>
        implements OneInputStreamOperator<TaskOutputs, TaskOutputs> {

    private static final long serialVersionUID = 1L;
    private static final Logger ICESTREAM_LOG = LoggerFactory.getLogger(CollectConversionOutputsOperator.class);

    private final String jobKey;
    private final RemoteConversionChannelClient channel;

    private transient List<TaskOutputs> epochOutputs;

    public CollectConversionOutputsOperator(String jobKey, RemoteConversionChannelClient channel) {
        this.jobKey = jobKey;
        this.channel = channel;
    }

    @Override
    public void open() throws Exception {
        super.open();
        epochOutputs = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<TaskOutputs> element) {
        epochOutputs.add(element.getValue());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        // All N writer slices for this epoch have been received (barrier alignment). Complete the
        // in-flight conversion with the aggregated list; no-op if this epoch had no conversion.
        // Swallow a transport failure rather than fail the operator — failing here would restart the
        // whole standing job (losing the warm cache). The conversion's future then times out and the
        // master re-dispatches it (idempotent); the warm job stays up.
        try {
            channel.complete(jobKey, new ArrayList<>(epochOutputs));
        } catch (RuntimeException e) {
            ICESTREAM_LOG.warn("conversion complete failed (continuing; conversion will be retried)", e);
        }
        epochOutputs.clear();
    }
}
