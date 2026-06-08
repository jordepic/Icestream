package io.github.jordepic.icestream.flinkpaimon.channel;

import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unbounded polling source that keeps the standing converter job alive (so the downstream lookup
 * operator's {@code LocalTableQuery} cache stays warm across conversions). It drains
 * {@link ConversionRequest}s from the {@link RemoteConversionChannelClient} and emits them.
 *
 * <p>Each conversion's work items are emitted <em>under the checkpoint lock</em>, with
 * {@link RemoteConversionChannelClient#markInFlight} called in the same critical section, so the
 * conversion is wholly contained within one checkpoint epoch — the checkpoint barrier that follows
 * triggers the writer flush that produces exactly this conversion's outputs. Keeping markInFlight
 * inside the lock (rather than folding it into the poll) is what prevents an empty-barrier race: a
 * checkpoint cannot interleave between marking in-flight and emitting.
 *
 * <p>The {@code channel} always reaches the driver's channel state over HTTP: the loopback when the
 * job runs in an in-JVM MiniCluster, or the network when it runs on a remote TaskManager — the
 * source code is identical either way.
 *
 * <p>P1 uses the legacy {@code SourceFunction} (still available in Flink 2.0) for its
 * checkpoint-lock semantics, which give the atomic-emit guarantee with minimal code; a FLIP-27
 * migration is a P2 cleanup.
 */
public final class ConversionRequestSource extends RichSourceFunction<ConversionRequest> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConversionRequestSource.class);

    private final String jobKey;
    private final RemoteConversionChannelClient channel;
    private volatile boolean running = true;

    public ConversionRequestSource(String jobKey, RemoteConversionChannelClient channel) {
        this.jobKey = jobKey;
        this.channel = channel;
    }

    @Override
    public void run(SourceContext<ConversionRequest> ctx) throws Exception {
        while (running) {
            // A 5s long-poll keeps the request volume (and connection churn) low — the channel
            // transport's stale-connection resets scale with the number of polls. Any error from the
            // channel is logged and swallowed: the source must NEVER fail, or the whole standing job
            // restarts and we lose the warm cache. A request lost to a transport hiccup times out at
            // the master, which re-dispatches it (idempotent).
            try {
                ConversionRequest request = channel.poll(jobKey, 5000);
                if (request == null) {
                    continue;
                }
                synchronized (ctx.getCheckpointLock()) {
                    channel.markInFlight(jobKey, request.conversionId());
                    ctx.collect(request);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                LOG.warn("conversion source poll/emit error (continuing)", e);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
