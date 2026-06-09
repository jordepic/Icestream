package io.github.jordepic.icestream.flinkpaimon.channel;

import io.github.jordepic.icestream.converter.TaskOutputs;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In-process work-in / results-out transport between the master JVM (which calls
 * {@code DeleteConverter.create}) and the standing streaming converter job, which runs in the same
 * JVM under a local {@code MiniCluster}. This is the P1 transport; a remote (cross-JVM) transport
 * for session clusters is P2 (see STREAMING_CONVERTER.md).
 *
 * <p>One {@code Channel} per running job, keyed by an opaque {@code jobKey}. The model is
 * <em>serial</em>: the master submits one {@link ConversionRequest} and blocks on its future before
 * submitting the next, so at most one conversion is ever in flight. Flow:
 * <ol>
 *   <li>master: {@link #submit} enqueues the request and gets a future.
 *   <li>source: {@link #poll} takes it; under the source's checkpoint lock it calls
 *       {@link #markInFlight} and emits the work items — so the conversion is wholly contained in
 *       one checkpoint epoch.
 *   <li>writer (parallelism 1): at the next checkpoint barrier (which, by barrier ordering, follows
 *       all of the conversion's matches) it calls {@link #complete} with the task outputs.
 * </ol>
 */
public final class InProcessConversionChannel {

    private InProcessConversionChannel() {}

    private static final Map<String, Channel> CHANNELS = new ConcurrentHashMap<>();

    public static void register(String jobKey) {
        CHANNELS.put(jobKey, new Channel());
    }

    public static void unregister(String jobKey) {
        Channel channel = CHANNELS.remove(jobKey);
        if (channel != null) {
            channel.pending
                    .values()
                    .forEach(f -> f.completeExceptionally(new IllegalStateException("conversion channel closed")));
        }
    }

    private static Channel channel(String jobKey) {
        Channel channel = CHANNELS.get(jobKey);
        if (channel == null) {
            throw new IllegalStateException("No conversion channel registered for " + jobKey);
        }
        return channel;
    }

    /** Master side: enqueue a conversion; the returned future completes with its task outputs. */
    public static CompletableFuture<List<TaskOutputs>> submit(String jobKey, ConversionRequest request) {
        Channel channel = channel(jobKey);
        CompletableFuture<List<TaskOutputs>> future = new CompletableFuture<>();
        channel.pending.put(request.conversionId(), future);
        channel.queue.add(request);
        return future;
    }

    /** Source side: block up to {@code timeoutMs} for the next request (null on timeout to re-poll). */
    public static ConversionRequest poll(String jobKey, long timeoutMs) throws InterruptedException {
        return channel(jobKey).queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Source side, under the checkpoint lock: record that this conversion's records are being emitted. */
    public static void markInFlight(String jobKey, long conversionId) {
        channel(jobKey).inFlight.add(conversionId);
    }

    /**
     * Writer side, at a checkpoint barrier: complete the oldest in-flight conversion's future with
     * its task outputs. No-op if no conversion ran this epoch (idle checkpoint).
     */
    public static void complete(String jobKey, List<TaskOutputs> outputs) {
        Channel channel = channel(jobKey);
        Long conversionId = channel.inFlight.poll();
        if (conversionId == null) {
            return; // idle epoch, nothing to complete
        }
        CompletableFuture<List<TaskOutputs>> future = channel.pending.remove(conversionId);
        if (future != null) {
            future.complete(outputs);
        }
    }

    private static final class Channel {
        final BlockingQueue<ConversionRequest> queue = new LinkedBlockingQueue<>();
        final ConcurrentLinkedQueue<Long> inFlight = new ConcurrentLinkedQueue<>();
        final Map<Long, CompletableFuture<List<TaskOutputs>>> pending = new ConcurrentHashMap<>();
    }
}
