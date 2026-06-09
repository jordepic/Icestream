package io.github.jordepic.icestream.flinkpaimon.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Long-lived handle the master pod uses to spawn a Flink {@link StreamExecutionEnvironment} per
 * fileRun job. Two flavors:
 * <ul>
 *   <li>{@link #local} — in-process MiniCluster. Used by IT and component tests, and as the dev
 *       fallback when no session cluster is available. Configurable parallelism caps the
 *       worker-pool size in-pod.
 *   <li>{@link #remote} — submits jobs to a long-lived Flink session cluster (typically managed
 *       by flink-kubernetes-operator). Per-fileRun {@link StreamExecutionEnvironment#execute}
 *       blocks until the job completes; submission is REST.
 * </ul>
 *
 * <p>Each {@link #newBatchEnv} call produces a fresh env — Flink envs hold mutable JobGraph state
 * and aren't safe to reuse across jobs. {@code BATCH} runtime mode is set on every env so the
 * Table API picks the batch optimizer and Paimon's batch sink emits a finite stream.
 */
public final class FlinkContext implements AutoCloseable {

    private final Mode mode;
    private final String remoteHost;
    private final int remotePort;
    private final String[] jarFiles;
    private final int parallelism;

    private FlinkContext(Mode mode, String remoteHost, int remotePort, String[] jarFiles, int parallelism) {
        this.mode = mode;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.jarFiles = jarFiles;
        this.parallelism = parallelism;
    }

    public static FlinkContext local(int parallelism) {
        return new FlinkContext(Mode.LOCAL, null, 0, new String[0], parallelism);
    }

    public static FlinkContext remote(String jmHost, int jmPort, int parallelism, String... jarFiles) {
        return new FlinkContext(Mode.REMOTE, jmHost, jmPort, jarFiles.clone(), parallelism);
    }

    public StreamExecutionEnvironment newBatchEnv() {
        StreamExecutionEnvironment env = switch (mode) {
            case LOCAL -> StreamExecutionEnvironment.createLocalEnvironment(parallelism);
            case REMOTE -> StreamExecutionEnvironment.createRemoteEnvironment(remoteHost, remotePort, jarFiles);
        };
        env.setParallelism(parallelism);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }

    /**
     * A fresh {@code STREAMING} env for the long-lived streaming converter. Unlike
     * {@link #newBatchEnv}, the job this builds runs indefinitely (an unbounded source), keeping its
     * operators — and Paimon's lookup-file cache — warm across conversions.
     */
    public StreamExecutionEnvironment newStreamEnv() {
        StreamExecutionEnvironment env = switch (mode) {
            case LOCAL -> StreamExecutionEnvironment.createLocalEnvironment(parallelism);
            case REMOTE -> StreamExecutionEnvironment.createRemoteEnvironment(remoteHost, remotePort, jarFiles);
        };
        env.setParallelism(parallelism);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        return env;
    }

    @Override
    public void close() {}

    private enum Mode {
        LOCAL,
        REMOTE
    }
}
