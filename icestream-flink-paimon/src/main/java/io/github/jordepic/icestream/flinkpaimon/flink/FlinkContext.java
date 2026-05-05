package io.github.jordepic.icestream.flinkpaimon.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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

    public StreamTableEnvironment newBatchTableEnv() {
        return StreamTableEnvironment.create(newBatchEnv());
    }

    @Override
    public void close() {
        // Nothing to release: createLocalEnvironment / createRemoteEnvironment don't allocate
        // long-lived resources at the FlinkContext level. Per-env cleanup happens when each
        // env's execute() returns.
    }

    private enum Mode {
        LOCAL,
        REMOTE
    }
}
