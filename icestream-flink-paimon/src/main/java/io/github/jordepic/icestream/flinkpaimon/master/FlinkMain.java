package io.github.jordepic.icestream.flinkpaimon.master;

import static io.github.jordepic.icestream.master.EnvConfig.envOrDefault;
import static io.github.jordepic.icestream.master.EnvConfig.optionsFromEnv;
import static io.github.jordepic.icestream.master.EnvConfig.requireEnv;
import static io.github.jordepic.icestream.master.LifecycleHooks.closeQuietly;

import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.flinkpaimon.converter.FlinkDeleteFileCreator;
import io.github.jordepic.icestream.flinkpaimon.converter.StreamingFlinkDeleteFileCreator;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.flinkpaimon.indexer.FlinkDataFileIndexer;
import io.github.jordepic.icestream.master.IcestreamMetrics;
import io.github.jordepic.icestream.master.MasterLoop;
import io.github.jordepic.icestream.master.TableProcessor;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlinkMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkMain.class);

    private FlinkMain() {}

    public static void main(String[] args) {
        Config config = Config.fromEnv();
        log.info("Starting icestream Flink+Paimon {}", config);
        run(config);
    }

    /**
     * One process: runs the master control loop (poll → plan → index → convert → commit → watermark)
     * and, in the same JVM, owns the converter that drives the Flink job. The streaming converter
     * hosts the channel server its job's operators dial back into for work-in/results-out; with
     * {@code FLINK_MODE=remote} the job runs on a session cluster and the TaskManagers reach the
     * advertised host over the network, with {@code local} an in-pod MiniCluster reaches it over
     * loopback. The master logic calls the converter/indexer directly — there is no separate
     * "worker" process or RPC hop.
     */
    private static void run(Config config) {
        boolean remoteFlink = "remote".equals(config.flinkMode());
        boolean streaming = !"batch".equals(config.converter());
        Catalog icebergCatalog = buildIcebergCatalog(config);
        FlinkContext flink = remoteFlink
                ? FlinkContext.remote(
                        config.flinkJmHost(), config.flinkJmPort(), config.flinkParallelism(), config.flinkJars())
                : FlinkContext.local(config.flinkParallelism());
        PaimonIndex paimonIndex =
                PaimonIndex.create(config.paimonWarehouse(), config.paimonDatabase(), config.paimonOptions());

        DeleteConverter converter = buildConverter(config, flink, paimonIndex, remoteFlink, streaming);
        TableProcessor processor = new TableProcessor(
                new SnapshotPlanner(),
                new FlinkDataFileIndexer(flink, paimonIndex),
                converter,
                paimonIndex,
                IcestreamMetrics.NOOP);
        MasterLoop loop = new MasterLoop(
                icebergCatalog, processor, config.pollInterval(), config.idleBackoff(), config.maxConcurrentTasks());

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    log.info("Shutdown signal received");
                    loop.stop();
                    if (converter instanceof AutoCloseable c) {
                        closeQuietly(c::close, "converter");
                    }
                    closeQuietly(flink::close, "flink");
                    closeQuietly(paimonIndex.catalog()::close, "paimon-catalog");
                    if (icebergCatalog instanceof Closeable c) {
                        closeQuietly(c::close, "iceberg-catalog");
                    }
                },
                "icestream-shutdown"));

        loop.run();
    }

    /**
     * Selects the converter.
     *
     * <p>{@code streaming} (the default, production) = one long-lived warm Flink job whose Paimon
     * lookup cache stays warm across conversions (see {@link StreamingFlinkDeleteFileCreator}),
     * avoiding the per-conversion SST/lookup rebuild that dominates cost. The converter hosts the
     * channel server itself; on a remote cluster the TaskManagers reach it at the configured
     * advertised host:port, in a local MiniCluster over loopback.
     *
     * <p>{@code batch} = a fresh Flink job per conversion that rebuilds the lookup state each time —
     * <b>testing/benchmarking only</b> (O(index) per conversion). It's self-contained (no channel).
     */
    private static DeleteConverter buildConverter(
            Config config, FlinkContext flink, PaimonIndex paimonIndex, boolean remoteFlink, boolean streaming) {
        if (!streaming) {
            log.warn("Using BATCH converter — rebuilds lookup state per conversion; intended for "
                    + "testing/benchmarking only, not production. Set ICESTREAM_CONVERTER=streaming.");
            return new FlinkDeleteFileCreator(flink, paimonIndex);
        }
        log.info("Using streaming converter (warm lookup cache, checkpoint={}ms, advertised channel host={})",
                config.streamingCheckpointMs(), remoteFlink ? config.channelAdvertisedHost() : "localhost");
        return remoteFlink
                ? new StreamingFlinkDeleteFileCreator(
                        flink, paimonIndex, config.streamingCheckpointMs(), config.conversionTimeoutMs(),
                        config.channelPort(), config.channelAdvertisedHost())
                : new StreamingFlinkDeleteFileCreator(
                        flink, paimonIndex, config.streamingCheckpointMs(), config.conversionTimeoutMs());
    }

    private static Catalog buildIcebergCatalog(Config config) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, config.icebergRestUri());
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.icebergWarehouse());
        config.icebergCatalogOptions().forEach(props::put);
        return CatalogUtil.loadCatalog(
                "org.apache.iceberg.rest.RESTCatalog",
                "icestream-rest",
                props,
                new Configuration());
    }

    record Config(
            String icebergRestUri,
            String icebergWarehouse,
            Map<String, String> icebergCatalogOptions,
            String paimonWarehouse,
            String paimonDatabase,
            Map<String, String> paimonOptions,
            String flinkMode,
            String flinkJmHost,
            int flinkJmPort,
            int flinkParallelism,
            String[] flinkJars,
            String converter,
            long streamingCheckpointMs,
            long conversionTimeoutMs,
            int channelPort,
            String channelAdvertisedHost,
            Duration pollInterval,
            Duration idleBackoff,
            int maxConcurrentTasks) {

        static Config fromEnv() {
            return new Config(
                    requireEnv("ICESTREAM_ICEBERG_REST_URI"),
                    requireEnv("ICESTREAM_ICEBERG_WAREHOUSE"),
                    optionsFromEnv("ICESTREAM_ICEBERG_OPT_"),
                    requireEnv("ICESTREAM_PAIMON_WAREHOUSE"),
                    envOrDefault("ICESTREAM_PAIMON_DATABASE", "icestream"),
                    optionsFromEnv("ICESTREAM_PAIMON_OPT_"),
                    envOrDefault("ICESTREAM_FLINK_MODE", "local"),
                    envOrDefault("ICESTREAM_FLINK_JM_HOST", "localhost"),
                    Integer.parseInt(envOrDefault("ICESTREAM_FLINK_JM_PORT", "8081")),
                    Integer.parseInt(envOrDefault("ICESTREAM_FLINK_PARALLELISM", "4")),
                    parseJars(envOrDefault("ICESTREAM_FLINK_JARS", "")),
                    envOrDefault("ICESTREAM_CONVERTER", "streaming"),
                    Long.parseLong(envOrDefault("ICESTREAM_STREAMING_CHECKPOINT_MS", "2000")),
                    Long.parseLong(envOrDefault("ICESTREAM_CONVERSION_TIMEOUT_MS", "600000")),
                    Integer.parseInt(envOrDefault("ICESTREAM_CHANNEL_PORT", "8090")),
                    // Host a remote TaskManager uses to reach THIS process's channel server (streaming +
                    // remote). Defaults to localhost (fine for a same-host standalone cluster); set to a
                    // routable host/Service DNS when the session cluster runs elsewhere.
                    envOrDefault("ICESTREAM_CHANNEL_ADVERTISED_HOST", "localhost"),
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_POLL_INTERVAL_SECONDS", "10"))),
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_IDLE_BACKOFF_SECONDS", "60"))),
                    Integer.parseInt(envOrDefault("ICESTREAM_MAX_CONCURRENT_TASKS", "4")));
        }

        private static String[] parseJars(String csv) {
            if (csv.isBlank()) {
                return new String[0];
            }
            return java.util.Arrays.stream(csv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
        }
    }
}
