package io.github.jordepic.icestream.flinkpaimon.master;

import io.github.jordepic.icestream.flinkpaimon.converter.FlinkDeleteFileCreator;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonCatalogFactory;
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
        log.info("Starting icestream Flink+Paimon master with {}", config);

        Catalog icebergCatalog = buildIcebergCatalog(config);
        FlinkContext flink = config.flinkMode().equals("remote")
                ? FlinkContext.remote(config.flinkJmHost(), config.flinkJmPort(), config.flinkParallelism())
                : FlinkContext.local(config.flinkParallelism());

        PaimonIndex paimonIndex =
                PaimonIndex.create(config.paimonWarehouse(), config.paimonDatabase(), config.paimonOptions());
        TableProcessor processor = new TableProcessor(
                new SnapshotPlanner(),
                new FlinkDataFileIndexer(flink, paimonIndex),
                new FlinkDeleteFileCreator(flink, paimonIndex),
                paimonIndex,
                IcestreamMetrics.NOOP);
        MasterLoop loop = new MasterLoop(
                icebergCatalog, processor, config.pollInterval(), config.idleBackoff(), config.maxConcurrentTasks());

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    log.info("Shutdown signal received");
                    loop.stop();
                    closeQuietly(flink::close, "flink");
                    closeQuietly(paimonIndex.catalog()::close, "paimon-catalog");
                    if (icebergCatalog instanceof Closeable c) {
                        closeQuietly(c::close, "iceberg-catalog");
                    }
                },
                "icestream-shutdown"));

        loop.run();
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

    private static void closeQuietly(ThrowingRunnable closer, String label) {
        try {
            closer.run();
        } catch (Exception e) {
            log.warn("Error closing {}", label, e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
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
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_POLL_INTERVAL_SECONDS", "10"))),
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_IDLE_BACKOFF_SECONDS", "60"))),
                    Integer.parseInt(envOrDefault("ICESTREAM_MAX_CONCURRENT_TASKS", "4")));
        }

        private static Map<String, String> optionsFromEnv(String prefix) {
            Map<String, String> out = new HashMap<>();
            System.getenv().forEach((k, v) -> {
                if (k.startsWith(prefix)) {
                    String key = k.substring(prefix.length()).toLowerCase().replace('_', '.');
                    out.put(key, v);
                }
            });
            return out;
        }

        private static String requireEnv(String key) {
            String value = System.getenv(key);
            if (value == null || value.isBlank()) {
                throw new IllegalStateException("Missing required env var " + key);
            }
            return value;
        }

        private static String envOrDefault(String key, String fallback) {
            String value = System.getenv(key);
            return (value == null || value.isBlank()) ? fallback : value;
        }
    }
}
