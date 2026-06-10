package io.github.jordepic.icestream.flinkpaimon.master;

import static io.github.jordepic.icestream.master.EnvConfig.envOrDefault;
import static io.github.jordepic.icestream.master.EnvConfig.optionsFromEnv;
import static io.github.jordepic.icestream.master.EnvConfig.requireEnv;
import static io.github.jordepic.icestream.master.LifecycleHooks.closeQuietly;

import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.flinkpaimon.job.IcebergCatalogSpec;
import io.github.jordepic.icestream.flinkpaimon.job.IcestreamTableJob;
import io.github.jordepic.icestream.flinkpaimon.job.PaimonIndexSpec;
import io.github.jordepic.icestream.master.IcestreamCatalogScan;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launcher/supervisor for the Flink+Paimon backend. Unlike the old design — a master control loop
 * that drove index/convert work itself and shipped conversions to a warm job over an RPC channel —
 * this process only <em>launches and supervises</em> jobs: it discovers icestream tables and submits
 * one long-lived autonomous {@link IcestreamTableJob} per table to the (local or session) cluster.
 * Each job walks the planner, indexes, converts, commits, and advances its own watermark; all
 * correctness lives in the job's source + committer, so there is no driver to keep in step and no
 * channel.
 */
public final class FlinkMain {

    private static final Logger log = LoggerFactory.getLogger(FlinkMain.class);

    private FlinkMain() {}

    public static void main(String[] args) {
        Config config = Config.fromEnv();
        log.info("Starting icestream Flink+Paimon launcher {}", config);
        run(config);
    }

    private static void run(Config config) {
        boolean remoteFlink = "remote".equals(config.flinkMode());
        Catalog icebergCatalog = buildIcebergCatalog(config);
        IcebergCatalogSpec catalogSpec = new IcebergCatalogSpec(
                "org.apache.iceberg.rest.RESTCatalog", "icestream-rest", icebergCatalogProps(config));
        FlinkContext flink = remoteFlink
                ? FlinkContext.remote(
                        config.flinkJmHost(), config.flinkJmPort(), config.flinkParallelism(), config.flinkJars())
                : FlinkContext.local(config.flinkParallelism());
        PaimonIndex paimonIndex =
                PaimonIndex.create(config.paimonWarehouse(), config.paimonDatabase(), config.paimonOptions());
        PaimonIndexSpec indexSpec =
                new PaimonIndexSpec(config.paimonWarehouse(), config.paimonDatabase(), config.paimonOptions());

        Supervisor supervisor =
                new Supervisor(config, icebergCatalog, catalogSpec, flink, paimonIndex, indexSpec);

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    log.info("Shutdown signal received");
                    supervisor.stop();
                    closeQuietly(flink::close, "flink");
                    closeQuietly(paimonIndex.catalog()::close, "paimon-catalog");
                    if (icebergCatalog instanceof Closeable c) {
                        closeQuietly(c::close, "iceberg-catalog");
                    }
                },
                "icestream-shutdown"));

        supervisor.run();
    }

    /** Discovers icestream tables and keeps one autonomous job per table submitted and alive. */
    private static final class Supervisor {

        private final Config config;
        private final Catalog catalog;
        private final IcebergCatalogSpec catalogSpec;
        private final FlinkContext flink;
        private final PaimonIndex paimonIndex;
        private final PaimonIndexSpec indexSpec;
        private final Map<TableIdentifier, JobClient> jobs = new ConcurrentHashMap<>();
        private volatile boolean running = true;

        Supervisor(
                Config config,
                Catalog catalog,
                IcebergCatalogSpec catalogSpec,
                FlinkContext flink,
                PaimonIndex paimonIndex,
                PaimonIndexSpec indexSpec) {
            this.config = config;
            this.catalog = catalog;
            this.catalogSpec = catalogSpec;
            this.flink = flink;
            this.paimonIndex = paimonIndex;
            this.indexSpec = indexSpec;
        }

        void run() {
            while (running) {
                try {
                    sweep();
                    Thread.sleep(config.pollIntervalMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Supervisor sweep failed; retrying after poll interval", e);
                    if (!sleep(config.pollIntervalMs())) {
                        break;
                    }
                }
            }
            jobs.values().forEach(Supervisor::cancelQuietly);
            jobs.clear();
        }

        private void sweep() {
            for (TableIdentifier id : IcestreamCatalogScan.icestreamTables(catalog)) {
                if (isRunning(jobs.get(id))) {
                    continue;
                }
                try {
                    launch(id);
                } catch (Exception e) {
                    log.warn("Failed to launch icestream job for {}; will retry next sweep", id, e);
                }
            }
        }

        private void launch(TableIdentifier id) {
            Table table = catalog.loadTable(id);
            IcestreamTableConfig tableConfig = IcestreamTableConfig.from(table);
            paimonIndex.initializeForTable(id, tableConfig);
            JobClient client = IcestreamTableJob.submit(
                    flink,
                    paimonIndex,
                    catalogSpec,
                    indexSpec,
                    id,
                    table,
                    config.streamingCheckpointMs(),
                    config.pollIntervalMs(),
                    config.idleBackoffMs(),
                    config.paimonCommitTimeoutMs());
            jobs.put(id, client);
            log.info("Launched icestream job for {}", id);
        }

        private static boolean isRunning(JobClient client) {
            if (client == null) {
                return false;
            }
            try {
                JobStatus status = client.getJobStatus().get();
                return !status.isGloballyTerminalState();
            } catch (Exception e) {
                return false;
            }
        }

        void stop() {
            running = false;
        }

        private static void cancelQuietly(JobClient client) {
            try {
                client.cancel().get();
            } catch (Exception ignored) {
                // best-effort
            }
        }

        private static boolean sleep(long ms) {
            try {
                Thread.sleep(ms);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private static Map<String, String> icebergCatalogProps(Config config) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, config.icebergRestUri());
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.icebergWarehouse());
        config.icebergCatalogOptions().forEach(props::put);
        return props;
    }

    private static Catalog buildIcebergCatalog(Config config) {
        return CatalogUtil.loadCatalog(
                "org.apache.iceberg.rest.RESTCatalog", "icestream-rest", icebergCatalogProps(config), new Configuration());
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
            long streamingCheckpointMs,
            long paimonCommitTimeoutMs,
            long pollIntervalMs,
            long idleBackoffMs) {

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
                    Long.parseLong(envOrDefault("ICESTREAM_STREAMING_CHECKPOINT_MS", "2000")),
                    Long.parseLong(envOrDefault("ICESTREAM_PAIMON_COMMIT_TIMEOUT_MS", "600000")),
                    Long.parseLong(envOrDefault("ICESTREAM_POLL_INTERVAL_SECONDS", "10")) * 1000L,
                    Long.parseLong(envOrDefault("ICESTREAM_IDLE_BACKOFF_SECONDS", "60")) * 1000L);
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
