package io.github.jordepic.icestream.master;

import com.datastax.oss.driver.api.core.CqlSession;
import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.converter.DeleteFileCreator;
import io.github.jordepic.icestream.indexer.DataFileIndexer;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {}

    public static void main(String[] args) {
        Config config = Config.fromEnv();
        log.info("Starting icestream master with {}", config);

        SparkSession spark = buildSpark(config);
        CqlSession cql = buildCqlSession(config);
        ensureKeyspace(cql, config.keyspace());
        Catalog catalog = buildCatalog(config);

        CassandraIndex cassandra = new CassandraIndex(cql, config.keyspace());
        TableProcessor processor = new TableProcessor(
                new SnapshotPlanner(),
                new DataFileIndexer(spark, cassandra),
                new DeleteFileCreator(spark, cassandra),
                cassandra);
        MasterLoop loop = new MasterLoop(
                catalog, processor, config.pollInterval(), config.idleBackoff(), config.maxConcurrentTasks());

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    log.info("Shutdown signal received");
                    loop.stop();
                    closeQuietly(spark::stop, "spark");
                    closeQuietly(cql::close, "cassandra");
                    if (catalog instanceof Closeable c) {
                        closeQuietly(c::close, "catalog");
                    }
                },
                "icestream-shutdown"));

        loop.run();
    }

    private static SparkSession buildSpark(Config config) {
        return SparkSession.builder()
                .master(config.sparkMaster())
                .appName("icestream-master")
                .config("spark.cassandra.connection.host", config.cassandraHost())
                .config("spark.cassandra.connection.port", String.valueOf(config.cassandraPort()))
                .config("spark.cassandra.connection.localDC", config.cassandraLocalDc())
                .getOrCreate();
    }

    private static CqlSession buildCqlSession(Config config) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.cassandraHost(), config.cassandraPort()))
                .withLocalDatacenter(config.cassandraLocalDc())
                .build();
    }

    private static void ensureKeyspace(CqlSession cql, String keyspace) {
        cql.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    private static Catalog buildCatalog(Config config) {
        Map<String, String> props = Map.of(
                CatalogProperties.URI, config.catalogUri(),
                CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        return CatalogUtil.loadCatalog(
                "org.apache.iceberg.rest.RESTCatalog", "icestream", props, new Configuration());
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
            String catalogUri,
            String warehouse,
            String cassandraHost,
            int cassandraPort,
            String cassandraLocalDc,
            String keyspace,
            String sparkMaster,
            Duration pollInterval,
            Duration idleBackoff,
            int maxConcurrentTasks) {

        static Config fromEnv() {
            return new Config(
                    requireEnv("ICESTREAM_CATALOG_URI"),
                    requireEnv("ICESTREAM_WAREHOUSE"),
                    requireEnv("ICESTREAM_CASSANDRA_HOST"),
                    Integer.parseInt(envOrDefault("ICESTREAM_CASSANDRA_PORT", "9042")),
                    requireEnv("ICESTREAM_CASSANDRA_LOCAL_DC"),
                    envOrDefault("ICESTREAM_KEYSPACE", "icestream"),
                    envOrDefault("ICESTREAM_SPARK_MASTER", "local[*]"),
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_POLL_INTERVAL_SECONDS", "10"))),
                    Duration.ofSeconds(Long.parseLong(envOrDefault("ICESTREAM_IDLE_BACKOFF_SECONDS", "60"))),
                    Integer.parseInt(envOrDefault("ICESTREAM_MAX_CONCURRENT_TASKS", "4")));
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
