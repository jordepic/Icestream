package io.github.jordepic.icestream.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.converter.ConversionCommitter;
import io.github.jordepic.icestream.converter.DeleteFileCreator;
import io.github.jordepic.icestream.indexer.DataFileIndexer;
import io.github.jordepic.icestream.master.IcestreamMetrics;
import io.github.jordepic.icestream.master.MasterLoop;
import io.github.jordepic.icestream.master.TableProcessor;
import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcestreamEndToEndIT {

    private static final Logger log = LoggerFactory.getLogger(IcestreamEndToEndIT.class);

    private static final String S3_BUCKET = "warehouse";
    private static final String AWS_ACCESS_KEY = "minioadmin";
    private static final String AWS_SECRET_KEY = "minioadmin";
    private static final String CASSANDRA_KEYSPACE = "icestream_it";

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "events");
    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()),
            NestedField.required(3, "ts", Types.LongType.get()));

    private static final String KAFKA_TOPIC = "icestream-input";
    private static final String FLINK_IMAGE = "flink:2.0.0-scala_2.12-java17";
    private static final String FLINK_PROPERTIES = "jobmanager.rpc.address: flink-jm\n"
            + "taskmanager.numberOfTaskSlots: 2\n"
            + "execution.attached: false\n"
            + "rest.address: flink-jm\n"
            + "rest.port: 8081\n"
            + "rest.bind-address: 0.0.0.0\n";

    private static final Duration RUN_DURATION = Duration.ofSeconds(60);
    private static final Duration ICESTREAM_POLL = Duration.ofSeconds(5);
    private static final Duration ICESTREAM_IDLE_BACKOFF = Duration.ofSeconds(2);
    private static final Duration FINAL_CHECKPOINT_WAIT = Duration.ofSeconds(20);

    private static Network network;
    private static MinIOContainer minio;
    private static GenericContainer<?> rest;
    private static CassandraContainer cassandra;
    private static ConfluentKafkaContainer kafka;
    private static GenericContainer<?> flinkJobManager;
    private static GenericContainer<?> flinkTaskManager;
    private static GenericContainer<?> sparkCompaction;
    private static Path flinkLibDir;
    private static Path sparkCompactionLibDir;

    @BeforeAll
    static void startStack() {
        network = Network.newNetwork();

        minio = new MinIOContainer("minio/minio:latest")
                .withUserName(AWS_ACCESS_KEY)
                .withPassword(AWS_SECRET_KEY)
                .withNetwork(network)
                .withNetworkAliases("minio");
        minio.start();

        try (GenericContainer<?> mc = new GenericContainer<>(DockerImageName.parse("minio/mc:latest"))
                .withNetwork(network)
                .withEnv("MC_HOST_local", "http://" + AWS_ACCESS_KEY + ":" + AWS_SECRET_KEY + "@minio:9000")
                .withCommand("mb", "-p", "local/" + S3_BUCKET)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy())) {
            mc.start();
        }

        rest = new GenericContainer<>(DockerImageName.parse("apache/iceberg-rest-fixture:1.10.1"))
                .withNetwork(network)
                .withNetworkAliases("rest")
                .withExposedPorts(8181)
                .withEnv("CATALOG_WAREHOUSE", "s3://" + S3_BUCKET + "/")
                .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
                .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                .withEnv("AWS_REGION", "us-east-1")
                .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY)
                .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_KEY)
                .waitingFor(Wait.forHttp("/v1/config").forStatusCode(200));
        rest.start();

        cassandra = new CassandraContainer("cassandra:5.0.8")
                .withNetwork(network)
                .withNetworkAliases("cassandra");
        cassandra.start();

        kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1")
                .withNetwork(network)
                .withNetworkAliases("kafka");
        kafka.start();

        flinkLibDir = Paths.get(System.getProperty("icestream.it.flinkLibDir"));
        sparkCompactionLibDir = Paths.get(System.getProperty("icestream.it.sparkCompactionLibDir"));
        flinkJobManager = withFlinkLibs(
                        new GenericContainer<>(FLINK_IMAGE)
                                .withNetwork(network)
                                .withNetworkAliases("flink-jm")
                                .withExposedPorts(8081)
                                .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                                .withCommand("jobmanager")
                                .waitingFor(Wait.forHttp("/").forStatusCode(200).forPort(8081)),
                        flinkLibDir);
        flinkJobManager.start();

        flinkTaskManager = withFlinkLibs(
                        new GenericContainer<>(FLINK_IMAGE)
                                .withNetwork(network)
                                .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                                .withCommand("taskmanager"),
                        flinkLibDir);
        flinkTaskManager.start();
    }

    private static GenericContainer<?> withFlinkLibs(GenericContainer<?> container, Path libDir) {
        try (Stream<Path> jars = Files.list(libDir)) {
            jars.filter(p -> p.toString().endsWith(".jar"))
                    .forEach(jar -> container.withCopyFileToContainer(
                            MountableFile.forHostPath(jar),
                            "/opt/flink/lib/" + jar.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed enumerating " + libDir, e);
        }
        return container;
    }

    @AfterAll
    static void stopStack() {
        if (sparkCompaction != null) {
            sparkCompaction.stop();
        }
        if (flinkTaskManager != null) {
            flinkTaskManager.stop();
        }
        if (flinkJobManager != null) {
            flinkJobManager.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (rest != null) {
            rest.stop();
        }
        if (cassandra != null) {
            cassandra.stop();
        }
        if (minio != null) {
            minio.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    void icestreamCommitsDoNotChangeVisibleRowState() throws Exception {
        try {
            runScenario();
        } catch (Throwable t) {
            dumpAllContainerLogs();
            throw t;
        }
    }

    private static void dumpAllContainerLogs() {
        System.out.println("\n==== REST catalog logs ====\n" + safeLogs(rest));
        System.out.println("\n==== Flink JM logs ====\n" + safeLogs(flinkJobManager));
        System.out.println("\n==== Flink TM logs ====\n" + safeLogs(flinkTaskManager));
        System.out.println("\n==== Spark compaction logs ====\n" + safeLogs(sparkCompaction));
    }

    private static String safeLogs(GenericContainer<?> c) {
        try {
            return c == null ? "<null container>" : c.getLogs();
        } catch (Exception e) {
            return "<failed to fetch logs: " + e.getMessage() + ">";
        }
    }

    private void runScenario() throws Exception {
        try (RESTCatalog catalog = newCatalog()) {
            catalog.createNamespace(Namespace.of("db"));
            Schema schemaWithIdentifier =
                    new Schema(SCHEMA.columns(), Set.of(SCHEMA.findField("id").fieldId()));
            catalog.createTable(TABLE_ID, schemaWithIdentifier, PartitionSpec.unpartitioned(), v2IcestreamProps());

            createKafkaTopic();
            submitFlinkJob();
            waitForFlinkJobRunning();
            startSparkCompaction();

            try (CqlSession cql = newCqlSession();
                    SparkSession spark = newSparkSession()) {
                ensureKeyspace(cql);

                KafkaJsonProducer producerRunnable = new KafkaJsonProducer(
                        kafka.getBootstrapServers(),
                        KAFKA_TOPIC,
                        100,
                        RUN_DURATION,
                        Duration.ofMillis(20),
                        42L);
                Thread producerThread = new Thread(producerRunnable, "icestream-it-producer");
                producerThread.setDaemon(true);
                producerThread.start();

                RecordingMetrics metrics = new RecordingMetrics();
                MasterLoop loop = newMasterLoop(spark, cql, catalog, metrics);
                Thread loopThread = new Thread(loop::run, "icestream-it-master");
                loopThread.setDaemon(true);
                loopThread.start();

                Thread.sleep(RUN_DURATION.toMillis());

                producerRunnable.stop();
                producerThread.join(Duration.ofSeconds(5).toMillis());
                loop.stop();
                loopThread.join(Duration.ofSeconds(10).toMillis());

                logIcestreamRunMetrics(metrics);
                assertThat(metrics.records())
                        .as("icestream master should have attempted at least one run")
                        .isNotEmpty();
            }

            assertThat(sparkCompaction.getLogs())
                    .as("spark compaction loop must have completed at least one rewrite")
                    .contains("compaction iteration succeeded");
            sparkCompaction.stop();

            waitForFlinkFinalCheckpoint(catalog);

            Table table = catalog.loadTable(TABLE_ID);
            assertThat(countSnapshots(table)).as("flink should have committed at least once").isGreaterThan(0);
            assertIcestreamSnapshotsAreNoOps(table);
        }
    }

    private static void logIcestreamRunMetrics(RecordingMetrics metrics) {
        List<RecordingMetrics.Record> records = metrics.records();
        long successes = records.stream().filter(RecordingMetrics.Record::isSuccess).count();
        long failures = records.size() - successes;
        LongSummaryStatistics overall = records.stream()
                .mapToLong(r -> r.elapsed().toMillis())
                .summaryStatistics();
        log.info(
                "Icestream run metrics: total={} successes={} failures={} latencyMs(min/avg/max)={}/{}/{}",
                records.size(),
                successes,
                failures,
                overall.getCount() == 0 ? 0 : overall.getMin(),
                overall.getCount() == 0 ? 0 : (long) overall.getAverage(),
                overall.getCount() == 0 ? 0 : overall.getMax());
        for (FileKind kind : FileKind.values()) {
            List<RecordingMetrics.Record> ofKind = records.stream()
                    .filter(r -> r.kind() == kind)
                    .toList();
            LongSummaryStatistics latency = ofKind.stream()
                    .mapToLong(r -> r.elapsed().toMillis())
                    .summaryStatistics();
            long kindSuccesses = ofKind.stream().filter(RecordingMetrics.Record::isSuccess).count();
            log.info(
                    "  kind={} count={} successes={} failures={} avgFileCount={} avgMs={}",
                    kind,
                    ofKind.size(),
                    kindSuccesses,
                    ofKind.size() - kindSuccesses,
                    ofKind.isEmpty()
                            ? 0
                            : (long) ofKind.stream().mapToInt(RecordingMetrics.Record::fileCount).average().orElse(0),
                    latency.getCount() == 0 ? 0 : (long) latency.getAverage());
        }
    }

    private static void startSparkCompaction() {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse("apache/spark:3.5.4-java17"))
                .withNetwork(network)
                .withNetworkAliases("spark-compaction")
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("compaction_loop.py"), "/scripts/compaction_loop.py")
                .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY)
                .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_KEY)
                .withEnv("AWS_REGION", "us-east-1")
                .withCommand(
                        "/opt/spark/bin/spark-submit",
                        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                        "--conf", "spark.sql.defaultCatalog=iceberg",
                        "--conf", "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
                        "--conf", "spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.rest.RESTCatalog",
                        "--conf", "spark.sql.catalog.iceberg.uri=http://rest:8181",
                        "--conf", "spark.sql.catalog.iceberg.warehouse=s3://" + S3_BUCKET + "/",
                        "--conf", "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
                        "--conf", "spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000",
                        "--conf", "spark.sql.catalog.iceberg.s3.path-style-access=true",
                        "--conf", "spark.sql.catalog.iceberg.s3.access-key-id=" + AWS_ACCESS_KEY,
                        "--conf", "spark.sql.catalog.iceberg.s3.secret-access-key=" + AWS_SECRET_KEY,
                        "--conf", "spark.sql.catalog.iceberg.client.region=us-east-1",
                        "--conf", "spark.sql.catalog.iceberg.cache-enabled=false",
                        "/scripts/compaction_loop.py")
                .withStartupTimeout(Duration.ofMinutes(5))
                .waitingFor(Wait.forLogMessage(".*compaction loop starting.*", 1))
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("spark-compaction"));
        sparkCompaction = withSparkCompactionLibs(container, sparkCompactionLibDir);
        sparkCompaction.start();
    }

    private static GenericContainer<?> withSparkCompactionLibs(GenericContainer<?> container, Path libDir) {
        try (Stream<Path> jars = Files.list(libDir)) {
            jars.filter(p -> p.toString().endsWith(".jar"))
                    .forEach(jar -> container.withCopyFileToContainer(
                            MountableFile.forHostPath(jar),
                            "/opt/spark/jars/" + jar.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed enumerating " + libDir, e);
        }
        return container;
    }

    private static void assertIcestreamSnapshotsAreNoOps(Table table) throws IOException {
        Map<Long, Snapshot> snapshotsById = StreamSupport.stream(table.snapshots().spliterator(), false)
                .collect(Collectors.toMap(Snapshot::snapshotId, s -> s));

        Set<Long> icestreamSnapshots = snapshotsById.values().stream()
                .filter(s -> "true".equals(s.summary().get(ConversionCommitter.ICESTREAM_CONVERTED_SUMMARY_KEY)))
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());

        long sparkRewriteSnapshots = snapshotsById.values().stream()
                .filter(s -> DataOperations.REPLACE.equals(s.operation()))
                .count();

        long icestreamSnapshotsAfterRewrite = icestreamSnapshots.stream()
                .map(snapshotsById::get)
                .filter(s -> s.parentId() != null
                        && DataOperations.REPLACE.equals(snapshotsById.get(s.parentId()).operation()))
                .count();

        log.info(
                "Snapshot stats: total={} icestream={} sparkRewrite={} icestreamAfterRewrite={}",
                snapshotsById.size(),
                icestreamSnapshots.size(),
                sparkRewriteSnapshots,
                icestreamSnapshotsAfterRewrite);

        assertThat(icestreamSnapshots)
                .as("icestream should have converted at least one eq-delete batch")
                .isNotEmpty();
        assertThat(sparkRewriteSnapshots)
                .as("spark rewrite_data_files should have produced replace snapshots")
                .isGreaterThan(0);
        assertThat(icestreamSnapshotsAfterRewrite)
                .as("icestream should have committed after at least one spark rewrite")
                .isGreaterThan(0);

        for (long snapshotId : icestreamSnapshots) {
            Snapshot snapshot = snapshotsById.get(snapshotId);
            Long parentId = snapshot.parentId();
            assertThat(parentId)
                    .as("icestream snapshot %d unexpectedly has no parent", snapshotId)
                    .isNotNull();
            Set<RowKey> child = readRowSet(table, snapshotId);
            Set<RowKey> parent = readRowSet(table, parentId);
            assertThat(child)
                    .as("icestream snapshot %d changed visible rows vs parent %d", snapshotId, parentId)
                    .isEqualTo(parent);
        }
    }

    private static Set<RowKey> readRowSet(Table table, long snapshotId) throws IOException {
        try (CloseableIterable<Record> iter = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .select("id", "name", "ts")
                .build()) {
            return StreamSupport.stream(iter.spliterator(), false)
                    .map(r -> new RowKey(
                            (Long) r.getField("id"), (String) r.getField("name"), (Long) r.getField("ts")))
                    .collect(Collectors.toSet());
        }
    }

    private record RowKey(long id, String name, long ts) {}

    private static void createKafkaTopic() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(java.util.List.of(new NewTopic(KAFKA_TOPIC, 1, (short) 1)))
                    .all()
                    .get();
        }
    }

    private void submitFlinkJob() {
        GenericContainer<?> sqlClient = withFlinkLibs(
                new GenericContainer<>(FLINK_IMAGE)
                        .withNetwork(network)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("flink-job.sql"), "/scripts/job.sql")
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withCommand("/opt/flink/bin/sql-client.sh", "-f", "/scripts/job.sql")
                        .withStartupCheckStrategy(
                                new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(2))),
                flinkLibDir);
        try {
            sqlClient.start();
        } finally {
            System.out.println("==== Flink SQL submitter logs ====");
            System.out.println(sqlClient.getLogs());
            System.out.println("==== end submitter logs ====");
            sqlClient.stop();
        }
    }

    private static void waitForFlinkJobRunning() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI url = URI.create(
                "http://" + flinkJobManager.getHost() + ":" + flinkJobManager.getMappedPort(8081) + "/jobs/overview");
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String last = "";
        while (System.nanoTime() < deadline) {
            HttpResponse<String> resp =
                    client.send(HttpRequest.newBuilder(url).build(), HttpResponse.BodyHandlers.ofString());
            last = resp.body();
            if (last.contains("\"state\":\"RUNNING\"")) {
                return;
            }
            Thread.sleep(500);
        }
        System.out.println("==== Flink JM /jobs/overview last response ====");
        System.out.println(last);
        System.out.println("==== Flink JM logs ====");
        System.out.println(flinkJobManager.getLogs());
        System.out.println("==== Flink TM logs ====");
        System.out.println(flinkTaskManager.getLogs());
        throw new AssertionError("Flink job never reached RUNNING within 30s");
    }

    private static void waitForFlinkFinalCheckpoint(RESTCatalog catalog) throws InterruptedException {
        long stable = 0;
        long last = -1;
        long deadline = System.nanoTime() + FINAL_CHECKPOINT_WAIT.toNanos();
        while (stable < 3 && System.nanoTime() < deadline) {
            Thread.sleep(2_000);
            long current = countSnapshots(catalog.loadTable(TABLE_ID));
            if (current == last) {
                stable++;
            } else {
                stable = 0;
                last = current;
            }
        }
    }

    private static long countSnapshots(Table table) {
        return StreamSupport.stream(table.snapshots().spliterator(), false).count();
    }

    private MasterLoop newMasterLoop(
            SparkSession spark, CqlSession cql, RESTCatalog catalog, IcestreamMetrics metrics) {
        CassandraIndex cassandraIndex = new CassandraIndex(cql, CASSANDRA_KEYSPACE);
        TableProcessor processor = new TableProcessor(
                new SnapshotPlanner(),
                new DataFileIndexer(spark, cassandraIndex),
                new DeleteFileCreator(spark, cassandraIndex),
                cassandraIndex,
                metrics);
        return new MasterLoop(catalog, processor, ICESTREAM_POLL, ICESTREAM_IDLE_BACKOFF, 2);
    }

    private static final class RecordingMetrics implements IcestreamMetrics {

        private final List<Record> records = new CopyOnWriteArrayList<>();

        @Override
        public void recordRunSuccess(TableIdentifier table, FileKind kind, int fileCount, Duration elapsed) {
            records.add(new Record(table, kind, fileCount, elapsed, null));
        }

        @Override
        public void recordRunFailure(
                TableIdentifier table, FileKind kind, int fileCount, Duration elapsed, Throwable cause) {
            records.add(new Record(table, kind, fileCount, elapsed, cause));
        }

        List<Record> records() {
            return List.copyOf(records);
        }

        record Record(TableIdentifier table, FileKind kind, int fileCount, Duration elapsed, Throwable failure) {

            boolean isSuccess() {
                return failure == null;
            }
        }
    }

    private static SparkSession newSparkSession() {
        return SparkSession.builder()
                .master("local[2]")
                .appName("icestream-it")
                .config(
                        "spark.cassandra.connection.host",
                        cassandra.getHost())
                .config(
                        "spark.cassandra.connection.port",
                        String.valueOf(cassandra.getMappedPort(9042)))
                .config("spark.cassandra.connection.localDC", cassandra.getLocalDatacenter())
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    private static CqlSession newCqlSession() {
        return CqlSession.builder()
                .addContactPoint(cassandra.getContactPoint())
                .withLocalDatacenter(cassandra.getLocalDatacenter())
                .build();
    }

    private static void ensureKeyspace(CqlSession cql) {
        cql.execute("CREATE KEYSPACE IF NOT EXISTS " + CASSANDRA_KEYSPACE
                + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
    }

    private static RESTCatalog newCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, "http://" + rest.getHost() + ":" + rest.getMappedPort(8181));
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + S3_BUCKET + "/");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", "http://" + minio.getHost() + ":" + minio.getMappedPort(9000));
        props.put("s3.path-style-access", "true");
        props.put("s3.access-key-id", AWS_ACCESS_KEY);
        props.put("s3.secret-access-key", AWS_SECRET_KEY);
        props.put("client.region", "us-east-1");
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("icestream-it", props);
        return catalog;
    }

    private static Map<String, String> v2IcestreamProps() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "2");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.CASSANDRA_BUCKETS, "4");
        return props;
    }
}
