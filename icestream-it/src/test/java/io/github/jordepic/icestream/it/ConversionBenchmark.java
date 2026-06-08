package io.github.jordepic.icestream.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.flinkpaimon.converter.FlinkDeleteFileCreator;
import io.github.jordepic.icestream.flinkpaimon.converter.StreamingFlinkDeleteFileCreator;
import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.flinkpaimon.indexer.FlinkDataFileIndexer;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import org.apache.paimon.schema.SchemaChange;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import jdk.jfr.Recording;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone, fully in-process performance benchmark for the Flink+Paimon equality-delete →
 * positional-delete conversion path. No Docker / testcontainers: an iceberg {@link HadoopCatalog},
 * a Paimon FilesystemCatalog, and a Flink {@link FlinkContext#local} MiniCluster all run in this
 * JVM against local temp dirs — the same wiring the unit tests use, scaled up.
 *
 * <p>It deliberately bypasses streaming ingestion: the data files and the equality-delete file are
 * pre-written directly via the iceberg API so the workload is deterministic (exact row count,
 * exact delete ratio) and the timings measure only icestream's index and convert work, not Kafka
 * or Flink-upsert jitter.
 *
 * <p>Phases:
 * <ol>
 *   <li><b>Setup</b> (untimed): create an unpartitioned v2 table, write {@code rows} rows across
 *       {@code files} parquet data files, commit them.
 *   <li><b>Index</b> (timed): {@link FlinkDataFileIndexer} reads the data files and populates the
 *       Paimon secondary index keyed on the primary key.
 *   <li><b>Convert — lookup</b> (timed): {@link FlinkDeleteFileCreator} converts an eq-delete
 *       covering {@code 1/ratio} of the rows, using Paimon's indexed temporal lookup join.
 *   <li><b>Convert — control</b> (timed): the identical conversion with the lookup hint dropped,
 *       forcing a full-scan regular join — isolates the speedup the indexed lookup buys.
 * </ol>
 *
 * <p>Not run by the default build (surefire only picks up {@code *IT.java}). Run on demand:
 * <pre>{@code
 *   mvn -pl icestream-it -am test-compile
 *   mvn -pl icestream-it test -Pbenchmark \
 *       -Dbench.rows=25000000 -Dbench.files=48 -Dbench.ratio=100 \
 *       -Dbench.buckets=8 -Dbench.slots=4 -Dbench.convertRepeats=2
 * }</pre>
 * Defaults (5M rows, ~a few hundred MB) run in a couple of minutes; bump {@code bench.rows} toward
 * 25M for a ~1 GB table. The reported "parquet bytes" tells you where you actually landed.
 */
@Tag("benchmark")
class ConversionBenchmark {

    private static final Logger log = LoggerFactory.getLogger(ConversionBenchmark.class);

    private static final Schema SCHEMA = new Schema(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.required(2, "name", Types.StringType.get()),
            NestedField.required(3, "ts", Types.LongType.get()));

    // --- configuration (system properties with defaults) ---------------------------------------
    private static final long ROWS = longProp("bench.rows", 5_000_000L);
    private static final int FILES = intProp("bench.files", 16);
    private static final int DELETE_RATIO = intProp("bench.ratio", 100); // delete 1/ratio of rows
    private static final long DELETES = longProp("bench.deletes", 0); // absolute count; overrides ratio when > 0
    private static final int BUCKETS = intProp("bench.buckets", 8);
    private static final int SLOTS = intProp("bench.slots", 4);
    private static final int CONVERT_REPEATS = intProp("bench.convertRepeats", 2);
    private static final int NAME_WIDTH = intProp("bench.nameWidth", 24);
    private static final int FORMAT_VERSION = intProp("bench.formatVersion", 2);
    private static final long SEED = longProp("bench.seed", 42L);
    // When set, data + index persist here so reruns warm-start straight into the sweep.
    private static final String WAREHOUSE_DIR = System.getProperty("bench.warehouseDir");
    // Which join strategies to time: "lookup", "control", or "both" (default).
    private static final String STRATEGIES = System.getProperty("bench.strategies", "both").toLowerCase();
    // When set, a JFR recording (profile settings) is captured around just the convert sweep and
    // written to this path — so it contains only the join work, not data-gen or indexing.
    private static final String JFR_FILE = System.getProperty("bench.jfrFile");
    // Streaming converter mode: run N conversions through one long-lived warm job and time each
    // (run 1 = cold SST build, runs 2+ = warm). Proves the warm-cache win.
    private static final boolean STREAMING = boolProp("bench.streaming", false);
    private static final int STREAM_RUNS = intProp("bench.streamRuns", 5);
    private static final long CHECKPOINT_MS = longProp("bench.checkpointMs", 500L);
    // Per-conversion result timeout for the streaming converter. Bump it for very large indexes where
    // a big-D conversion can legitimately take minutes.
    private static final long CONV_TIMEOUT_MS = longProp("bench.conversionTimeoutMs", 600_000L);

    // Run the batch control in LOOKUP mode — a COLD fresh-batch-job-per-conversion that downloads the
    // pre-built remote lookup files (via the LocalTableQuery monkey patch) instead of the default
    // regular full-scan join. This is the apples-to-apples test of "do we even need the warm standing
    // job, given the lookup files are built once at index time?" Compare its column to warm lookup.
    private static final boolean BATCH_LOOKUP = boolProp("bench.batchLookup", false);

    // Batch-only D-sweep: the FINAL design. Every conversion is a cold fresh Flink BATCH job. The
    // primary arm is the temporal lookup join (downloads pre-persisted remote lookup SSTs via the
    // LocalTableQuery monkey patch) timed across the whole D-sweep; the control is the non-temporal
    // full-scan hash join over the same index, timed only at the D's in bench.batchDValues.
    private static final boolean BATCH_ONLY = boolProp("bench.batchOnly", false);

    // Run the standing job on a real remote Flink session cluster (FlinkContext.remote) instead of the
    // in-JVM MiniCluster, driving conversions over the HTTP channel. Requires a shared warehouse
    // (bench.warehouseDir, reachable by the TMs) and the shaded job jar (bench.flinkJars). This is the
    // genuine distributed run: reads + lookup + writes spread across the cluster's TaskManagers.
    private static final boolean REMOTE_CLUSTER = boolProp("bench.remoteCluster", false);
    private static final String JM_HOST = System.getProperty("bench.jmHost", "localhost");
    private static final int JM_PORT = intProp("bench.jmPort", 8081);
    private static final String FLINK_JARS = System.getProperty("bench.flinkJars", "");
    // Override the lookup-file in-memory page cache (read-side) without reindexing — e.g. "0 b"
    // (off) or "2 gb". Applied via ALTER TABLE before the converter starts.
    private static final String LOOKUP_MEM_CACHE = System.getProperty("bench.lookupMemCache");
    // Crossover chart: warm a single index once, then sweep these D values, timing warm-lookup vs
    // isolated batch-join at each. e.g. -Dbench.dSweep=1,10,100,1000,10000,100000,1000000
    private static final String D_SWEEP = System.getProperty("bench.dSweep");
    // D values for which to ALSO run the (expensive, O(N)) batch-join control. Empty = all (default).
    // The batch baseline is flat in D, so a couple of points (e.g. "1,1000000") pin it cheaply while
    // the lookup arm still sweeps every D. Lookup correctness is checked against the invariant
    // positions==D regardless, so flakes are still caught where batch is skipped.
    private static final String BATCH_D = System.getProperty("bench.batchDValues", "");

    @TempDir
    Path icebergWarehouse;

    @TempDir
    Path paimonWarehouse;

    private HadoopCatalog icebergCatalog;
    private PaimonIndex paimonIndex;
    private FlinkContext flink;
    private String paimonWarehouseUri;
    private final AtomicInteger pathCounter = new AtomicInteger();

    @BeforeEach
    void setUp() {
        // When bench.warehouseDir is set, use a persistent layout on disk so the (expensive) data
        // generation + indexing happens once and later runs warm-start straight into the sweep.
        // Otherwise fall back to the auto-cleaned @TempDir layout.
        String icebergPath;
        String paimonUri;
        if (WAREHOUSE_DIR != null && !WAREHOUSE_DIR.isBlank()) {
            icebergPath = Paths.get(WAREHOUSE_DIR, "iceberg").toString();
            paimonUri = Paths.get(WAREHOUSE_DIR, "paimon").toUri().toString();
        } else {
            icebergPath = icebergWarehouse.toString();
            paimonUri = paimonWarehouse.toUri().toString();
        }
        icebergCatalog = new HadoopCatalog(new Configuration(), icebergPath);
        paimonWarehouseUri = paimonUri;
        paimonIndex = PaimonIndex.create(paimonUri, "icestream", Map.of());
        if (REMOTE_CLUSTER) {
            String[] jars = FLINK_JARS.isBlank() ? new String[0] : FLINK_JARS.split(",");
            flink = FlinkContext.remote(JM_HOST, JM_PORT, SLOTS, jars);
        } else {
            flink = FlinkContext.local(SLOTS);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flink != null) {
            flink.close();
        }
        if (paimonIndex != null) {
            try {
                paimonIndex.catalog().close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        if (icebergCatalog != null) {
            icebergCatalog.close();
        }
    }

    @Test
    void benchmarkConversion() throws Exception {
        TableIdentifier id = TableIdentifier.of("db", "events");

        long rowsPerFile = (ROWS + FILES - 1) / FILES;
        Table table;
        List<DataFile> dataFiles;

        // --- Phase 0: ensure the data exists. Reuse the persisted iceberg table when present
        // (warehouseDir set + table already written); otherwise generate + commit it. The data is
        // the expensive part to build, so reusing it lets index-setting experiments skip ~minutes
        // of generation. ---
        boolean reuseData = WAREHOUSE_DIR != null && icebergCatalog.tableExists(id);
        if (reuseData) {
            table = icebergCatalog.loadTable(id);
            dataFiles = currentDataFiles(table);
            log.info("Reusing persisted iceberg table: {} data files", dataFiles.size());
        } else {
            try {
                icebergCatalog.createNamespace(Namespace.of("db"));
            } catch (AlreadyExistsException ignored) {
                // namespace left over from a prior persistent run
            }
            Schema schemaWithIdentifier = new Schema(SCHEMA.columns(), Set.of(SCHEMA.findField("id").fieldId()));
            table = icebergCatalog.createTable(id, schemaWithIdentifier, PartitionSpec.unpartitioned(), tableProps());
            long totalRowsToWrite = rowsPerFile * FILES;
            log.info("Writing {} rows across {} files ({} rows/file)...", totalRowsToWrite, FILES, rowsPerFile);
            dataFiles = new ArrayList<>(FILES);
            long writeStart = System.nanoTime();
            for (int f = 0; f < FILES; f++) {
                dataFiles.add(writeDataFile(table, f * rowsPerFile, rowsPerFile));
            }
            AppendFiles append = table.newAppend();
            dataFiles.forEach(append::appendFile);
            append.commit();
            table.refresh();
            log.info("Wrote data in {}", secs(System.nanoTime() - writeStart));
        }

        Snapshot snap = table.currentSnapshot();
        long totalRows = Long.parseLong(snap.summary().getOrDefault("total-records", "0"));
        long parquetBytes = Long.parseLong(snap.summary().getOrDefault("total-files-size", "0"));
        int fileCount = dataFiles.size();
        long dataSeq = snap.sequenceNumber();
        IcestreamTableConfig config = IcestreamTableConfig.from(table);

        // --- Phase 1: ensure the index. Reuse a persisted index unless it is missing or
        // bench.reindex=true (e.g. you changed the index format/settings and want to rebuild it
        // over the same data). Rebuilding drops the old index table first. ---
        boolean indexExists = paimonIndexExists(id);
        boolean buildIndex = !indexExists || boolProp("bench.reindex", false);
        long indexNanos;
        if (buildIndex) {
            if (indexExists) {
                log.info("Dropping existing index to rebuild (bench.reindex)...");
                paimonIndex.catalog().dropTable(paimonIndex.identifierFor(id), true);
            }
            paimonIndex.initializeForTable(id, config);
            DataFileRun dataRun = new DataFileRun(dataSeq, dataFiles, Map.of());
            long indexStart = System.nanoTime();
            new FlinkDataFileIndexer(flink, paimonIndex).index(id, table, dataRun, config);
            indexNanos = System.nanoTime() - indexStart;
            table.refresh();
        } else {
            indexNanos = 0; // reused
            log.info("Reusing persisted index");
        }

        // Read-side cache override: change the lookup-file in-memory page cache size on the existing
        // index (no reindex — it's a read-side option) before the converter loads the table.
        if (LOOKUP_MEM_CACHE != null && !LOOKUP_MEM_CACHE.isBlank()) {
            paimonIndex.catalog().alterTable(
                    paimonIndex.identifierFor(id),
                    SchemaChange.setOption("lookup.cache-max-memory-size", LOOKUP_MEM_CACHE),
                    false);
            table.refresh();
            log.info("Set lookup.cache-max-memory-size = {} via ALTER (read-side, no reindex)", LOOKUP_MEM_CACHE);
        }

        long deleteCount = DELETES > 0 ? Math.min(DELETES, totalRows) : totalRows / DELETE_RATIO;

        // Batch-only crossover: cold batch lookup (download) vs full-scan hash-join control.
        if (BATCH_ONLY && D_SWEEP != null && !D_SWEEP.isBlank()) {
            runBatchCrossoverBenchmark(id, table, config, totalRows);
            return;
        }
        // D-sweep crossover: in-process (STREAMING) or on the remote cluster (REMOTE_CLUSTER).
        if ((STREAMING || REMOTE_CLUSTER) && D_SWEEP != null && !D_SWEEP.isBlank()) {
            runCrossoverBenchmark(id, table, config, totalRows);
            return;
        }
        if (REMOTE_CLUSTER) {
            runClusterBenchmark(id, table, config, totalRows, parquetBytes, fileCount, deleteCount);
            return;
        }
        if (STREAMING) {
            runStreamingBenchmark(id, table, config, totalRows, parquetBytes, fileCount, indexNanos, deleteCount);
            return;
        }

        // --- Phase 2: sweep D, converting lookup vs control at each point (timed) --------------
        // The index is built once above; each eq-delete is written to disk but NOT committed, and
        // create() does not mutate the table, so every D runs against the identical post-index
        // state and is directly comparable. This avoids re-indexing per D.
        boolean runLookup = STRATEGIES.contains("lookup") || STRATEGIES.contains("both");
        boolean runControl = STRATEGIES.contains("control") || STRATEGIES.contains("both");

        List<Long> sweep = sweepValues(deleteCount, totalRows);
        List<SweepRow> rows = new ArrayList<>(sweep.size());

        Recording recording = startJfr();
        try {
            for (long d : sweep) {
                log.info("Converting eq-delete with {} keys...", d);
                DeleteFile eqDelete = writeEqDelete(table, totalRows, d);
                EqualityDeleteFileRun eqRun = new EqualityDeleteFileRun(dataSeq + 1, List.of(eqDelete));

                long lookupBest = -1;
                long controlBest = -1;
                long lookupPositions = -1;
                long controlPositions = -1;
                if (runLookup) {
                    long[] nanos = new long[CONVERT_REPEATS];
                    for (int r = 0; r < CONVERT_REPEATS; r++) {
                        long t0 = System.nanoTime();
                        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex, true)
                                .create(id, table, eqRun, config);
                        nanos[r] = System.nanoTime() - t0;
                        lookupPositions = totalPositions(plan);
                        log.info("D={} lookup run{}: {}", d, r + 1, secs(nanos[r]));
                    }
                    lookupBest = min(nanos);
                }
                if (runControl) {
                    long[] nanos = new long[CONVERT_REPEATS];
                    for (int r = 0; r < CONVERT_REPEATS; r++) {
                        long t0 = System.nanoTime();
                        CommitPlan plan = new FlinkDeleteFileCreator(flink, paimonIndex, false)
                                .create(id, table, eqRun, config);
                        nanos[r] = System.nanoTime() - t0;
                        controlPositions = totalPositions(plan);
                        log.info("D={} control run{}: {}", d, r + 1, secs(nanos[r]));
                    }
                    controlBest = min(nanos);
                }
                rows.add(new SweepRow(d, lookupBest, controlBest, lookupPositions, controlPositions));

                // Sanity: when both strategies run they must convert the same rows.
                if (runLookup && runControl) {
                    assertThat(lookupPositions)
                            .as("lookup vs control must emit the same positions at D=%d", d)
                            .isEqualTo(controlPositions);
                }
            }
        } finally {
            stopJfr(recording);
        }

        report(totalRows, parquetBytes, fileCount, indexNanos, rows);
    }

    private static Recording startJfr() throws Exception {
        if (JFR_FILE == null || JFR_FILE.isBlank()) {
            return null;
        }
        Recording recording = new Recording(jdk.jfr.Configuration.getConfiguration("profile"));
        recording.start();
        System.out.println("JFR recording started -> " + JFR_FILE);
        return recording;
    }

    private static void stopJfr(Recording recording) throws IOException {
        if (recording == null) {
            return;
        }
        recording.stop();
        recording.dump(Paths.get(JFR_FILE));
        recording.close();
        System.out.println("JFR recording written -> " + JFR_FILE);
    }

    private record SweepRow(long deletes, long lookupNanos, long controlNanos, long lookupPos, long controlPos) {}

    /**
     * Crossover chart: warm a single index once, then sweep {@code bench.dSweep} D values, timing the
     * warm lookup join vs the isolated batch regular join at each. Shows where the point-lookup
     * (O(D·log N)) loses to the full-scan hash join (O(N)).
     */
    private void runCrossoverBenchmark(
            TableIdentifier id, Table table, IcestreamTableConfig config, long totalRows) throws Exception {
        List<Long> dValues = java.util.Arrays.stream(D_SWEEP.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).map(d -> Math.min(d, totalRows)).distinct().sorted()
                .collect(Collectors.toList());
        record CrossRow(long d, long lookupNanos, long controlNanos, long lookupPos, long controlPos) {}
        List<CrossRow> rows = new ArrayList<>();

        // Batch-join control stays on a local MiniCluster (same baseline as the prior in-process
        // crossover charts, so the numbers line up). When REMOTE_CLUSTER, the warm-lookup arm runs on
        // the session cluster via the HTTP channel; otherwise it's the in-process converter.
        try {
            log.info("LocalTableQuery loaded from: {}",
                    Class.forName("org.apache.paimon.table.query.LocalTableQuery")
                            .getProtectionDomain().getCodeSource().getLocation());
        } catch (Throwable t) {
            log.warn("could not resolve LocalTableQuery source", t);
        }
        FlinkContext controlFlink = FlinkContext.local(SLOTS);
        PaimonIndex controlIndex = PaimonIndex.create(paimonWarehouseUri, "icestream", Map.of());
        try (StreamingFlinkDeleteFileCreator converter =
                new StreamingFlinkDeleteFileCreator(flink, paimonIndex, CHECKPOINT_MS, CONV_TIMEOUT_MS)) {
            // Settle the standing job with a cheap probe conversion so the lookup join is live for the sweep.
            long settleDeadline = System.nanoTime() + 180_000_000_000L;
            while (System.nanoTime() < settleDeadline) {
                if (timeConversion(converter, id, table, config, totalRowsNow(table), 1000)[1] >= 1000) {
                    break;
                }
                Thread.sleep(3000);
            }
            long idSpace = totalRowsNow(table);

            java.util.Set<Long> batchDs = BATCH_D.isBlank() ? java.util.Set.of()
                    : java.util.Arrays.stream(BATCH_D.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                            .map(Long::parseLong).collect(Collectors.toSet());
            for (long d : dValues) {
                DeleteFile eqDelete = writeEqDelete(table, idSpace, d);
                EqualityDeleteFileRun run =
                        new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete));

                long t1 = System.nanoTime();
                CommitPlan lookupPlan = converter.create(id, table, run, config);
                long lookupNanos = System.nanoTime() - t1;
                long lookupPos = totalPositions(lookupPlan);

                // Run the O(N) batch-join control only for selected D (it's flat in D); -1 = skipped.
                long controlNanos = -1;
                long controlPos = -1;
                if (batchDs.isEmpty() || batchDs.contains(d)) {
                    long t2 = System.nanoTime();
                    CommitPlan controlPlan = new FlinkDeleteFileCreator(controlFlink, controlIndex, BATCH_LOOKUP)
                            .create(id, table, run, config);
                    controlNanos = System.nanoTime() - t2;
                    controlPos = totalPositions(controlPlan);
                }
                // Correctness invariant: D distinct existing keys → exactly D positions. A flake (the
                // boundary/transport returning an empty plan) shows up as lookupPos != D — caught here
                // whether or not the batch control ran. The soft check never aborts the sweep.
                if (lookupPos != d) {
                    log.warn("D={}: lookup positions {} != expected {} (flaked) — marking row", d, lookupPos, d);
                }
                rows.add(new CrossRow(d, lookupNanos, controlNanos, lookupPos, controlPos));
                log.info("D={} lookup={}s batch-join={}s", d, lookupNanos / 1e9,
                        controlNanos < 0 ? "(skipped)" : String.valueOf(controlNanos / 1e9));
            }
        } finally {
            controlFlink.close();
            try {
                controlIndex.catalog().close();
            } catch (Exception ignored) {
                // best-effort
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n=============== Icestream Warm-Lookup vs Batch-Join — D crossover ===============\n");
        String lookupWhere = REMOTE_CLUSTER
                ? String.format("warm lookup on Flink session cluster @ %s:%d (distributed, bucket-keyed)", JM_HOST, JM_PORT)
                : "warm lookup in-process (local MiniCluster)";
        String batchLabel = BATCH_LOOKUP ? "cold batch-lookup (remote-file download)" : "batch-join full-scan";
        sb.append(String.format("Index: %,d rows, %d buckets, slots %d (warmed once); %s; control = %s (local, fresh job/conversion)%n",
                totalRows, BUCKETS, SLOTS, lookupWhere, batchLabel));
        sb.append("--------------------------------------------------------------------------------\n");
        sb.append(String.format("%14s %10s %14s %14s %9s %10s%n",
                "D", "D/N", "warm lookup", BATCH_LOOKUP ? "cold batch-lk" : "batch-join", "speedup", "winner"));
        sb.append("--------------------------------------------------------------------------------\n");
        for (CrossRow row : rows) {
            double lk = row.lookupNanos() / 1e9;
            boolean flaked = row.lookupPos() != row.d();
            boolean haveBatch = row.controlNanos() >= 0;
            String batchStr = haveBatch ? String.format("%.2f s", row.controlNanos() / 1e9) : "-";
            String speedupStr = haveBatch ? String.format("%.2fx", (row.controlNanos() / 1e9) / lk) : "-";
            String winner = flaked
                    ? String.format("FLAKED (%d pos, expected %d)", row.lookupPos(), row.d())
                    : (haveBatch ? (row.controlNanos() >= row.lookupNanos() ? "LOOKUP" : "batch") : "LOOKUP");
            sb.append(String.format("%,14d %9.4f%% %12.2f s %14s %9s %s%n",
                    row.d(), 100.0 * row.d() / totalRows, lk, batchStr, speedupStr, winner));
        }
        sb.append("================================================================================");
        System.out.println(sb);
    }

    /**
     * Batch-only D-sweep (the final design): every conversion is a cold, fresh Flink batch job.
     * <ul>
     *   <li><b>lookup arm</b> (all D): {@code FlinkDeleteFileCreator(useLookupJoin=true)} — the temporal
     *       join the planner compiles into Paimon's {@code FileStoreLookupFunction}, which (via the
     *       {@code LocalTableQuery} monkey patch) downloads the pre-persisted remote lookup SSTs.
     *   <li><b>control arm</b> ({@code bench.batchDValues}): {@code FlinkDeleteFileCreator(false)} — the
     *       non-temporal full-scan hash join over the same index, the O(N) baseline.
     * </ul>
     * Each {@code create()} spins a fresh batch job, so the lookup state is cold every time (only the
     * shared MiniCluster's local lookup-file disk cache may carry across, as a real TM's would).
     */
    private void runBatchCrossoverBenchmark(
            TableIdentifier id, Table table, IcestreamTableConfig config, long totalRows) throws Exception {
        List<Long> dValues = java.util.Arrays.stream(D_SWEEP.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Long::parseLong).map(d -> Math.min(d, totalRows)).distinct().sorted()
                .collect(Collectors.toList());
        java.util.Set<Long> controlDs = BATCH_D.isBlank() ? java.util.Set.of()
                : java.util.Arrays.stream(BATCH_D.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .map(Long::parseLong).collect(Collectors.toSet());
        try {
            log.info("LocalTableQuery loaded from: {}",
                    Class.forName("org.apache.paimon.table.query.LocalTableQuery")
                            .getProtectionDomain().getCodeSource().getLocation());
        } catch (Throwable t) {
            log.warn("could not resolve LocalTableQuery source", t);
        }

        record CrossRow(long d, long lookupNanos, long controlNanos, long lookupPos, long controlPos) {}
        List<CrossRow> rows = new ArrayList<>();
        long idSpace = totalRowsNow(table);
        Recording recording = startJfr();
        try {
        for (long d : dValues) {
            DeleteFile eqDelete = writeEqDelete(table, idSpace, d);
            EqualityDeleteFileRun run =
                    new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete));

            long t1 = System.nanoTime();
            CommitPlan lookupPlan = new FlinkDeleteFileCreator(flink, paimonIndex, true).create(id, table, run, config);
            long lookupNanos = System.nanoTime() - t1;
            long lookupPos = totalPositions(lookupPlan);

            long controlNanos = -1;
            long controlPos = -1;
            if (controlDs.contains(d)) {
                long t2 = System.nanoTime();
                CommitPlan controlPlan = new FlinkDeleteFileCreator(flink, paimonIndex, false).create(id, table, run, config);
                controlNanos = System.nanoTime() - t2;
                controlPos = totalPositions(controlPlan);
            }
            if (lookupPos != d) {
                log.warn("D={}: lookup positions {} != expected {}", d, lookupPos, d);
            }
            rows.add(new CrossRow(d, lookupNanos, controlNanos, lookupPos, controlPos));
            log.info("D={} batch-lookup={}s hash-join={}s", d, lookupNanos / 1e9,
                    controlNanos < 0 ? "(skipped)" : String.valueOf(controlNanos / 1e9));
        }
        } finally {
            stopJfr(recording);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n========== Icestream Batch Lookup (remote-file download) vs Full-Scan Hash Join ==========\n");
        sb.append(String.format(
                "Index: %,d rows, %d buckets, slots %d; lookup mem-cache %s; both = cold fresh Flink batch job/conversion%n",
                totalRows, BUCKETS, SLOTS, LOOKUP_MEM_CACHE == null ? "(default)" : LOOKUP_MEM_CACHE));
        sb.append("-----------------------------------------------------------------------------------------\n");
        sb.append(String.format("%14s %10s %18s %16s %9s%n",
                "D", "D/N", "batch-lookup (dl)", "hash-join", "speedup"));
        sb.append("-----------------------------------------------------------------------------------------\n");
        for (CrossRow row : rows) {
            double lk = row.lookupNanos() / 1e9;
            boolean haveCtl = row.controlNanos() >= 0;
            String ctlStr = haveCtl ? String.format("%.2f s", row.controlNanos() / 1e9) : "-";
            String speedupStr = haveCtl ? String.format("%.2fx", (row.controlNanos() / 1e9) / lk) : "-";
            sb.append(String.format("%,14d %9.4f%% %16.2f s %16s %9s%n",
                    row.d(), 100.0 * row.d() / totalRows, lk, ctlStr, speedupStr));
        }
        sb.append("=========================================================================================");
        System.out.println(sb);
    }

    /** Returns {convertNanos, positions}. */
    private long[] timeConversion(
            StreamingFlinkDeleteFileCreator converter,
            TableIdentifier id,
            Table table,
            IcestreamTableConfig config,
            long idSpace,
            long deleteCount)
            throws Exception {
        DeleteFile eqDelete = writeEqDelete(table, idSpace, deleteCount);
        EqualityDeleteFileRun run =
                new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete));
        long t0 = System.nanoTime();
        CommitPlan plan = converter.create(id, table, run, config);
        long elapsed = System.nanoTime() - t0;
        return new long[] {elapsed, totalPositions(plan)};
    }

    private static long totalRowsNow(Table table) {
        return Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-records", "0"));
    }

    /**
     * Streaming converter benchmark: one long-lived warm job, {@code STREAM_RUNS} conversions at a
     * single D. Run 1 pays the cold SST build (~the batch cost); runs 2+ hit the warm lookup cache.
     */
    private void runStreamingBenchmark(
            TableIdentifier id,
            Table table,
            IcestreamTableConfig config,
            long totalRows,
            long parquetBytes,
            int fileCount,
            long indexNanos,
            long deleteCount)
            throws Exception {
        DeleteFile eqDelete = writeEqDelete(table, totalRows, deleteCount);
        EqualityDeleteFileRun run =
                new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete));

        long[] latency = new long[STREAM_RUNS];
        long positions = 0;
        try (StreamingFlinkDeleteFileCreator converter =
                new StreamingFlinkDeleteFileCreator(flink, paimonIndex, CHECKPOINT_MS, CONV_TIMEOUT_MS)) {
            for (int r = 0; r < STREAM_RUNS; r++) {
                long t0 = System.nanoTime();
                CommitPlan plan = converter.create(id, table, run, config);
                latency[r] = System.nanoTime() - t0;
                positions = totalPositions(plan);
                log.info("streaming run {}: {}  ({} positions)", r + 1, secs(latency[r]), positions);
            }
        }

        long cold = latency[0];
        long warmBest = Long.MAX_VALUE;
        for (int r = 1; r < STREAM_RUNS; r++) {
            warmBest = Math.min(warmBest, latency[r]);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("\n===================== Icestream Streaming Converter Benchmark =====================\n");
        sb.append(String.format("Data   : %,d rows across %d files, %s parquet%n", totalRows, fileCount, humanBytes(parquetBytes)));
        sb.append(String.format("Index  : %d buckets, format-version %d, Flink slots %d, checkpoint %d ms%n",
                BUCKETS, FORMAT_VERSION, SLOTS, CHECKPOINT_MS));
        sb.append(String.format("Eq-delete : %,d keys; %d conversions through one warm job%n", deleteCount, STREAM_RUNS));
        sb.append("-----------------------------------------------------------------------------------\n");
        for (int r = 0; r < STREAM_RUNS; r++) {
            String label = r == 0 ? "(cold — builds SSTs)" : "(warm)";
            sb.append(String.format("  conversion %2d : %8.2f s   %s%n", r + 1, latency[r] / 1e9, label));
        }
        sb.append("-----------------------------------------------------------------------------------\n");
        sb.append(String.format("cold (run 1)      : %.2f s%n", cold / 1e9));
        sb.append(String.format("warm (best 2..%d)  : %.2f s%n", STREAM_RUNS, warmBest / 1e9));
        sb.append(String.format("warm speedup      : %.1fx%n", (double) cold / warmBest));
        sb.append(String.format("positions emitted : %,d (consistent across runs)%n", positions));
        sb.append("===================================================================================");
        System.out.println(sb);

        assertThat(positions).as("streaming conversion should match indexed rows").isPositive();
    }

    /**
     * Distributed run on a real Flink session cluster: the index (phase 1, above) and these warm
     * conversions both submit to the cluster via {@link FlinkContext#remote}; the standing job's
     * reads/lookup/writes spread across the TaskManagers, and the operators reach this JVM's channel
     * over HTTP. Reports warm-conversion latency to compare against the in-process/loopback numbers.
     */
    private void runClusterBenchmark(
            TableIdentifier id,
            Table table,
            IcestreamTableConfig config,
            long totalRows,
            long parquetBytes,
            int fileCount,
            long deleteCount)
            throws Exception {
        DeleteFile eqDelete = writeEqDelete(table, totalRows, deleteCount);
        EqualityDeleteFileRun run =
                new EqualityDeleteFileRun(table.currentSnapshot().sequenceNumber() + 1, List.of(eqDelete));

        long[] latency = timeWarmConversions(id, table, config, run);
        long warm = warmBest(latency);

        StringBuilder sb = new StringBuilder();
        sb.append("\n=================== Icestream Distributed Cluster Conversion Benchmark ===================\n");
        sb.append(String.format("Cluster : Flink session @ %s:%d, parallelism %d%n", JM_HOST, JM_PORT, SLOTS));
        sb.append(String.format("Data    : %,d rows across %d files, %s parquet%n", totalRows, fileCount, humanBytes(parquetBytes)));
        sb.append(String.format("Eq-delete : %,d keys; %d conversions through one warm job on the cluster%n",
                deleteCount, STREAM_RUNS));
        sb.append("------------------------------------------------------------------------------------------\n");
        for (int r = 0; r < latency.length; r++) {
            String label = r == 0 ? "cold" : "warm";
            sb.append(String.format("  conversion %2d : %8.2f s   (%s)%n", r + 1, latency[r] / 1e9, label));
        }
        sb.append("------------------------------------------------------------------------------------------\n");
        sb.append(String.format("warm best : %.2f s   (reads + lookup + writes distributed across TaskManagers)%n", warm / 1e9));
        sb.append("==========================================================================================");
        System.out.println(sb);

        assertThat(warm).as("distributed cluster conversion should complete").isPositive();
    }

    /** Run STREAM_RUNS conversions through one warm job; returns per-run latencies (ns). */
    private long[] timeWarmConversions(
            TableIdentifier id,
            Table table,
            IcestreamTableConfig config,
            EqualityDeleteFileRun run)
            throws Exception {
        long[] latency = new long[STREAM_RUNS];
        try (StreamingFlinkDeleteFileCreator converter =
                new StreamingFlinkDeleteFileCreator(flink, paimonIndex, CHECKPOINT_MS, CONV_TIMEOUT_MS)) {
            for (int r = 0; r < STREAM_RUNS; r++) {
                long t0 = System.nanoTime();
                CommitPlan plan = converter.create(id, table, run, config);
                latency[r] = System.nanoTime() - t0;
                log.info("conversion {}: {}  ({} positions)", r + 1, secs(latency[r]), totalPositions(plan));
            }
        }
        return latency;
    }

    private static long warmBest(long[] latency) {
        long best = Long.MAX_VALUE;
        for (int r = 1; r < latency.length; r++) { // skip run 0 (cold SST build)
            best = Math.min(best, latency[r]);
        }
        return best;
    }

    // --- reporting ------------------------------------------------------------------------------

    private void report(long totalRows, long parquetBytes, int fileCount, long indexNanos, List<SweepRow> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n============================= Icestream Conversion Benchmark =============================\n");
        sb.append(String.format("Data   : %,d rows across %d files, %s parquet%n", totalRows, fileCount, humanBytes(parquetBytes)));
        sb.append(String.format("Index  : %d buckets, format-version %d, Flink slots %d%n", BUCKETS, FORMAT_VERSION, SLOTS));
        if (indexNanos > 0) {
            double indexSec = indexNanos / 1e9;
            sb.append(String.format(
                    "Phase 1 INDEX: %.2f s   %,.0f rows/s   %.2f MB/s%n",
                    indexSec, totalRows / indexSec, (parquetBytes / 1e6) / indexSec));
        } else {
            sb.append("Phase 1 INDEX: (reused persisted index)\n");
        }
        sb.append("-----------------------------------------------------------------------------------------\n");
        sb.append(String.format(
                "%14s %10s   %10s   %10s   %9s   %s%n",
                "deletes (D)", "D/N", "lookup", "control", "speedup", "winner"));
        sb.append("-----------------------------------------------------------------------------------------\n");
        for (SweepRow row : rows) {
            boolean haveLookup = row.lookupNanos() >= 0;
            boolean haveControl = row.controlNanos() >= 0;
            String lookupStr = haveLookup ? String.format("%.2f s", row.lookupNanos() / 1e9) : "-";
            String controlStr = haveControl ? String.format("%.2f s", row.controlNanos() / 1e9) : "-";
            String speedupStr = "-";
            String winner = "-";
            if (haveLookup && haveControl) {
                double speedup = (double) row.controlNanos() / row.lookupNanos(); // >1 means lookup faster
                speedupStr = String.format("%.2fx", speedup);
                winner = speedup >= 1.0 ? "LOOKUP" : "control";
                if (row.lookupPos() != row.controlPos()) {
                    winner += " (POSITION MISMATCH!)";
                }
            }
            sb.append(String.format(
                    "%,14d %9.4f%% %10s %10s %9s   %s%n",
                    row.deletes(),
                    100.0 * row.deletes() / totalRows,
                    lookupStr,
                    controlStr,
                    speedupStr,
                    winner));
        }
        sb.append("=========================================================================================");
        // Print to stdout (surefire captures it into the test report); the logger may be routed
        // elsewhere by the active SLF4J provider.
        System.out.println(sb);
    }

    /**
     * The list of D (delete-count) values to sweep. {@code -Dbench.deletesSweep=1,1000,100000}
     * overrides; otherwise a single point from {@code bench.deletes}/{@code bench.ratio}. Values
     * are clamped to the row count and de-duplicated in ascending order.
     */
    private static List<Long> sweepValues(long singlePoint, long totalRows) {
        String spec = System.getProperty("bench.deletesSweep");
        if (spec == null || spec.isBlank()) {
            return List.of(singlePoint);
        }
        return java.util.Arrays.stream(spec.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .map(d -> Math.min(d, totalRows))
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    /** Gather the data files of the table's current snapshot (used when reusing persisted data). */
    private static List<DataFile> currentDataFiles(Table table) throws IOException {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(task.file());
            }
        }
        return files;
    }

    private boolean paimonIndexExists(TableIdentifier id) {
        try {
            paimonIndex.catalog().getTable(paimonIndex.identifierFor(id));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static long totalPositions(CommitPlan plan) {
        return plan.deletesToAdd().stream().mapToLong(DeleteFile::recordCount).sum();
    }

    private static long min(long[] xs) {
        long m = Long.MAX_VALUE;
        for (long x : xs) {
            m = Math.min(m, x);
        }
        return m;
    }

    // --- data generation ------------------------------------------------------------------------

    private DataFile writeDataFile(Table table, long startId, long count) throws IOException {
        // Write under the table's own data location (which lives in the warehouse) so the data
        // persists with the table and survives across runs — required for warm-start / reindex.
        // (Eq-delete files stay in the scratch temp dir; they are rewritten every run.)
        String path = table.locationProvider().newDataLocation("data-" + pathCounter.incrementAndGet() + ".parquet");
        OutputFile out = table.io().newOutputFile(path);
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        DataWriter<Record> writer =
                factory.newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        // Random name values (per-file seeded for reproducibility) keep parquet from
        // dictionary-collapsing, so the file size tracks the row count more predictably.
        Random rng = new Random(SEED ^ startId);
        Record template = GenericRecord.create(table.schema());
        try (Closeable toClose = writer) {
            for (long i = 0; i < count; i++) {
                long id = startId + i;
                template.setField("id", id);
                template.setField("name", randomName(rng));
                template.setField("ts", id);
                writer.write(template.copy());
            }
        }
        return writer.toDataFile();
    }

    private DeleteFile writeEqDelete(Table table, long totalRows, long deleteCount) throws IOException {
        Schema deleteSchema = table.schema().select("id");
        OutputFile out = table.io().newOutputFile(newFilePath("eq-delete", "parquet"));
        GenericAppenderFactory factory =
                new GenericAppenderFactory(table.schema(), table.spec(), new int[] {1}, deleteSchema, null);
        EqualityDeleteWriter<Record> writer =
                factory.newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, null);
        Set<Long> ids = pickDistinctIds(totalRows, deleteCount);
        Record projected = GenericRecord.create(deleteSchema);
        try (Closeable toClose = writer) {
            for (Long victim : ids) {
                projected.setField("id", victim);
                writer.write(projected.copy());
            }
        }
        return writer.toDeleteFile();
    }

    private static Set<Long> pickDistinctIds(long totalRows, long deleteCount) {
        Random rng = new Random(SEED * 31 + 7);
        Set<Long> ids = new LinkedHashSet<>((int) (deleteCount * 1.5));
        while (ids.size() < deleteCount) {
            ids.add(Math.floorMod(rng.nextLong(), totalRows));
        }
        return ids;
    }

    private static String randomName(Random rng) {
        char[] chars = new char[NAME_WIDTH];
        for (int i = 0; i < NAME_WIDTH; i++) {
            chars[i] = (char) ('a' + rng.nextInt(26));
        }
        return new String(chars);
    }

    private String newFilePath(String prefix, String ext) {
        return icebergWarehouse
                .resolve(prefix + "-" + pathCounter.incrementAndGet() + "." + ext)
                .toString();
    }

    private static Map<String, String> tableProps() {
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, Integer.toString(FORMAT_VERSION));
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, Integer.toString(BUCKETS));
        return props;
    }

    // --- helpers --------------------------------------------------------------------------------

    private static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format("%.1f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format("%.1f MB", mb);
        }
        return String.format("%.2f GB", mb / 1024.0);
    }

    private static String secs(long nanos) {
        return String.format("%.2f s", nanos / 1e9);
    }

    private static long longProp(String key, long fallback) {
        String v = System.getProperty(key);
        return (v == null || v.isBlank()) ? fallback : Long.parseLong(v.trim());
    }

    private static int intProp(String key, int fallback) {
        String v = System.getProperty(key);
        return (v == null || v.isBlank()) ? fallback : Integer.parseInt(v.trim());
    }

    private static boolean boolProp(String key, boolean fallback) {
        String v = System.getProperty(key);
        return (v == null || v.isBlank()) ? fallback : Boolean.parseBoolean(v.trim());
    }
}
