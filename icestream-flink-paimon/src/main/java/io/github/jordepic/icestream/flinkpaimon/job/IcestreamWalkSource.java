package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.indexer.FileWorkItem;
import io.github.jordepic.icestream.master.IcestreamWatermark;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.planner.FileKind;
import io.github.jordepic.icestream.planner.FileRun;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import io.github.jordepic.icestream.planner.State;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallelism-1 unbounded source that <em>is</em> the control loop: it walks the
 * {@link SnapshotPlanner} in {@code (seq, kind)} order and emits one {@link WorkUnit} per run, then
 * keeps the job alive (so the downstream Paimon lookup cache stays warm across runs).
 *
 * <p><b>Pacing &amp; ordering.</b> Progress is owned by {@link RunCommitter}, which advances the
 * iceberg watermark only after a run is durably committed (the Paimon index commit for DATA, the
 * {@code RowDelta} for EQ_DEL). This source reads that watermark from iceberg, plans the next run,
 * emits it, then blocks — re-reading the watermark — until the committer has caught up to the run it
 * just emitted, before planning the next. One run in flight at a time. Because a single sequential
 * source drives the whole walk, the strict {@code EQ_DEL(N)} → must-see-all-{@code DATA<N}
 * interleaving is correct by construction, and the "gate on the last persisted snapshot" falls out
 * for free: an {@code EQ_DEL(N)} is only planned once the watermark sits at the last {@code DATA<N},
 * which the committer advanced to only after that data's Paimon commit.
 *
 * <p><b>Crash safety.</b> No Flink-checkpointed state: on restart the source rebuilds the catalog
 * and resumes from the iceberg watermark. A crash after emit-before-commit re-emits the same run;
 * re-running a unit reproduces the same files and the commit is idempotent.
 *
 * <p>The run is emitted under the checkpoint lock so its whole computation stays inside one
 * checkpoint epoch — the barrier that follows triggers the writer flush / Paimon commit that the
 * committer keys its commit off.
 */
public final class IcestreamWalkSource extends RichSourceFunction<WorkUnit> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcestreamWalkSource.class);

    private final IcebergCatalogSpec catalogSpec;
    private final String tableId;
    private final int formatVersion;
    private final long pollIntervalMs;
    private final long idleBackoffMs;

    private volatile boolean running = true;
    private transient Catalog catalog;

    public IcestreamWalkSource(
            IcebergCatalogSpec catalogSpec,
            TableIdentifier tableId,
            int formatVersion,
            long pollIntervalMs,
            long idleBackoffMs) {
        this.catalogSpec = catalogSpec;
        this.tableId = tableId.toString();
        this.formatVersion = formatVersion;
        this.pollIntervalMs = pollIntervalMs;
        this.idleBackoffMs = idleBackoffMs;
    }

    @Override
    public void run(SourceContext<WorkUnit> ctx) throws Exception {
        catalog = catalogSpec.load();
        TableIdentifier id = TableIdentifier.parse(tableId);
        Table table = catalog.loadTable(id);
        SnapshotPlanner planner = new SnapshotPlanner();
        State emitted = null; // last position emitted, awaiting the committer

        while (running) {
            table.refresh();
            State watermark = IcestreamWatermark.read(table.properties());
            if (emitted != null && watermark.compareTo(emitted) < 0) {
                // Committer hasn't committed the in-flight run yet — wait, don't emit anything else.
                sleep(pollIntervalMs);
                continue;
            }
            Optional<FileRun> next = planner.planNextRun(table, watermark);
            if (next.isEmpty()) {
                sleep(idleBackoffMs);
                continue;
            }
            FileRun run = next.get();
            long startingSnapshotId = table.currentSnapshot().snapshotId();
            WorkUnit unit = buildUnit(table, run, startingSnapshotId);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(unit);
            }
            LOG.info("emitted run kind={} maxSeq={} table={}", run.kind(), run.maxSeq(), tableId);
            emitted = new State(run.maxSeq(), run.kind());
        }
    }

    /**
     * Do all the live-table work for a run here on the p1 source (the driver-equivalent), so the
     * emitted {@link WorkUnit} carries only Flink-serializable value types: build the per-file work
     * items and, for EQ_DEL, scan the prior per-data-file deletes (partition-filtered) and capture
     * the eq-delete files the committer will remove.
     */
    private WorkUnit buildUnit(Table table, FileRun run, long startingSnapshotId) {
        if (run instanceof DataFileRun dataRun) {
            return new WorkUnit(
                    FileKind.DATA,
                    run.maxSeq(),
                    startingSnapshotId,
                    List.of(),
                    FileWorkItem.buildAll(table, dataRun),
                    Map.of(),
                    List.of());
        }
        EqualityDeleteFileRun eqRun = (EqualityDeleteFileRun) run;
        List<EqDeleteWorkItem> eqItems = DeleteFileCreatorSupport.buildWorkItems(table, eqRun);
        Set<PartitionKey> touched = DeleteFileCreatorSupport.touchedPartitions(eqItems);
        Map<String, ExistingPerFileDeletes> existing =
                ExistingPerFileDeleteLoader.collect(table, formatVersion, touched);
        return new WorkUnit(
                FileKind.EQ_DEL,
                run.maxSeq(),
                startingSnapshotId,
                eqItems,
                List.of(),
                existing,
                List.copyOf(eqRun.files()));
    }

    private void sleep(long ms) throws InterruptedException {
        if (ms > 0) {
            Thread.sleep(ms);
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (catalog instanceof Closeable c) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }
}
