package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.ConversionCommitter;
import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.master.IcestreamWatermark;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.planner.State;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallelism-1 terminal operator that owns all correctness: it commits each run and advances the
 * iceberg watermark, which is what paces {@link IcestreamWalkSource}. There is no RPC and no driver —
 * the source emits, the writers flush, this operator commits, the source sees the watermark move.
 *
 * <p>Two inputs, both delivered within the run's checkpoint epoch:
 * <ul>
 *   <li>input 1 — the {@link WorkUnit} control record (which run is in this epoch + its starting
 *       snapshot id).
 *   <li>input 2 — the distributed writers' {@link TaskOutputs} slices (EQ_DEL only); barrier
 *       alignment guarantees all N have arrived before the barrier.
 * </ul>
 *
 * <p>On {@code notifyCheckpointComplete(K)} — when the epoch's writer flush and Paimon sink commit
 * have happened — it commits the epoch's run:
 * <ul>
 *   <li><b>EQ_DEL</b>: assemble the {@link CommitPlan} from the writer slices and commit the
 *       {@code RowDelta} (validateFromSnapshot + {@code icestream-converted} tag), then advance the
 *       watermark to {@code (maxSeq, EQ_DEL)}.
 *   <li><b>DATA</b>: poll the Paimon index until its latest snapshot moves past the pre-run snapshot
 *       (the sink committed this epoch's rows), then advance the watermark to {@code (maxSeq, DATA)}.
 * </ul>
 *
 * <p>The commit runs after the checkpoint rather than inside it (no exactly-once) — correctness comes
 * from idempotent re-runs + the watermark only advancing post-commit, exactly the at-least-once
 * model the channel design used. A failure here restarts the job; the source re-emits from the
 * unadvanced watermark.
 */
public final class RunCommitter extends AbstractStreamOperator<Void>
        implements TwoInputStreamOperator<WorkUnit, TaskOutputs, Void> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RunCommitter.class);
    private static final long PAIMON_COMMIT_POLL_MS = 200;

    private final IcebergCatalogSpec catalogSpec;
    private final String tableId;
    private final PaimonIndexSpec indexSpec;
    private final long paimonCommitTimeoutMs;

    private transient Catalog catalog;
    private transient Table table;
    private transient PaimonIndex paimonIndex;
    private transient FileStoreTable indexTable;

    private transient WorkUnit currentUnit;
    private transient List<TaskOutputs> epochOutputs;

    public RunCommitter(
            IcebergCatalogSpec catalogSpec, TableIdentifier tableId, PaimonIndexSpec indexSpec, long paimonCommitTimeoutMs) {
        this.catalogSpec = catalogSpec;
        this.tableId = tableId.toString();
        this.indexSpec = indexSpec;
        this.paimonCommitTimeoutMs = paimonCommitTimeoutMs;
    }

    @Override
    public void open() throws Exception {
        super.open();
        catalog = catalogSpec.load();
        table = catalog.loadTable(TableIdentifier.parse(tableId));
        paimonIndex = indexSpec.load();
        indexTable = (FileStoreTable) paimonIndex.load(TableIdentifier.parse(tableId));
        epochOutputs = new ArrayList<>();
    }

    @Override
    public void processElement1(StreamRecord<WorkUnit> element) {
        currentUnit = element.getValue();
    }

    @Override
    public void processElement2(StreamRecord<TaskOutputs> element) {
        epochOutputs.add(element.getValue());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        if (currentUnit == null) {
            return;
        }
        WorkUnit unit = currentUnit;
        try {
            if (unit.isEqDelete()) {
                commitConversion(unit);
            } else {
                awaitPaimonCommit(checkpointId);
            }
            table.refresh();
            IcestreamWatermark.commit(table, new State(unit.maxSeq(), unit.kind()), table.properties());
            LOG.info("committed run kind={} maxSeq={} table={}", unit.kind(), unit.maxSeq(), tableId);
        } finally {
            currentUnit = null;
            epochOutputs.clear();
        }
    }

    private void commitConversion(WorkUnit unit) {
        table.refresh();
        EqualityDeleteFileRun run = new EqualityDeleteFileRun(unit.maxSeq(), unit.eqDeletesToRemove());
        CommitPlan plan =
                DeleteFileCreatorSupport.assemblePlan(unit.startingSnapshotId(), run, new ArrayList<>(epochOutputs));
        ConversionCommitter.commit(table, plan);
    }

    /**
     * Wait until the Paimon index has durably committed this DATA run's rows before advancing the
     * watermark — otherwise a later EQ_DEL conversion's lookup could miss them. The streaming sink
     * commits epoch {@code K}'s rows under commit identifier {@code K} (Paimon stamps each streaming
     * commit with the Flink checkpoint id, and {@link org.apache.paimon.Snapshot#commitIdentifier()}
     * is monotonic), so we poll the index's latest snapshot until its commit identifier reaches the
     * checkpoint that flushed this run. Exact — unlike snapshot-id advancement, a background
     * compaction snapshot can't satisfy it. Bounded by {@code paimonCommitTimeoutMs}.
     */
    private void awaitPaimonCommit(long checkpointId) throws InterruptedException {
        long deadline = System.nanoTime() + paimonCommitTimeoutMs * 1_000_000L;
        while (latestPaimonCommitIdentifier() < checkpointId) {
            if (System.nanoTime() >= deadline) {
                LOG.warn(
                        "Paimon index commit identifier did not reach checkpoint {} within {}ms for DATA run on {};"
                                + " proceeding",
                        checkpointId,
                        paimonCommitTimeoutMs,
                        tableId);
                return;
            }
            Thread.sleep(PAIMON_COMMIT_POLL_MS);
        }
    }

    private long latestPaimonCommitIdentifier() {
        org.apache.paimon.Snapshot latest = indexTable.snapshotManager().latestSnapshot();
        return latest == null ? Long.MIN_VALUE : latest.commitIdentifier();
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeQuietly(paimonIndex == null ? null : paimonIndex.catalog());
        closeQuietly(catalog instanceof Closeable c ? c : null);
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }
}
