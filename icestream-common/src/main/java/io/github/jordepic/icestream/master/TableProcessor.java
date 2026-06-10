package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.ConversionCommitter;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.index.IndexBackend;
import io.github.jordepic.icestream.indexer.DataIndexer;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.planner.FileRun;
import io.github.jordepic.icestream.planner.SnapshotPlanner;
import io.github.jordepic.icestream.planner.State;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives a single icestream "run" against a table: data files indexed via the configured
 * {@link DataIndexer}, OR eq-delete files converted to per-data-file delete files via the
 * configured {@link DeleteConverter} and committed via {@link ConversionCommitter} in a
 * {@code RowDelta}.
 *
 * <p>Engine-agnostic: TableProcessor holds interfaces. Backends (Spark+Cassandra,
 * Flink+Paimon, ...) wire their concretes in their respective {@code Main} class.
 *
 * <p>One run per call. Caller (master loop) is responsible for refreshing the table and re-invoking
 * to drain a backlog. State watermark + pinned config are persisted as iceberg properties only when
 * a run was applied successfully.
 *
 * <p>Pinned values are captured at the START of {@link #processNextRun} so a concurrent property
 * edit during the run can't sneak in undetected — drift will be caught on the next call.
 */
public final class TableProcessor implements RunProcessor {

    private static final Logger log = LoggerFactory.getLogger(TableProcessor.class);

    private final SnapshotPlanner planner;
    private final DataIndexer indexer;
    private final DeleteConverter converter;
    private final IndexBackend index;
    private final IcestreamMetrics metrics;

    public TableProcessor(
            SnapshotPlanner planner,
            DataIndexer indexer,
            DeleteConverter converter,
            IndexBackend index,
            IcestreamMetrics metrics) {
        this.planner = planner;
        this.indexer = indexer;
        this.converter = converter;
        this.index = index;
        this.metrics = metrics;
    }

    @Override
    public boolean processNextRun(TableIdentifier id, Table table) {
        Map<String, String> propsAtStart = Map.copyOf(table.properties());
        if (!propsAtStart.containsKey(IcestreamProperties.PRIMARY_KEYS)) {
            return false;
        }
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        index.initializeForTable(id, config);

        Optional<FileRun> nextRun = planner.planNextRun(table, readState(propsAtStart));
        if (nextRun.isEmpty()) {
            return false;
        }
        FileRun run = nextRun.get();
        int fileCount = fileCount(run);
        log.info("Processing run kind={} maxSeq={} fileCount={} table={}", run.kind(), run.maxSeq(), fileCount, id);
        long startNanos = System.nanoTime();
        try {
            applyRun(id, table, run, config);
            IcestreamWatermark.commit(table, new State(run.maxSeq(), run.kind()), propsAtStart);
            metrics.recordRunSuccess(id, run.kind(), fileCount, elapsedSince(startNanos));
            return true;
        } catch (RuntimeException | Error e) {
            log.warn("Run failed kind={} maxSeq={} table={}", run.kind(), run.maxSeq(), id, e);
            metrics.recordRunFailure(id, run.kind(), fileCount, elapsedSince(startNanos), e);
            throw e;
        }
    }

    private static int fileCount(FileRun run) {
        if (run instanceof DataFileRun dataRun) {
            return dataRun.files().size();
        }
        if (run instanceof EqualityDeleteFileRun eqRun) {
            return eqRun.files().size();
        }
        throw new IllegalStateException("Unknown FileRun: " + run);
    }

    private static Duration elapsedSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    private void applyRun(TableIdentifier id, Table table, FileRun run, IcestreamTableConfig config) {
        if (run instanceof DataFileRun dataRun) {
            indexer.index(id, table, dataRun, config);
        } else if (run instanceof EqualityDeleteFileRun eqRun) {
            CommitPlan plan = converter.create(id, table, eqRun, config);
            ConversionCommitter.commit(table, plan);
        }
    }

    private static State readState(Map<String, String> props) {
        return IcestreamWatermark.read(props);
    }
}
