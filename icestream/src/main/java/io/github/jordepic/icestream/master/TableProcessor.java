package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.ConversionCommitter;
import io.github.jordepic.icestream.converter.DeleteFileCreator;
import io.github.jordepic.icestream.indexer.DataFileIndexer;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.planner.FileKind;
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
 * Drives a single icestream "run" against a table: data files indexed into Cassandra OR eq-delete
 * files converted to DVs and committed via RowDelta.
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
    private final DataFileIndexer indexer;
    private final DeleteFileCreator creator;
    private final CassandraIndex cassandra;
    private final IcestreamMetrics metrics;

    public TableProcessor(
            SnapshotPlanner planner,
            DataFileIndexer indexer,
            DeleteFileCreator creator,
            CassandraIndex cassandra,
            IcestreamMetrics metrics) {
        this.planner = planner;
        this.indexer = indexer;
        this.creator = creator;
        this.cassandra = cassandra;
        this.metrics = metrics;
    }

    @Override
    public boolean processNextRun(TableIdentifier id, Table table) {
        Map<String, String> propsAtStart = Map.copyOf(table.properties());
        if (!propsAtStart.containsKey(IcestreamProperties.PRIMARY_KEYS)) {
            return false;
        }
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        cassandra.createIfAbsent(id);

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
            commitWatermark(table, new State(run.maxSeq(), run.kind()), propsAtStart);
            metrics.recordRunSuccess(id, run.kind(), fileCount, elapsedSince(startNanos));
            return true;
        } catch (RuntimeException | Error e) {
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
            CommitPlan plan = creator.create(id, table, eqRun, config);
            ConversionCommitter.commit(table, plan);
        }
    }

    private static State readState(Map<String, String> props) {
        long seq = Long.parseLong(props.getOrDefault(IcestreamProperties.LAST_PROCESSED_SEQUENCE, "0"));
        FileKind kind =
                FileKind.valueOf(props.getOrDefault(IcestreamProperties.LAST_PROCESSED_KIND, FileKind.DATA.name()));
        return new State(seq, kind);
    }

    private static void commitWatermark(Table table, State state, Map<String, String> propsAtStart) {
        table.updateProperties()
                .set(IcestreamProperties.LAST_PROCESSED_SEQUENCE, Long.toString(state.sequenceNumber()))
                .set(IcestreamProperties.LAST_PROCESSED_KIND, state.fileKind().name())
                .set(IcestreamProperties.PINNED_PRIMARY_KEYS, propsAtStart.get(IcestreamProperties.PRIMARY_KEYS))
                .set(
                        IcestreamProperties.PINNED_CASSANDRA_BUCKETS,
                        propsAtStart.getOrDefault(IcestreamProperties.CASSANDRA_BUCKETS, "1"))
                .commit();
    }
}
