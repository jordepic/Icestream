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
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

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

    private final SnapshotPlanner planner;
    private final DataFileIndexer indexer;
    private final DeleteFileCreator creator;
    private final CassandraIndex cassandra;

    public TableProcessor(
            SnapshotPlanner planner,
            DataFileIndexer indexer,
            DeleteFileCreator creator,
            CassandraIndex cassandra) {
        this.planner = planner;
        this.indexer = indexer;
        this.creator = creator;
        this.cassandra = cassandra;
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
        applyRun(id, table, run, config);
        commitWatermark(table, new State(run.maxSeq(), run.kind()), propsAtStart);
        return true;
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
