package io.github.jordepic.icestream.indexer;

import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Indexes a {@link DataFileRun} into the engine-specific secondary-index storage.
 * Implementations: {@code SparkDataFileIndexer}, {@code FlinkDataFileIndexer}.
 *
 * <p>The contract is engine-agnostic: read the run's data files at the run's max sequence number,
 * apply only positional-deletes / DVs at seq ≤ max, project to the pk schema, and persist
 * one index row per surviving data row keyed by {@code (spec_id, partition_key, pk)} →
 * {@code (data_file_path, pos)}.
 */
public interface DataIndexer {

    void index(TableIdentifier icebergTableId, Table table, DataFileRun run, IcestreamTableConfig config);
}
