package io.github.jordepic.icestream.index;

import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Engine-agnostic surface for the secondary-index storage. Two implementations:
 * <ul>
 *   <li>{@code CassandraIndex} (icestream-spark-cassandra) — backs the index with a Cassandra
 *       table per iceberg table. Uses the Spark Cassandra connector for replica-aware writes
 *       and pushed-down lookup joins.
 *   <li>{@code PaimonIndex} (icestream-flink-paimon) — backs the index with a Paimon
 *       primary-key table per iceberg table on the same object store as the iceberg warehouse.
 * </ul>
 */
public interface IndexBackend {

    /**
     * Idempotent: ensure the per-iceberg-table index storage exists. Called once per
     * {@code TableProcessor.processNextRun} before any indexer or converter work.
     */
    void initializeForTable(TableIdentifier icebergTableId, IcestreamTableConfig config);
}
