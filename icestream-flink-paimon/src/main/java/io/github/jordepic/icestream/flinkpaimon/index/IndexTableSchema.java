package io.github.jordepic.icestream.flinkpaimon.index;

import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

/**
 * Builds the Paimon {@link Schema} icestream uses for its secondary index table. Schema is
 * deterministic given the iceberg-table config; called once when the index table is first
 * created via {@link PaimonIndex#initializeForTable}.
 *
 * <p>Layout:
 * <ul>
 *   <li>columns {@code spec_id INT, partition_key STRING, pk STRING, data_file_path STRING,
 *       pos BIGINT}. {@code partition_key} and {@code pk} carry hex-encoded avro bytes —
 *       Paimon's lookup machinery requires Comparable types on PK columns, and {@code byte[]}
 *       is not Comparable. Hex preserves byte ordering and stays sortable.
 *   <li>primary key {@code (spec_id, partition_key, pk)} — what the lookup join probes.
 * </ul>
 *
 * <p>Partitioning: {@code (spec_id, partition_key)}. Mirrors the iceberg table's partition
 * layout so Paimon's filesystem write places each iceberg partition's index rows into a distinct
 * Paimon partition directory, and Flink's lookup join can prune to the matching partition per
 * probe row (the join condition includes equality on both partition columns). The hex-encoded
 * {@code STRING} partition_key renders cleanly into the path string.
 *
 * <p>Options:
 * <ul>
 *   <li>{@code bucket = icestream.index-buckets} — fixed-bucket mode (required for lookup join).
 *   <li>{@code bucket-key = pk} — distribute rows across buckets by primary key.
 *   <li>{@code deletion-vectors.enabled = true} — DV mode triggers lookup-friendly compaction
 *       inside the writer, so we don't need {@code changelog-producer = lookup} on top.
 *   <li>{@code num-sorted-run.compaction-trigger = 2} — lower than default 5; keep L0 small
 *       between batches so lookup probes don't fan out.
 * </ul>
 */
public final class IndexTableSchema {

    public static final String COL_SPEC_ID = "spec_id";
    public static final String COL_PARTITION_KEY = "partition_key";
    public static final String COL_PK = "pk";
    public static final String COL_DATA_FILE_PATH = "data_file_path";
    public static final String COL_POS = "pos";

    private IndexTableSchema() {}

    public static Schema build(IcestreamTableConfig config) {
        return Schema.newBuilder()
                .column(COL_SPEC_ID, DataTypes.INT())
                .column(COL_PARTITION_KEY, DataTypes.STRING())
                .column(COL_PK, DataTypes.STRING())
                .column(COL_DATA_FILE_PATH, DataTypes.STRING())
                .column(COL_POS, DataTypes.BIGINT())
                .partitionKeys(COL_SPEC_ID, COL_PARTITION_KEY)
                .primaryKey(COL_SPEC_ID, COL_PARTITION_KEY, COL_PK)
                .option(CoreOptions.BUCKET.key(), Integer.toString(config.indexBuckets()))
                .option(CoreOptions.BUCKET_KEY.key(), COL_PK)
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "2")
                .build();
    }
}
