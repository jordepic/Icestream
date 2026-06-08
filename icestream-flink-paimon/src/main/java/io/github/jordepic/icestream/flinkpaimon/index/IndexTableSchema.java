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
 *   <li>{@code deletion-vectors.enabled = false} — merge-on-read with the "value" persist processor,
 *       so write-built lookup ssts match the read side and can be downloaded as remote files.
 *   <li>{@code force-lookup = true}, {@code lookup-compact = radical}, {@code lookup-wait = true} —
 *       each write does a lookup compaction (ForceUpLevel0Compaction pushes L0→L1) that materializes
 *       the lookup ssts, and the commit waits for it so the next conversion reads a compacted state.
 *   <li>{@code lookup.remote-file.enabled = true}, {@code lookup.remote-file.level-threshold = 1} —
 *       persist L1+ ssts to the warehouse (object store); paged-out files are downloaded, not rebuilt
 *       (see the {@code org.apache.paimon.table.query.LocalTableQuery} monkey patch).
 *   <li>{@code file.format = parquet}, {@code file.compression = zstd} — Paimon's defaults. The
 *       lookup reads the separate {@code .lookup} ssts (format-independent), so this mainly affects
 *       on-disk index size and compaction cost.
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
                // Deletion-vectors OFF → merge-on-read with the "value" persist processor, so the
                // lookup ssts the write side builds use the same processor the read-side
                // LocalTableQuery reads — required for the remote ssts to be downloadable.
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false")
                // Index data files in Paimon's defaults (parquet + zstd). The lookup probes the
                // separate .lookup ssts (format-independent), so this mainly shrinks the index on
                // disk and shifts compaction cost vs the earlier avro/uncompressed layout.
                .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                .option(CoreOptions.FILE_COMPRESSION.key(), "zstd")
                // Lookup compaction that materializes ssts on write and persists them to object store,
                // so a paged-out lookup file is re-fetched by DOWNLOAD (via the LocalTableQuery monkey
                // patch) rather than rebuilt from the data file:
                //  - force-lookup: take the lookup-compaction path without needing DV/changelog=lookup.
                //  - lookup-compact=radical: ForceUpLevel0Compaction pushes each write's L0 up to L1.
                //    Our writes are whole iceberg data files (well-populated L0, not a trickle), so this
                //    keeps the read side hitting a small set of sst-backed higher-level files.
                //  - lookup-wait: the commit waits for that compaction, so the conversion that follows
                //    reads a fully-compacted, sst-built state.
                //  - remote-file.enabled + level-threshold=1: upload L1+ ssts to the warehouse; skip
                //    transient L0. (num-sorted-run.compaction-trigger left at the Paimon default.)
                .option(CoreOptions.FORCE_LOOKUP.key(), "true")
                .option(CoreOptions.LOOKUP_COMPACT.key(), "radical")
                .option("lookup-wait", "true")
                .option("lookup.remote-file.enabled", "true")
                .option("lookup.remote-file.level-threshold", "1")
                // Radical compaction collapses each bucket toward a single sorted run, so the
                // per-file lookup bloom has ~nothing to skip — it's redundant read-time paging. Off.
                // (lookup.cache-spill-compression left at the default "zstd": with a starved cache,
                // compression shrinks the bytes re-read per probe, which beats skipping decompress —
                // a "none" trial was ~2.6x slower as the uncompressed SSTs quadrupled the read volume.)
                .option("lookup.cache.bloom.filter.enabled", "false")
                // Refresh the lookup join's view quickly so a conversion sees a just-written delta.
                .option("continuous.discovery-interval", "1 s")
                .build();
    }
}
