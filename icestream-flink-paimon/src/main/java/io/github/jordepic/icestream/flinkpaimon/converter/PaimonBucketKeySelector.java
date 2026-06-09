package io.github.jordepic.icestream.flinkpaimon.converter;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;

/**
 * Keys probe rows by the <b>Paimon bucket</b> their pk lands in, so the lookup join's input is
 * partitioned the same way the index is bucketed: each lookup subtask then only ever queries (and
 * caches) the buckets routed to it, instead of every subtask caching every bucket it happens to be
 * handed under round-robin {@code rebalance}. Cache locality only — correctness is identical either
 * way, since the lookup function can resolve any key regardless of which subtask runs it.
 *
 * <p>The bucket is computed by Paimon's own {@link FixedBucketRowKeyExtractor} over the index table's
 * {@link TableSchema} (bucket = {@code abs(bucketKey.hashCode() % numBuckets)} for the {@code pk}
 * bucket-key), so it matches the write-side assignment by construction — no hash replication. The
 * schema is {@link java.io.Serializable}, so it ships with the operator; the extractor (stateful, not
 * serializable) is built lazily per subtask.
 */
public final class PaimonBucketKeySelector implements KeySelector<Row, Integer> {

    private static final long serialVersionUID = 1L;

    private final TableSchema indexSchema;
    private transient FixedBucketRowKeyExtractor extractor;
    private transient GenericRow reusable;

    public PaimonBucketKeySelector(TableSchema indexSchema) {
        this.indexSchema = indexSchema;
    }

    @Override
    public Integer getKey(Row row) {
        if (extractor == null) {
            extractor = new FixedBucketRowKeyExtractor(indexSchema);
            // Full index-row width (spec_id, partition_key, pk, data_file_path, pos); only the pk
            // (index 2 = bucket-key) feeds the bucket computation, so the rest can stay unset.
            reusable = new GenericRow(5);
        }
        // Probe row layout matches EqDeleteSourceFlatMap: (spec_id, partition_key, pk).
        reusable.setField(2, BinaryString.fromString((String) row.getField(2)));
        extractor.setRecord(reusable);
        return extractor.bucket();
    }
}
