package io.github.jordepic.icestream.schema;

public final class IcestreamProperties {

    public static final String PRIMARY_KEYS = "icestream.primary-keys";
    public static final String INDEX_BUCKETS = "icestream.index-buckets";

    /** Spark Cassandra connector knob (icestream-spark-cassandra only). */
    public static final String CASSANDRA_PARTITIONS_PER_HOST = "icestream.cassandra-partitions-per-host";

    public static final int DEFAULT_CASSANDRA_PARTITIONS_PER_HOST = 10;

    public static final String LAST_PROCESSED_SEQUENCE = "icestream.last-processed-sequence";
    public static final String LAST_PROCESSED_KIND = "icestream.last-processed-kind";

    /**
     * Set by the Flink+Paimon backend after each successful indexer run; consumed by the
     * converter's {@code FOR SYSTEM_TIME AS OF} to pin the lookup join to a Paimon snapshot
     * that is at least as fresh as the indexed state of the iceberg snapshot being converted.
     */
    public static final String LAST_INDEXED_PAIMON_SNAPSHOT = "icestream.last-indexed-paimon-snapshot";

    public static final String PINNED_PRIMARY_KEYS = "icestream.pinned-primary-keys";
    public static final String PINNED_INDEX_BUCKETS = "icestream.pinned-index-buckets";

    private IcestreamProperties() {}
}
