package io.github.jordepic.icestream.schema;

public final class IcestreamProperties {

    public static final String PRIMARY_KEYS = "icestream.primary-keys";
    public static final String CASSANDRA_BUCKETS = "icestream.cassandra-buckets";
    public static final String CASSANDRA_PARTITIONS_PER_HOST = "icestream.cassandra-partitions-per-host";
    public static final int DEFAULT_CASSANDRA_PARTITIONS_PER_HOST = 10;

    public static final String LAST_PROCESSED_SEQUENCE = "icestream.last-processed-sequence";
    public static final String LAST_PROCESSED_KIND = "icestream.last-processed-kind";

    public static final String PINNED_PRIMARY_KEYS = "icestream.pinned-primary-keys";
    public static final String PINNED_CASSANDRA_BUCKETS = "icestream.pinned-cassandra-buckets";

    private IcestreamProperties() {}
}
