package io.github.jordepic.icestream.schema;

import org.apache.iceberg.Table;

public record IcestreamTableConfig(PrimaryKeySchema primaryKey, int cassandraBuckets, int cassandraPartitionsPerHost) {

    public static IcestreamTableConfig from(Table table) {
        return new IcestreamTableConfig(
                PrimaryKeySchema.parse(table.schema(), requireProperty(table, IcestreamProperties.PRIMARY_KEYS)),
                Integer.parseInt(getOrDefault(table, IcestreamProperties.CASSANDRA_BUCKETS, "1")),
                Integer.parseInt(getOrDefault(
                        table,
                        IcestreamProperties.CASSANDRA_PARTITIONS_PER_HOST,
                        Integer.toString(IcestreamProperties.DEFAULT_CASSANDRA_PARTITIONS_PER_HOST))));
    }

    private static String requireProperty(Table table, String key) {
        String value = table.properties().get(key);
        if (value == null) {
            throw new IllegalStateException("Table " + table.name() + " missing property " + key);
        }
        return value;
    }

    private static String getOrDefault(Table table, String key, String defaultValue) {
        String value = table.properties().get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
