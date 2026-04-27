package io.github.jordepic.icestream.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;

public final class CassandraIndex {

    private static final Pattern ILLEGAL_IDENT_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

    private final CqlSession session;
    private final String keyspace;

    public CassandraIndex(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    public CqlSession session() {
        return session;
    }

    public String keyspace() {
        return keyspace;
    }

    public String tableName(TableIdentifier table) {
        return ILLEGAL_IDENT_CHARS.matcher(table.toString()).replaceAll("_");
    }

    public String qualifiedTableName(TableIdentifier table) {
        return keyspace + "." + tableName(table);
    }

    public void createIfAbsent(TableIdentifier table) {
        session.execute("CREATE TABLE IF NOT EXISTS " + qualifiedTableName(table) + " ("
                + "spec_id int, "
                + "partition_key blob, "
                + "bucket int, "
                + "serialized_delete_condition blob, "
                + "data_file_path text, "
                + "pos bigint, "
                + "PRIMARY KEY ((spec_id, partition_key, bucket), serialized_delete_condition))");
    }
}
