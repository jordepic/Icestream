package io.github.jordepic.icestream.sparkcassandra.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class CassandraIndexTest {

    private static final String KEYSPACE = "icestream_test";

    @Container
    static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:5.0.3");

    private static CqlSession session;
    private CassandraIndex index;

    @BeforeAll
    static void openSession() {
        session = CqlSession.builder()
                .addContactPoint(CASSANDRA.getContactPoint())
                .withLocalDatacenter(CASSANDRA.getLocalDatacenter())
                .build();
    }

    @AfterAll
    static void closeSession() {
        session.close();
    }

    @BeforeEach
    void createKeyspace() {
        session.execute("CREATE KEYSPACE " + KEYSPACE
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        index = new CassandraIndex(session, KEYSPACE);
    }

    @AfterEach
    void dropKeyspace() {
        session.execute("DROP KEYSPACE " + KEYSPACE);
    }

    @Test
    void createIfAbsent_createsTableWithExpectedSchema() {
        TableIdentifier table = TableIdentifier.of("db", "t");

        index.createIfAbsent(table);

        Map<String, Row> columns = fetchColumns("db_t");
        assertThat(columns).containsOnlyKeys("spec_id", "partition_key", "bucket", "serialized_delete_condition", "data_file_path", "pos");
        assertThat(columns.get("spec_id").getString("kind")).isEqualTo("partition_key");
        assertThat(columns.get("partition_key").getString("kind")).isEqualTo("partition_key");
        assertThat(columns.get("bucket").getString("kind")).isEqualTo("partition_key");
        assertThat(columns.get("serialized_delete_condition").getString("kind")).isEqualTo("clustering");
        assertThat(columns.get("data_file_path").getString("kind")).isEqualTo("regular");
        assertThat(columns.get("pos").getString("kind")).isEqualTo("regular");
        assertThat(columns.get("spec_id").getString("type")).isEqualTo("int");
        assertThat(columns.get("partition_key").getString("type")).isEqualTo("blob");
        assertThat(columns.get("bucket").getString("type")).isEqualTo("int");
        assertThat(columns.get("serialized_delete_condition").getString("type")).isEqualTo("blob");
        assertThat(columns.get("data_file_path").getString("type")).isEqualTo("text");
        assertThat(columns.get("pos").getString("type")).isEqualTo("bigint");
    }

    @Test
    void createIfAbsent_isIdempotent() {
        TableIdentifier table = TableIdentifier.of("db", "t");

        index.createIfAbsent(table);
        index.createIfAbsent(table);

        assertThat(fetchColumns("db_t")).isNotEmpty();
    }

    @Test
    void tableName_sanitizesDotsToUnderscores() {
        String name = index.tableName(TableIdentifier.of("ns1", "ns2", "events"));

        assertThat(name).isEqualTo("ns1_ns2_events");
    }

    @Test
    void qualifiedTableName_prependsKeyspace() {
        String name = index.qualifiedTableName(TableIdentifier.of("db", "t"));

        assertThat(name).isEqualTo(KEYSPACE + ".db_t");
    }

    private Map<String, Row> fetchColumns(String cassandraTable) {
        Map<String, Row> columns = new HashMap<>();
        for (Row row : session.execute(
                "SELECT column_name, kind, type FROM system_schema.columns "
                        + "WHERE keyspace_name='" + KEYSPACE + "' AND table_name='" + cassandraTable + "'")) {
            columns.put(row.getString("column_name"), row);
        }
        return columns;
    }
}
