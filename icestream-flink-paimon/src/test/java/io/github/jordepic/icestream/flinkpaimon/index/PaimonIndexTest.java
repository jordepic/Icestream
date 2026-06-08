package io.github.jordepic.icestream.flinkpaimon.index;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import io.github.jordepic.icestream.schema.PrimaryKeySchema;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PaimonIndexTest {

    private PaimonIndex index;

    @BeforeEach
    void setUp(@TempDir Path warehouse) {
        index = PaimonIndex.create(warehouse.toUri().toString(), "icestream", Map.of());
    }

    @AfterEach
    void tearDown() throws Exception {
        index.catalog().close();
    }

    @Test
    void initializeForTable_createsPaimonTableWithExpectedSchema() throws Exception {
        TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "events");
        IcestreamTableConfig config = configWithBuckets(4);

        index.initializeForTable(tableId, config);

        Table created = index.catalog().getTable(Identifier.create("icestream", "db_events"));

        assertThat(created.partitionKeys()).containsExactly("spec_id", "partition_key");
        assertThat(created.primaryKeys()).containsExactly("spec_id", "partition_key", "pk");
        assertThat(created.options())
                .containsEntry(CoreOptions.BUCKET.key(), "4")
                .containsEntry(CoreOptions.BUCKET_KEY.key(), "pk")
                // Merge-on-read (DV off → "value"-processor lookup files), avro, and radical lookup
                // compaction that materializes + persists ssts to object store for download on the
                // read path (see the LocalTableQuery monkey patch).
                .containsEntry(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false")
                .containsEntry(CoreOptions.FILE_FORMAT.key(), "parquet")
                .containsEntry(CoreOptions.FORCE_LOOKUP.key(), "true")
                .containsEntry(CoreOptions.LOOKUP_COMPACT.key(), "radical")
                .containsEntry("lookup.remote-file.enabled", "true")
                .containsEntry("lookup.remote-file.level-threshold", "1");

        List<String> rowFieldNames =
                created.rowType().getFields().stream().map(DataField::name).toList();
        assertThat(rowFieldNames).containsExactly("spec_id", "partition_key", "pk", "data_file_path", "pos");
    }

    @Test
    void initializeForTable_isIdempotent() {
        TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "events");
        IcestreamTableConfig config = configWithBuckets(2);

        index.initializeForTable(tableId, config);
        index.initializeForTable(tableId, config);
    }

    @Test
    void identifierFor_flattensIcebergNamespaceDotsToUnderscores() {
        TableIdentifier nested = TableIdentifier.of(Namespace.of("a", "b", "c"), "events");

        assertThat(index.tableName(nested)).isEqualTo("a_b_c_events");
        assertThat(index.qualifiedTableName(nested)).isEqualTo("icestream.a_b_c_events");
    }

    private static IcestreamTableConfig configWithBuckets(int buckets) {
        return new IcestreamTableConfig(new PrimaryKeySchema(List.of()), buckets, 10);
    }
}
