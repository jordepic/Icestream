package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Shared Flink+Paimon lookup-join wiring for the {@link StreamingFlinkDeleteFileCreator}: registers
 * the Paimon catalog, the probe types, and turns a stream of probe rows {@code (spec_id,
 * partition_key, pk)} into the lookup matches {@code (spec_id, partition_key, data_file_path, pos)}
 * via {@link LookupJoinSql}.
 */
public final class PaimonLookupJoin {

    private static final String CATALOG = "paimon";

    /** Probe row layout the eq-delete reader emits and the lookup join consumes. */
    public static final TypeInformation<Row> PROBE_TYPE_INFO = new RowTypeInfo(
            new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
            new String[] {"spec_id", "partition_key", "pk"});

    private PaimonLookupJoin() {}

    public static List<NestedField> pkFields(Table table) {
        return IcestreamTableConfig.from(table).primaryKey().fields();
    }

    /** Register the Paimon index catalog (named {@value #CATALOG}) on this table environment. */
    public static void registerCatalog(StreamTableEnvironment tEnv, PaimonIndex paimonIndex) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "paimon");
        options.putAll(paimonIndex.catalogOptionsForFlink());
        StringBuilder withClause = new StringBuilder();
        options.forEach((k, v) -> withClause.append("'").append(k).append("'='").append(v).append("',"));
        if (withClause.length() > 0) {
            withClause.setLength(withClause.length() - 1);
        }
        tEnv.executeSql("CREATE CATALOG " + CATALOG + " WITH (" + withClause + ")");
    }

    /** Fully-qualified name of an iceberg table's Paimon index in the registered catalog. */
    public static String indexFqn(PaimonIndex paimonIndex, TableIdentifier id) {
        return String.format("`%s`.`%s`.`%s`", CATALOG, paimonIndex.database(), paimonIndex.tableName(id));
    }

    /** The probe view's schema: the three probe columns plus a {@code PROCTIME()} the temporal join needs. */
    public static Schema probeViewSchema() {
        return Schema.newBuilder()
                .column("spec_id", DataTypes.INT())
                .column("partition_key", DataTypes.STRING())
                .column("pk", DataTypes.STRING())
                .columnByExpression("proc", "PROCTIME()")
                .build();
    }

    /**
     * Register {@code probeRows} as the eq-delete view and run the temporal lookup join against the
     * index, returning the match rows.
     */
    public static DataStream<Row> lookupMatches(
            StreamTableEnvironment tEnv,
            DataStream<Row> probeRows,
            PaimonIndex paimonIndex,
            TableIdentifier id) {
        tEnv.createTemporaryView(LookupJoinSql.EQ_DELETES_VIEW, tEnv.fromDataStream(probeRows, probeViewSchema()));
        return tEnv.toDataStream(tEnv.sqlQuery(LookupJoinSql.lookupJoin(indexFqn(paimonIndex, id))));
    }
}
