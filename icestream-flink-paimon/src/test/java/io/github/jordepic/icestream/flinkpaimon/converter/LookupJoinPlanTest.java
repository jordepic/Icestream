package io.github.jordepic.icestream.flinkpaimon.converter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.schema.IcestreamProperties;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Guards the planner choice for the convert branch: the {@code FOR SYSTEM_TIME AS OF} lookup join
 * must compile to a {@code LookupJoin} against Paimon's {@code FileStoreLookupFunction} (indexed
 * point probes), not a regular hash/sort-merge join — otherwise we silently lose indexed-probe
 * semantics.
 */
class LookupJoinPlanTest {

    private static final org.apache.iceberg.Schema SCHEMA = new org.apache.iceberg.Schema(
            NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()),
            NestedField.required(2, "name", org.apache.iceberg.types.Types.StringType.get()));

    @TempDir
    Path icebergWarehouse;

    @TempDir
    Path paimonWarehouse;

    private HadoopCatalog icebergCatalog;
    private PaimonIndex paimonIndex;

    @BeforeEach
    void setUp() {
        icebergCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse.toString());
        paimonIndex = PaimonIndex.create(paimonWarehouse.toUri().toString(), "icestream", Map.of());
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            paimonIndex.catalog().close();
        } catch (Exception ignored) {
        }
        icebergCatalog.close();
    }

    @Test
    void lookupJoinPlanUsesPaimonFileStoreLookupFunction() {
        TableIdentifier id = TableIdentifier.of("db", "events");
        Map<String, String> props = new HashMap<>();
        props.put(TableProperties.FORMAT_VERSION, "3");
        props.put(IcestreamProperties.PRIMARY_KEYS, "id");
        props.put(IcestreamProperties.INDEX_BUCKETS, "2");
        Table table = icebergCatalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), props);
        IcestreamTableConfig config = IcestreamTableConfig.from(table);
        paimonIndex.initializeForTable(id, config);

        try (FlinkContext ctx = FlinkContext.local(1)) {
            StreamExecutionEnvironment env = ctx.newBatchEnv();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            PaimonLookupJoin.registerCatalog(tEnv, paimonIndex);

            DataStream<Row> empty = env.fromCollection(
                    List.<Row>of(),
                    new RowTypeInfo(
                            new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
                            new String[] {"spec_id", "partition_key", "pk"}));
            Schema probeSchema = Schema.newBuilder()
                    .column("spec_id", DataTypes.INT())
                    .column("partition_key", DataTypes.STRING())
                    .column("pk", DataTypes.STRING())
                    .columnByExpression("proc", "PROCTIME()")
                    .build();
            tEnv.createTemporaryView("icestream_eq_deletes", tEnv.fromDataStream(empty, probeSchema));

            String sql = LookupJoinSql.lookupJoin(PaimonLookupJoin.indexFqn(paimonIndex, id));
            String plan = tEnv.sqlQuery(sql).explain();

            assertThat(plan)
                    .as("planner must pick LookupJoin against Paimon's FileStoreLookupFunction.\nPlan:\n%s", plan)
                    .contains("LookupJoin");
        }
    }
}
