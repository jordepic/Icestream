package io.github.jordepic.icestream.sparkcassandra.converter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import io.github.jordepic.icestream.sparkcassandra.cassandra.CassandraIndex;
import io.github.jordepic.icestream.sparkcassandra.cassandra.JoinedFileLocation;
import io.github.jordepic.icestream.sparkcassandra.cassandra.SerializableEqualityDelete;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Pure planner: computes a {@link CommitPlan} that converts an {@link EqualityDeleteFileRun}'s
 * eq-delete files into per-data-file scoped delete files (puffin DVs for V3, parquet/avro/orc
 * pos-delete files for V2) by joining their pk values against the Cassandra index.
 *
 * <p>Pipeline (executor side: {@link SparkEqDeleteReader} → {@link WriteDeleteFiles}):
 * <pre>
 *   parallelize(eq-delete work items)
 *     flatMap(read eq-delete file via BaseDeleteLoader, encode pk + partition)
 *     repartitionByCassandraReplica
 *     joinWithCassandraTable(on the four-column lookup key)
 *     mapToPair(data_file_path → (pos, specId, partitionBytes))
 *     cogroup(driver-collected existing per-data-file deletes pair-RDD)
 *     mapPartitions(WriteDeleteFiles)   // emits TaskOutputs (new + rewritten delete files)
 * </pre>
 *
 * <p>Format-version is read once at the top of {@code create()}, drives the existing-deletes
 * loader and is passed to the writer flatmap; everywhere below this method is V2/V3-blind. A
 * mid-run upgrade lands a delete file the committer rejects and the master loop replans.
 *
 * <p>No internal retry. {@code ValidationException} from {@link ConversionCommitter} propagates;
 * the master loop re-plans against the new snapshot.
 */
public final class SparkDeleteFileCreator implements DeleteConverter {

    private static final ColumnSelector PARTITION_KEY_COLUMNS = someColumns("spec_id", "partition_key", "bucket");
    private static final ColumnSelector LOOKUP_KEY_COLUMNS =
            someColumns("spec_id", "partition_key", "bucket", "serialized_delete_condition");
    private static final ColumnSelector PROJECTED_COLUMNS = someColumns("data_file_path", "pos");

    private final SparkSession spark;
    private final CassandraIndex cassandra;

    public SparkDeleteFileCreator(SparkSession spark, CassandraIndex cassandra) {
        this.spark = spark;
        this.cassandra = cassandra;
    }

    @Override
    public CommitPlan create(TableIdentifier id, Table table, EqualityDeleteFileRun run, IcestreamTableConfig config) {
        long startingSnapshotId = table.currentSnapshot().snapshotId();
        if (run.files().isEmpty()) {
            return new CommitPlan(startingSnapshotId, List.of(), List.of(), List.of());
        }
        int formatVersion = DeleteFileCreatorSupport.requireSupportedFormatVersion(table);
        List<EqDeleteWorkItem> workItems = DeleteFileCreatorSupport.buildWorkItems(table, run);
        Set<PartitionKey> touchedPartitions = DeleteFileCreatorSupport.touchedPartitions(workItems);
        Map<String, ExistingPerFileDeletes> existingByDataFile =
                ExistingPerFileDeleteLoader.collect(table, formatVersion, touchedPartitions);

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Table> tableBroadcast = jsc.broadcast(SerializableTable.copyOf(table));

        JavaPairRDD<String, PerPositionMatch> byDataFile =
                buildJoinPipeline(id, workItems, config, tableBroadcast);
        JavaPairRDD<String, ExistingPerFileDeletes> existingByPath = pairRddOf(jsc, existingByDataFile);

        List<TaskOutputs> taskOutputs = byDataFile
                .cogroup(existingByPath)
                .mapPartitions(new WriteDeleteFiles(tableBroadcast, formatVersion))
                .collect();

        return DeleteFileCreatorSupport.assemblePlan(startingSnapshotId, run, taskOutputs);
    }

    JavaPairRDD<String, PerPositionMatch> buildJoinPipeline(
            TableIdentifier id,
            List<EqDeleteWorkItem> workItems,
            IcestreamTableConfig config,
            Broadcast<Table> tableBroadcast) {
        Schema pkSchema = new Schema(config.primaryKey().fields());
        Types.StructType pkStruct = Types.StructType.of(config.primaryKey().fields());
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<SerializableEqualityDelete> pkKeys = jsc.parallelize(workItems)
                .flatMap(new SparkEqDeleteReader(tableBroadcast, pkSchema, pkStruct, config.indexBuckets()));

        JavaRDD<SerializableEqualityDelete> repartitionedByCassandraReplica = javaFunctions(pkKeys)
                .repartitionByCassandraReplica(
                        cassandra.keyspace(),
                        cassandra.tableName(id),
                        config.cassandraPartitionsPerHost(),
                        PARTITION_KEY_COLUMNS,
                        mapToRow(SerializableEqualityDelete.class));

        CassandraJavaPairRDD<SerializableEqualityDelete, JoinedFileLocation> joined =
                javaFunctions(repartitionedByCassandraReplica)
                        .joinWithCassandraTable(
                                cassandra.keyspace(),
                                cassandra.tableName(id),
                                PROJECTED_COLUMNS,
                                LOOKUP_KEY_COLUMNS,
                                mapRowTo(JoinedFileLocation.class),
                                mapToRow(SerializableEqualityDelete.class));

        return joined.mapToPair(
                (PairFunction<Tuple2<SerializableEqualityDelete, JoinedFileLocation>, String, PerPositionMatch>)
                        t -> new Tuple2<>(
                                t._2.getDataFilePath(),
                                new PerPositionMatch(t._2.getPos(), t._1.getSpecId(), t._1.getPartitionKey())));
    }

    private static JavaPairRDD<String, ExistingPerFileDeletes> pairRddOf(
            JavaSparkContext jsc, Map<String, ExistingPerFileDeletes> map) {
        List<Tuple2<String, ExistingPerFileDeletes>> tuples = map.entrySet().stream()
                .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return jsc.parallelizePairs(tuples);
    }
}
