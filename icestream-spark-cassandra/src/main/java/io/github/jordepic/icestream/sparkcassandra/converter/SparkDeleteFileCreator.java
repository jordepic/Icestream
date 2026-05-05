package io.github.jordepic.icestream.sparkcassandra.converter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import io.github.jordepic.icestream.converter.CommitPlan;
import io.github.jordepic.icestream.converter.DeleteConverter;
import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import io.github.jordepic.icestream.sparkcassandra.cassandra.CassandraIndex;
import io.github.jordepic.icestream.sparkcassandra.cassandra.JoinedFileLocation;
import io.github.jordepic.icestream.sparkcassandra.cassandra.SerializableEqualityDelete;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
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
 * <p>Pipeline (executor side: {@link EqDeleteReader} → {@link DeleteFileStrategy}):
 * <pre>
 *   parallelize(eq-delete work items)
 *     flatMap(read eq-delete file via BaseDeleteLoader, encode pk + partition)
 *     repartitionByCassandraReplica
 *     joinWithCassandraTable(on the four-column lookup key)
 *     mapToPair(data_file_path → (pos, specId, partitionBytes))
 *     groupByKey
 *     mapPartitions(strategy.writer)   // emits TaskOutputs (new + rewritten delete files)
 * </pre>
 *
 * <p>Format-version is read once at the top of {@code create()} and pins the strategy for this
 * run. A mid-run upgrade (V2 → V3) lands a delete file the committer rejects and the master loop
 * replans. Existing per-data-file scoped delete files are tracked by the writer
 * (BaseDVFileWriter for V3, SortingPositionOnlyDeleteWriter for V2) and surfaced as
 * {@code TaskOutputs.rewrittenDeletes()} — no driver-side re-derivation needed. Unscoped /
 * partition-granularity pos-delete files (V2 only, rare in streaming pipelines) are left
 * alongside our output and merged by readers.
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
        DeleteFileStrategy strategy = pickStrategy(table);
        List<EqDeleteWorkItem> workItems = buildEqDeleteWorkItems(table, run);
        Set<PartitionKey> touchedPartitions = workItems.stream()
                .map(item -> new PartitionKey(item.specId(), item.partitionBytes()))
                .collect(Collectors.toSet());

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Table> tableBroadcast = jsc.broadcast(SerializableTable.copyOf(table));

        JavaPairRDD<String, PerPositionMatch> byDataFile =
                buildJoinPipeline(id, workItems, config, tableBroadcast);

        List<TaskOutputs> taskOutputs = strategy.writePerDataFileDeletes(
                        jsc, tableBroadcast, table, touchedPartitions, byDataFile)
                .collect();

        return assemblePlan(startingSnapshotId, run, taskOutputs);
    }

    private static DeleteFileStrategy pickStrategy(Table table) {
        int formatVersion =
                ((HasTableOperations) table).operations().current().formatVersion();
        return switch (formatVersion) {
            case 2 -> new SparkPosDeleteFileStrategy();
            case 3 -> new SparkDvFileStrategy();
            default -> throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        };
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

    private List<EqDeleteWorkItem> buildEqDeleteWorkItems(Table table, EqualityDeleteFileRun run) {
        List<EqDeleteWorkItem> items = new ArrayList<>(run.files().size());
        for (DeleteFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
            items.add(new EqDeleteWorkItem(file, file.specId(), partitionBytes));
        }
        return items;
    }

    private CommitPlan assemblePlan(
            long startingSnapshotId, EqualityDeleteFileRun run, List<TaskOutputs> taskOutputs) {
        List<DeleteFile> newDeletes = new ArrayList<>();
        List<DeleteFile> rewrittenDeletes = new ArrayList<>();
        CharSequenceSet seenRewrittenLocations = CharSequenceSet.empty();
        for (TaskOutputs output : taskOutputs) {
            newDeletes.addAll(output.newDeletes());
            for (DeleteFile rewritten : output.rewrittenDeletes()) {
                if (seenRewrittenLocations.add(rewritten.location())) {
                    rewrittenDeletes.add(rewritten);
                }
            }
        }
        return new CommitPlan(startingSnapshotId, new ArrayList<>(run.files()), rewrittenDeletes, newDeletes);
    }
}
