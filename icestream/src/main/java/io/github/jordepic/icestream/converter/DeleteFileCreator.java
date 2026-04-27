package io.github.jordepic.icestream.converter;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import com.datastax.spark.connector.ColumnSelector;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.cassandra.IndexEncoding;
import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
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
 * eq-delete files into puffin DVs by joining their pk values against the Cassandra index.
 *
 * <p>Pipeline (executor side: {@link EqDeleteReader} → {@link WriteDvs}):
 * <pre>
 *   parallelize(eq-delete work items)
 *     flatMap(read eq-delete file via BaseDeleteLoader, encode pk + partition)
 *     repartitionByCassandraReplica
 *     joinWithCassandraTable(on the four-column lookup key)
 *     mapToPair(data_file_path → (pos, specId, partitionBytes))
 *     groupByKey
 *     mapPartitions(write puffin DV per data file via BaseDVFileWriter)
 * </pre>
 *
 * <p>{@link WriteDvs} passes the real {@code (specId, partition)} to {@code BaseDVFileWriter.delete}
 * (decoded from the avro bytes that traveled through the pipeline) so each emitted {@code
 * DeleteFile} already carries the correct partition tuple — the driver does not need to rebuild.
 *
 * <p>Existing DVs are pre-collected with a partition prune: only DVs whose {@code (specId,
 * partition)} matches a touched partition in this run are loaded.
 *
 * <p>No internal retry. {@code ValidationException} from {@link ConversionCommitter} propagates;
 * the master loop re-plans against the new snapshot.
 */
public final class DeleteFileCreator {

    private static final ColumnSelector PARTITION_KEY_COLUMNS = someColumns("spec_id", "partition_key", "bucket");
    private static final ColumnSelector LOOKUP_KEY_COLUMNS =
            someColumns("spec_id", "partition_key", "bucket", "serialized_delete_condition");
    private static final ColumnSelector PROJECTED_COLUMNS = someColumns("data_file_path", "pos");

    private final SparkSession spark;
    private final CassandraIndex cassandra;

    public DeleteFileCreator(SparkSession spark, CassandraIndex cassandra) {
        this.spark = spark;
        this.cassandra = cassandra;
    }

    public CommitPlan create(TableIdentifier id, Table table, EqualityDeleteFileRun run, IcestreamTableConfig config) {
        long startingSnapshotId = table.currentSnapshot().snapshotId();
        if (run.files().isEmpty()) {
            return new CommitPlan(startingSnapshotId, List.of(), List.of(), List.of());
        }
        List<EqDeleteWorkItem> workItems = buildEqDeleteWorkItems(table, run);
        Set<PartitionKey> touchedPartitions = workItems.stream()
                .map(item -> new PartitionKey(item.specId(), item.partitionBytes()))
                .collect(Collectors.toSet());
        DeletionVectorLoader.CollectedDvs existingDvs = DeletionVectorLoader.collect(table, touchedPartitions);

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Table> tableBroadcast = jsc.broadcast(SerializableTable.copyOf(table));
        Broadcast<Map<String, DvInfo>> dvInfoBroadcast = jsc.broadcast(existingDvs.serializableDeletes());

        List<DeleteFile> newDvs =
                buildDvWritePipeline(id, workItems, config, tableBroadcast, dvInfoBroadcast).collect();

        return assemblePlan(startingSnapshotId, run, existingDvs.deletes(), newDvs);
    }

    JavaRDD<DeleteFile> buildDvWritePipeline(
            TableIdentifier id,
            List<EqDeleteWorkItem> workItems,
            IcestreamTableConfig config,
            Broadcast<Table> tableBroadcast,
            Broadcast<Map<String, DvInfo>> dvInfoBroadcast) {
        Schema pkSchema = new Schema(config.primaryKey().fields());
        Types.StructType pkStruct = Types.StructType.of(config.primaryKey().fields());
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<SerializableEqualityDelete> pkKeys = jsc.parallelize(workItems)
                .flatMap(new EqDeleteReader(tableBroadcast, pkSchema, pkStruct, config.cassandraBuckets()));

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

        JavaPairRDD<String, PerPositionMatch> byDataFile = joined.mapToPair(
                (PairFunction<Tuple2<SerializableEqualityDelete, JoinedFileLocation>, String, PerPositionMatch>)
                        t -> new Tuple2<>(
                                t._2.getDataFilePath(),
                                new PerPositionMatch(t._2.getPos(), t._1.getSpecId(), t._1.getPartitionKey())));

        return byDataFile.groupByKey().mapPartitions(new WriteDvs(tableBroadcast, dvInfoBroadcast));
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
            long startingSnapshotId,
            EqualityDeleteFileRun run,
            Map<String, DeleteFile> existingDvsByDataFile,
            List<DeleteFile> newDvs) {
        Set<String> seen = new HashSet<>();
        List<DeleteFile> existingToRemove = new ArrayList<>();
        for (DeleteFile newDv : newDvs) {
            String referenced = newDv.referencedDataFile().toString();
            DeleteFile existing = existingDvsByDataFile.get(referenced);
            if (existing != null && seen.add(referenced)) {
                existingToRemove.add(existing);
            }
        }
        return new CommitPlan(startingSnapshotId, new ArrayList<>(run.files()), existingToRemove, newDvs);
    }
}
