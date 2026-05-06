package io.github.jordepic.icestream.sparkcassandra.indexer;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.someColumns;

import com.datastax.spark.connector.ColumnSelector;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.indexer.DataFileReader;
import io.github.jordepic.icestream.indexer.DataIndexer;
import io.github.jordepic.icestream.indexer.FileWorkItem;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import io.github.jordepic.icestream.sparkcassandra.cassandra.CassandraIndex;
import io.github.jordepic.icestream.sparkcassandra.cassandra.IndexRow;
import io.github.jordepic.icestream.sparkcassandra.cassandra.SaltBucket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

public final class SparkDataFileIndexer implements DataIndexer {

    private static final ColumnSelector PARTITION_KEY_COLUMNS =
            someColumns("spec_id", "partition_key", "bucket");

    private final SparkSession spark;
    private final CassandraIndex cassandra;

    public SparkDataFileIndexer(SparkSession spark, CassandraIndex cassandra) {
        this.spark = spark;
        this.cassandra = cassandra;
    }

    @Override
    public void index(TableIdentifier id, Table table, DataFileRun run, IcestreamTableConfig config) {
        if (run.files().isEmpty()) {
            return;
        }
        List<FileWorkItem> workItems = FileWorkItem.buildAll(table, run);
        FileIO io = table.io();
        Schema pkProjection = new Schema(config.primaryKey().fields());
        Types.StructType pkStructType = Types.StructType.of(config.primaryKey().fields());
        int buckets = config.indexBuckets();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<FileWorkItem> workItemRdd = jsc.parallelize(workItems);
        FlatMapFunction<FileWorkItem, IndexRow> encode =
                item -> encodeFile(item, io, pkProjection, pkStructType, buckets);
        JavaRDD<IndexRow> indexRows = workItemRdd.flatMap(encode);

        JavaRDD<IndexRow> replicaRouted = javaFunctions(indexRows)
                .repartitionByCassandraReplica(
                        cassandra.keyspace(),
                        cassandra.tableName(id),
                        config.cassandraPartitionsPerHost(),
                        PARTITION_KEY_COLUMNS,
                        mapToRow(IndexRow.class));

        javaFunctions(replicaRouted)
                .writerBuilder(cassandra.keyspace(), cassandra.tableName(id), mapToRow(IndexRow.class))
                .saveToCassandra();
    }

    private static Iterator<IndexRow> encodeFile(
            FileWorkItem item, FileIO io, Schema pkProjection, Types.StructType pkStructType, int buckets)
            throws Exception {
        List<IndexRow> rows = new ArrayList<>();
        try (CloseableIterable<Record> records =
                DataFileReader.read(io, item.dataFile(), item.deletes(), pkProjection)) {
            for (Record record : records) {
                byte[] pkBytes = IndexEncoding.encodeAsAvroBytes(pkStructType, record);
                int bucket = SaltBucket.bucket(pkBytes, buckets);
                long pos = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
                rows.add(new IndexRow(
                        item.specId(), item.partitionBytes(), bucket, pkBytes, item.dataFilePath(), pos));
            }
        }
        return rows.iterator();
    }
}
