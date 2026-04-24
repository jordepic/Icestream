package io.github.jordepic.icestream.indexer;

import static org.apache.spark.sql.functions.col;

import io.github.jordepic.icestream.cassandra.CassandraIndex;
import io.github.jordepic.icestream.cassandra.IndexEncoding;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class DataFileIndexer {

    private final SparkSession spark;
    private final CassandraIndex cassandra;

    public DataFileIndexer(SparkSession spark, CassandraIndex cassandra) {
        this.spark = spark;
        this.cassandra = cassandra;
    }

    public void index(TableIdentifier id, Table table, DataFileRun run, IcestreamTableConfig config) {
        if (run.files().isEmpty()) {
            return;
        }
        List<FileWorkItem> workItems = buildWorkItems(table, run);
        FileIO io = table.io();
        Schema pkProjection = new Schema(config.primaryKey().fields());
        Types.StructType pkStructType = Types.StructType.of(config.primaryKey().fields());
        int buckets = config.cassandraBuckets();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<FileWorkItem> workItemRdd = jsc.parallelize(workItems);
        FlatMapFunction<FileWorkItem, Row> encode = item -> encodeFile(item, io, pkProjection, pkStructType, buckets);
        JavaRDD<Row> indexRows = workItemRdd.flatMap(encode);
        Dataset<Row> ds = spark.createDataFrame(indexRows.rdd(), indexSchema());
        writeToCassandra(ds, id);
    }

    private List<FileWorkItem> buildWorkItems(Table table, DataFileRun run) {
        List<FileWorkItem> items = new ArrayList<>(run.files().size());
        for (DataFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeStruct(spec.partitionType(), file.partition());
            items.add(new FileWorkItem(file, run.deletesFor(file), file.specId(), partitionBytes, file.location()));
        }
        return items;
    }

    private static Iterator<Row> encodeFile(
            FileWorkItem item, FileIO io, Schema pkProjection, Types.StructType pkStructType, int buckets)
            throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = DataFileReader.read(io, item.dataFile, item.deletes, pkProjection)) {
            for (Record record : records) {
                byte[] pkBytes = IndexEncoding.encodeStruct(pkStructType, record);
                int bucket = IndexEncoding.bucket(pkBytes, buckets);
                long pos = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
                rows.add(RowFactory.create(item.specId, item.partitionBytes, bucket, pkBytes, item.dataFilePath, pos));
            }
        }
        return rows.iterator();
    }

    private void writeToCassandra(Dataset<Row> indexRows, TableIdentifier id) {
        indexRows.repartition(col("spec_id"), col("partition_key"), col("bucket"))
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", cassandra.keyspace())
                .option("table", cassandra.tableName(id))
                .mode(SaveMode.Append)
                .save();
    }

    private static StructType indexSchema() {
        return new StructType(new StructField[] {
            new StructField("spec_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("partition_key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("bucket", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("pk", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("data_file_path", DataTypes.StringType, false, Metadata.empty()),
            new StructField("pos", DataTypes.LongType, false, Metadata.empty())
        });
    }

    private static final class FileWorkItem implements Serializable {

        private static final long serialVersionUID = 1L;
        private final DataFile dataFile;
        private final List<DeleteFile> deletes;
        private final int specId;
        private final byte[] partitionBytes;
        private final String dataFilePath;

        FileWorkItem(DataFile dataFile, List<DeleteFile> deletes, int specId, byte[] partitionBytes, String dataFilePath) {
            this.dataFile = dataFile;
            this.deletes = deletes;
            this.specId = specId;
            this.partitionBytes = partitionBytes;
            this.dataFilePath = dataFilePath;
        }
    }
}
