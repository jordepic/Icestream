package io.github.jordepic.icestream.sparkcassandra.converter;

import io.github.jordepic.icestream.converter.ExistingPosDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * V2 strategy: emits parquet/avro/orc per-data-file scoped position-delete files via
 * {@link WritePosDeleteFiles}. Existing FILE-scoped pos-delete files in touched partitions are
 * pre-collected by {@link ExistingPosDeleteLoader}, flattened into a pair RDD keyed by data
 * file path, and {@code cogroup}'d with the matches RDD so each spark task receives exactly the
 * existing pos-delete files for the data files it processes — no broadcast.
 */
final class SparkPosDeleteFileStrategy implements DeleteFileStrategy {

    @Override
    public JavaRDD<TaskOutputs> writePerDataFileDeletes(
            JavaSparkContext jsc,
            Broadcast<Table> tableBroadcast,
            Table table,
            Set<PartitionKey> touchedPartitions,
            JavaPairRDD<String, PerPositionMatch> rowsToDeleteByDataFilePath) {
        Map<String, List<DeleteFile>> existingByDataFile = ExistingPosDeleteLoader.collect(table, touchedPartitions);
        JavaPairRDD<String, DeleteFile> existingByPath = flatten(jsc, existingByDataFile);
        JavaPairRDD<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DeleteFile>>> cogrouped =
                rowsToDeleteByDataFilePath.cogroup(existingByPath);
        return cogrouped.mapPartitions(new WritePosDeleteFiles(tableBroadcast));
    }

    private static JavaPairRDD<String, DeleteFile> flatten(
            JavaSparkContext jsc, Map<String, List<DeleteFile>> existingByDataFile) {
        List<Tuple2<String, DeleteFile>> tuples = new ArrayList<>();
        existingByDataFile.forEach((path, files) -> files.forEach(f -> tuples.add(new Tuple2<>(path, f))));
        return jsc.parallelizePairs(tuples);
    }
}
