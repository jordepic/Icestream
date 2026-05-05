package io.github.jordepic.icestream.sparkcassandra.converter;

import io.github.jordepic.icestream.converter.DeletionVectorLoader;
import io.github.jordepic.icestream.converter.DvInfo;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * V3 strategy: emits puffin DV files via {@link WriteDvFiles}. Existing per-data-file scoped DVs
 * for touched partitions are pre-collected by {@link DeletionVectorLoader}, parallelized into a
 * pair RDD keyed by data file path, and {@code cogroup}'d with the matches RDD so each spark task
 * receives exactly the existing-DV metadata for the data files it processes — no broadcast.
 */
final class SparkDvFileStrategy implements DeleteFileStrategy {

    @Override
    public JavaRDD<TaskOutputs> writePerDataFileDeletes(
            JavaSparkContext jsc,
            Broadcast<Table> tableBroadcast,
            Table table,
            Set<PartitionKey> touchedPartitions,
            JavaPairRDD<String, PerPositionMatch> rowsToDeleteByDataFilePath) {
        DeletionVectorLoader.CollectedDvs existingDvs = DeletionVectorLoader.collect(table, touchedPartitions);
        JavaPairRDD<String, DvInfo> existingByPath = pairRddOf(jsc, existingDvs.serializableDeletesByDataFilePath());
        JavaPairRDD<String, Tuple2<Iterable<PerPositionMatch>, Iterable<DvInfo>>> cogrouped =
                rowsToDeleteByDataFilePath.cogroup(existingByPath);
        return cogrouped.mapPartitions(new WriteDvFiles(tableBroadcast));
    }

    private static <V> JavaPairRDD<String, V> pairRddOf(JavaSparkContext jsc, Map<String, V> map) {
        List<Tuple2<String, V>> tuples = map.entrySet().stream()
                .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return jsc.parallelizePairs(tuples);
    }
}
