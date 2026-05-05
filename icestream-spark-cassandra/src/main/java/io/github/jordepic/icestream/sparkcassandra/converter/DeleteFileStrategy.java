package io.github.jordepic.icestream.sparkcassandra.converter;

import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.PerPositionMatch;
import io.github.jordepic.icestream.converter.TaskOutputs;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * Format-version-specific writer for the
 * "(data file, [(pos, specId, partition)]) → per-data-file delete file" half of icestream's
 * conversion pipeline.
 *
 * <p>Two implementations:
 * <ul>
 *   <li>{@link DvFileStrategy} for V3 tables — emits Puffin DV files via {@code BaseDVFileWriter}.
 *   <li>{@link PosDeleteFileStrategy} for V2 tables — emits parquet/avro/orc position-delete files
 *       via {@code SortingPositionOnlyDeleteWriter} with {@code DeleteGranularity.FILE}.
 * </ul>
 *
 * <p>Both implementations:
 * <ul>
 *   <li>Pre-collect existing per-data-file scoped delete files in touched partitions on the driver
 *       (DVs in V3, file-scoped pos-delete files in V2), parallelize keyed by data file path, and
 *       cogroup with the matches RDD so each spark task receives only the existing-delete entries
 *       for the data files it processes.
 *   <li>On each executor, build one writer per task, fold any existing per-data-file scoped
 *       previous deletes into the new output, and emit one {@link TaskOutputs} carrying the new
 *       and rewritten delete-file lists.
 * </ul>
 *
 * <p>Picked once at the top of {@link DeleteFileCreator#create} based on
 * {@code table.operations().current().formatVersion()}. A mid-run upgrade to V3 will fail the
 * commit and the master loop replans.
 */
interface DeleteFileStrategy {

    JavaRDD<TaskOutputs> writePerDataFileDeletes(
            JavaSparkContext jsc,
            Broadcast<Table> tableBroadcast,
            Table table,
            Set<PartitionKey> touchedPartitions,
            JavaPairRDD<String, PerPositionMatch> rowsToDeleteByDataFilePath);
}
