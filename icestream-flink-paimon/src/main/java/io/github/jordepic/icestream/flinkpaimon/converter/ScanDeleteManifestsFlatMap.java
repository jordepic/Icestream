package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;

/**
 * Per-task body of the converter's existing-deletes load. For each {@link ManifestFile} input —
 * shipped from the master, which only enumerates {@code snapshot.deleteManifests} — opens the
 * manifest via {@link ExistingPerFileDeleteLoader#scanManifest}, filters to the run's
 * touched partitions and the format-version's content predicate, and emits one
 * {@code (data_file_path, ExistingPerFileDeletes)} per matching entry.
 *
 * <p>Output is co-partitioned downstream with the matches stream via
 * {@code keyBy(data_file_path)} so each task slot owns the existing-deletes for the data files
 * its lookup-join matches will land on. Replaces the prior pattern of master-loading a full map
 * and shipping it broadcast-style into operator state.
 */
public final class ScanDeleteManifestsFlatMap
        implements FlatMapFunction<ManifestFile, Tuple2<String, ExistingPerFileDeletes>> {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final int formatVersion;
    private final Set<PartitionKey> touchedPartitions;

    public ScanDeleteManifestsFlatMap(
            Table table, int formatVersion, Set<PartitionKey> touchedPartitions) {
        this.serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        this.formatVersion = formatVersion;
        this.touchedPartitions = touchedPartitions;
    }

    @Override
    public void flatMap(ManifestFile manifest, Collector<Tuple2<String, ExistingPerFileDeletes>> out) {
        ExistingPerFileDeleteLoader.scanManifest(
                serializableTable,
                formatVersion,
                touchedPartitions,
                manifest,
                (path, entry) -> out.collect(Tuple2.of(path, entry)));
    }
}
