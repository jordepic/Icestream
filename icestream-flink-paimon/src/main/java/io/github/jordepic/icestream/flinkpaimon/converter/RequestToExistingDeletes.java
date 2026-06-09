package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.DeleteFileCreatorSupport;
import io.github.jordepic.icestream.converter.ExistingPerFileDeleteLoader;
import io.github.jordepic.icestream.converter.PartitionKey;
import io.github.jordepic.icestream.converter.writers.ExistingPerFileDeletes;
import io.github.jordepic.icestream.flinkpaimon.channel.ConversionRequest;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;

/**
 * Per-conversion existing-deletes load for the standing streaming job. For each {@link
 * ConversionRequest} it scans the snapshot's delete manifests (carried on the request) filtered to
 * the run's touched partitions, emitting one {@code (data_file_path, ExistingPerFileDeletes)} per
 * matching entry. Co-keyed downstream with the lookup matches by {@code data_file_path} so each
 * writer subtask sees the existing deletes for the data files its matches land on, and merges them
 * into the new delete file (only data files that also receive a new delete produce output).
 *
 * <p>Runs at parallelism 1 (chained to the p1 request source): the per-conversion delete-manifest
 * set is small, and keeping the scan in the source's epoch — like {@link RequestToEqDeleteFiles}
 * does for the eq-delete files — keeps the whole conversion within one checkpoint epoch. The heavier
 * eq-delete reads are still distributed by the downstream rebalance.
 */
public final class RequestToExistingDeletes
        implements FlatMapFunction<ConversionRequest, Tuple2<String, ExistingPerFileDeletes>> {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final int formatVersion;

    public RequestToExistingDeletes(Table table, int formatVersion) {
        this.serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        this.formatVersion = formatVersion;
    }

    @Override
    public void flatMap(ConversionRequest request, Collector<Tuple2<String, ExistingPerFileDeletes>> out) {
        Set<PartitionKey> touchedPartitions = DeleteFileCreatorSupport.touchedPartitions(request.workItems());
        for (ManifestFile manifest : request.deleteManifests()) {
            ExistingPerFileDeleteLoader.scanManifest(
                    serializableTable,
                    formatVersion,
                    touchedPartitions,
                    manifest,
                    (path, entry) -> out.collect(Tuple2.of(path, entry)));
        }
    }
}
