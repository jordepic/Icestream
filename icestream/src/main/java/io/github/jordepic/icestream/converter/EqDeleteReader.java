package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.cassandra.IndexEncoding;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * Executor-side: reads one eq-delete file's pk values via {@link BaseDeleteLoader}, encodes them
 * into Cassandra-lookup keys.
 *
 * <p>{@code BaseDeleteLoader.loadEqualityDeletes} is format-agnostic (parquet/avro/orc) and
 * materializes a {@link StructLikeSet} per file. Eq-deletes carry only pk columns so per-file
 * memory footprint is small.
 */
public final class EqDeleteReader implements FlatMapFunction<EqDeleteWorkItem, SerializableEqualityDelete> {

    private static final long serialVersionUID = 1L;

    private final Broadcast<Table> tableBroadcast;
    private final Schema pkSchema;
    private final Types.StructType pkStruct;
    private final int buckets;

    public EqDeleteReader(
            Broadcast<Table> tableBroadcast, Schema pkSchema, Types.StructType pkStruct, int buckets) {
        this.tableBroadcast = tableBroadcast;
        this.pkSchema = pkSchema;
        this.pkStruct = pkStruct;
        this.buckets = buckets;
    }

    @Override
    public Iterator<SerializableEqualityDelete> call(EqDeleteWorkItem item) {
        FileIO io = tableBroadcast.value().io();
        DeleteLoader loader = new BaseDeleteLoader(file -> io.newInputFile(file.location()));
        StructLikeSet rows = loader.loadEqualityDeletes(List.of(item.deleteFile()), pkSchema);
        List<SerializableEqualityDelete> keys = new ArrayList<>(rows.size());
        for (StructLike row : rows) {
            byte[] pkBytes = IndexEncoding.encodeAsAvroBytes(pkStruct, row);
            int bucket = IndexEncoding.bucket(pkBytes, buckets);
            keys.add(new SerializableEqualityDelete(item.specId(), item.partitionBytes(), bucket, pkBytes));
        }
        return keys.iterator();
    }
}
