package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.index.IndexEncoding;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;

/**
 * Per-task body of the converter's eq-delete probe stream. For each {@link EqDeleteWorkItem},
 * loads the eq-delete file's pk values via {@link BaseDeleteLoader} (format-agnostic — handles
 * parquet/avro/orc), encodes them, and emits one {@link Row}
 * {@code (spec_id, partition_key, pk)} per pk value.
 *
 * <p>The result stream feeds into a Flink Table API {@code LOOKUP JOIN} against the Paimon
 * index table. Eq-deletes carry only pk columns so per-file footprint is small enough for
 * in-memory materialization.
 */
public final class EqDeleteSourceFlatMap implements FlatMapFunction<EqDeleteWorkItem, Row> {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final Schema pkSchema;
    private final Types.StructType pkStruct;

    public EqDeleteSourceFlatMap(Table table, Schema pkSchema, Types.StructType pkStruct) {
        this.serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        this.pkSchema = pkSchema;
        this.pkStruct = pkStruct;
    }

    @Override
    public void flatMap(EqDeleteWorkItem item, Collector<Row> out) {
        DeleteLoader loader =
                new BaseDeleteLoader(file -> serializableTable.io().newInputFile(file.location()));
        StructLikeSet rows = loader.loadEqualityDeletes(List.of(item.deleteFile()), pkSchema);
        for (StructLike row : rows) {
            byte[] pkBytes = IndexEncoding.encodeAsAvroBytes(pkStruct, row);
            out.collect(Row.of(item.specId(), item.partitionBytes(), pkBytes));
        }
    }
}
