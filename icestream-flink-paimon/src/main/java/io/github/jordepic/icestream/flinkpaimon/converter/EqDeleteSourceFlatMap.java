package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.indexer.DataFileReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * Per-task body of the eq-delete probe stream: streams each {@link EqDeleteWorkItem}'s pk rows and
 * emits {@code Row(spec_id, partition_key, pk)} for the lookup join. Rows are streamed rather than
 * loaded through a deduplicating {@code StructLikeSet} because duplicate probes are harmless — the
 * positional-delete / DV writers collapse repeats into one bit.
 */
public final class EqDeleteSourceFlatMap implements FlatMapFunction<EqDeleteWorkItem, Row> {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final Schema pkSchema;
    private final Types.StructType pkStruct;
    private transient IndexEncoding.AvroByteEncoder pkEncoder;

    public EqDeleteSourceFlatMap(Table table, Schema pkSchema, Types.StructType pkStruct) {
        this.serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        this.pkSchema = pkSchema;
        this.pkStruct = pkStruct;
    }

    @Override
    public void flatMap(EqDeleteWorkItem item, Collector<Row> out) throws Exception {
        if (pkEncoder == null) {
            pkEncoder = new IndexEncoding.AvroByteEncoder(pkStruct);
        }
        String partitionHex = IndexEncoding.toHex(item.partitionBytes());
        try (CloseableIterable<Record> rows =
                DataFileReader.readEqualityDeletes(serializableTable.io(), item.deleteFile(), pkSchema)) {
            for (Record row : rows) {
                String pkHex = IndexEncoding.toHex(pkEncoder.encode(row));
                out.collect(Row.of(item.specId(), partitionHex, pkHex));
            }
        }
    }
}
