package io.github.jordepic.icestream.flinkpaimon.indexer;

import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.indexer.DataFileReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * Per-task body of the indexer's Flink batch job. For each {@link FileWorkItem}, opens the data
 * file via {@link DataFileReader} (applying only the run's positional-deletes and DVs), projects
 * each surviving row to the primary-key schema, and emits one {@link RowData} matching the
 * Paimon index table layout: {@code (spec_id, partition_key, pk, data_file_path, pos)}.
 *
 * <p>The iceberg {@link Table} is shipped via {@link SerializableTable} (the field is serialized
 * as part of operator construction). pk-projection, pk-struct, and the FileIO it grants are all
 * derived from that.
 */
public final class DataFileFlatMap implements FlatMapFunction<FileWorkItem, RowData> {

    private static final long serialVersionUID = 1L;

    private final SerializableTable serializableTable;
    private final Schema pkProjection;
    private final Types.StructType pkStruct;

    public DataFileFlatMap(Table table, Schema pkProjection, Types.StructType pkStruct) {
        this.serializableTable = (SerializableTable) SerializableTable.copyOf(table);
        this.pkProjection = pkProjection;
        this.pkStruct = pkStruct;
    }

    @Override
    public void flatMap(FileWorkItem item, Collector<RowData> out) throws Exception {
        String partitionHex = IndexEncoding.toHex(item.partitionBytes());
        try (CloseableIterable<Record> records =
                DataFileReader.read(serializableTable.io(), item.dataFile(), item.deletes(), pkProjection)) {
            for (Record record : records) {
                String pkHex = IndexEncoding.toHex(IndexEncoding.encodeAsAvroBytes(pkStruct, record));
                long pos = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
                out.collect(toRowData(item.specId(), partitionHex, pkHex, item.dataFilePath(), pos));
            }
        }
    }

    private static RowData toRowData(int specId, String partitionHex, String pkHex, String dataFilePath, long pos) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, specId);
        row.setField(1, StringData.fromString(partitionHex));
        row.setField(2, StringData.fromString(pkHex));
        row.setField(3, StringData.fromString(dataFilePath));
        row.setField(4, pos);
        return row;
    }
}
