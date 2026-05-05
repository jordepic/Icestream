package io.github.jordepic.icestream.flinkpaimon.indexer;

import io.github.jordepic.icestream.flinkpaimon.flink.FlinkContext;
import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.indexer.DataIndexer;
import io.github.jordepic.icestream.planner.DataFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.FileStoreTable;

/**
 * {@link DataIndexer} that runs a Flink batch job per {@link DataFileRun}: parallelizes the run's
 * file work items, reads each file via {@code DataFileReader}, encodes pk + partition bytes, and
 * sinks {@code (spec_id, partition_key, pk, data_file_path, pos)} rows into the Paimon index
 * table via {@link FlinkSinkBuilder#forRowData}.
 *
 * <p>Per-run lifecycle: build {@link FileWorkItem}s on the master JVM, build a fresh batch
 * {@code StreamExecutionEnvironment}, register source + flatMap + sink, call
 * {@code env.execute()} and block until the job completes. Re-running the same run is idempotent
 * because Paimon writes are PK upserts on {@code (spec_id, partition_key, pk)}.
 */
public final class FlinkDataFileIndexer implements DataIndexer {

    private final FlinkContext flink;
    private final PaimonIndex paimonIndex;

    public FlinkDataFileIndexer(FlinkContext flink, PaimonIndex paimonIndex) {
        this.flink = flink;
        this.paimonIndex = paimonIndex;
    }

    @Override
    public void index(TableIdentifier id, Table table, DataFileRun run, IcestreamTableConfig config) {
        if (run.files().isEmpty()) {
            return;
        }
        List<FileWorkItem> workItems = buildWorkItems(table, run);
        Schema pkProjection = new Schema(config.primaryKey().fields());
        Types.StructType pkStruct = Types.StructType.of(config.primaryKey().fields());
        FileStoreTable indexTable = (FileStoreTable) paimonIndex.load(id);
        RowType flinkRowType = LogicalTypeConversion.toLogicalType(indexTable.rowType());
        TypeInformation<RowData> rowDataTypeInfo = InternalTypeInfo.of(flinkRowType);

        StreamExecutionEnvironment env = flink.newBatchEnv();
        DataStreamSource<FileWorkItem> source =
                env.fromCollection(workItems, TypeInformation.of(FileWorkItem.class));
        DataStream<RowData> rows =
                source.flatMap(new DataFileFlatMap(table, pkProjection, pkStruct))
                        .returns(rowDataTypeInfo);
        new FlinkSinkBuilder(indexTable).forRowData(rows).build();

        try {
            env.execute("icestream-indexer-" + id + "-" + run.maxSeq());
        } catch (Exception e) {
            throw new RuntimeException("Flink indexer job failed for table " + id + " maxSeq " + run.maxSeq(), e);
        }
    }

    private static List<FileWorkItem> buildWorkItems(Table table, DataFileRun run) {
        List<FileWorkItem> items = new ArrayList<>(run.files().size());
        for (DataFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
            items.add(new FileWorkItem(file, run.deletesFor(file), file.specId(), partitionBytes, file.location()));
        }
        return items;
    }
}
