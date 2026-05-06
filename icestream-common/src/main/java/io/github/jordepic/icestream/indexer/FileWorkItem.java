package io.github.jordepic.icestream.indexer;

import io.github.jordepic.icestream.index.IndexEncoding;
import io.github.jordepic.icestream.planner.DataFileRun;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

/**
 * One data file plus the positional-delete / DV files at seq ≤ run-max that should be applied
 * when reading it. Built on the master JVM from {@link DataFileRun} and shipped to each
 * compute task (Spark RDD partition or Flink TaskManager) so the per-row read can run there
 * without re-walking iceberg manifests.
 */
public final class FileWorkItem implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataFile dataFile;
    private final List<DeleteFile> deletes;
    private final int specId;
    private final byte[] partitionBytes;
    private final String dataFilePath;

    public FileWorkItem(
            DataFile dataFile, List<DeleteFile> deletes, int specId, byte[] partitionBytes, String dataFilePath) {
        this.dataFile = dataFile;
        this.deletes = deletes;
        this.specId = specId;
        this.partitionBytes = partitionBytes;
        this.dataFilePath = dataFilePath;
    }

    /**
     * Build the per-data-file work items for an entire {@link DataFileRun}. Driver-side: encodes
     * each data file's partition tuple via {@link IndexEncoding#encodeAsAvroBytes} and pairs it
     * with the run's positional-deletes / DVs that apply.
     */
    public static List<FileWorkItem> buildAll(Table table, DataFileRun run) {
        List<FileWorkItem> items = new ArrayList<>(run.files().size());
        for (DataFile file : run.files()) {
            PartitionSpec spec = table.specs().get(file.specId());
            byte[] partitionBytes = IndexEncoding.encodeAsAvroBytes(spec.partitionType(), file.partition());
            items.add(new FileWorkItem(file, run.deletesFor(file), file.specId(), partitionBytes, file.location()));
        }
        return items;
    }

    public DataFile dataFile() {
        return dataFile;
    }

    public List<DeleteFile> deletes() {
        return deletes;
    }

    public int specId() {
        return specId;
    }

    public byte[] partitionBytes() {
        return partitionBytes;
    }

    public String dataFilePath() {
        return dataFilePath;
    }
}
