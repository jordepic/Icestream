package io.github.jordepic.icestream.flinkpaimon.indexer;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

/**
 * One data file plus the positional-delete / DV files at seq ≤ run-max that should be applied
 * when reading it. Built on the master JVM from {@link io.github.jordepic.icestream.planner.DataFileRun}
 * and shipped to each Flink TaskManager so the per-row read can run there without re-walking
 * iceberg manifests.
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
