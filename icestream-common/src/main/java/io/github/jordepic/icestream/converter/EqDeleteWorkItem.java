package io.github.jordepic.icestream.converter;

import java.io.Serializable;
import org.apache.iceberg.DeleteFile;

/**
 * One eq-delete file plus its driver-precomputed {@code (spec_id, encoded partition bytes)}
 * forwarded to the executor for reading.
 */
public final class EqDeleteWorkItem implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DeleteFile deleteFile;
    private final int specId;
    private final byte[] partitionBytes;

    public EqDeleteWorkItem(DeleteFile deleteFile, int specId, byte[] partitionBytes) {
        this.deleteFile = deleteFile;
        this.specId = specId;
        this.partitionBytes = partitionBytes;
    }

    public DeleteFile deleteFile() {
        return deleteFile;
    }

    public int specId() {
        return specId;
    }

    public byte[] partitionBytes() {
        return partitionBytes;
    }
}
