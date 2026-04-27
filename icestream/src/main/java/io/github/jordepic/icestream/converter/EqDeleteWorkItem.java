package io.github.jordepic.icestream.converter;

import java.io.Serializable;
import org.apache.iceberg.DeleteFile;

/**
 * One eq-delete file plus its driver-precomputed {@code (spec_id, encoded partition bytes)}
 * forwarded to the executor for reading.
 */
final class EqDeleteWorkItem implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DeleteFile deleteFile;
    private final int specId;
    private final byte[] partitionBytes;

    EqDeleteWorkItem(DeleteFile deleteFile, int specId, byte[] partitionBytes) {
        this.deleteFile = deleteFile;
        this.specId = specId;
        this.partitionBytes = partitionBytes;
    }

    DeleteFile deleteFile() {
        return deleteFile;
    }

    int specId() {
        return specId;
    }

    byte[] partitionBytes() {
        return partitionBytes;
    }
}
