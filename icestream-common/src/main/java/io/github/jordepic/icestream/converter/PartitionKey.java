package io.github.jordepic.icestream.converter;

import java.io.Serializable;
import java.util.Arrays;

/** Composite key {@code (spec_id, encoded_partition_bytes)} used for the driver-side partition lookup. */
public final class PartitionKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int specId;
    private final byte[] partitionBytes;

    public PartitionKey(int specId, byte[] partitionBytes) {
        this.specId = specId;
        this.partitionBytes = partitionBytes;
    }

    public int specId() {
        return specId;
    }

    public byte[] partitionBytes() {
        return partitionBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionKey other)) {
            return false;
        }
        return specId == other.specId && Arrays.equals(partitionBytes, other.partitionBytes);
    }

    @Override
    public int hashCode() {
        return 31 * specId + Arrays.hashCode(partitionBytes);
    }
}
