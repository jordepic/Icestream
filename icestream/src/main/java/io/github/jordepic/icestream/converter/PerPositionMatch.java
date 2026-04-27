package io.github.jordepic.icestream.converter;

import java.io.Serializable;

/**
 * Carrier through {@code mapToPair → groupByKey → mapPartitions}: one matched (data file, position)
 * plus the {@code (spec_id, partition_bytes)} that produced the match. The partition info is
 * preserved so the driver can rebuild the new DV with the correct {@code PartitionSpec} and
 * partition tuple — no need to walk data manifests for partition lookup.
 */
final class PerPositionMatch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long pos;
    private final int specId;
    private final byte[] partitionBytes;

    PerPositionMatch(long pos, int specId, byte[] partitionBytes) {
        this.pos = pos;
        this.specId = specId;
        this.partitionBytes = partitionBytes;
    }

    long pos() {
        return pos;
    }

    int specId() {
        return specId;
    }

    byte[] partitionBytes() {
        return partitionBytes;
    }
}
