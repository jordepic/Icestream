package io.github.jordepic.icestream.converter;

import java.io.Serializable;

/**
 * Carrier through {@code mapToPair → groupByKey → mapPartitions}: one matched (data file, position)
 * plus the {@code (spec_id, partition_bytes)} that produced the match. The partition info is
 * preserved so the driver can rebuild the new DV with the correct {@code PartitionSpec} and
 * partition tuple — no need to walk data manifests for partition lookup.
 */
public final class PerPositionMatch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long pos;
    private final int specId;
    private final byte[] partitionBytes;

    public PerPositionMatch(long pos, int specId, byte[] partitionBytes) {
        this.pos = pos;
        this.specId = specId;
        this.partitionBytes = partitionBytes;
    }

    public long pos() {
        return pos;
    }

    public int specId() {
        return specId;
    }

    public byte[] partitionBytes() {
        return partitionBytes;
    }
}
