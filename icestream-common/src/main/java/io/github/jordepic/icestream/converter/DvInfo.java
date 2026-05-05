package io.github.jordepic.icestream.converter;

import java.io.Serializable;

/**
 * Broadcast-friendly view of an existing puffin DV: just enough to read bytes back from object
 * storage and reconstruct a {@code DeleteFile} on the executor for {@code
 * PositionDeleteIndex.deserialize}. Mirrors the {@code DvInfo} POJO used by iceberg's
 * {@code ConvertEqualityDeleteFilesSparkAction}.
 */
public final class DvInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String location;
    private final long contentOffset;
    private final long contentSizeInBytes;
    private final long recordCount;
    private final String referencedDataFile;

    public DvInfo(
            String location, long contentOffset, long contentSizeInBytes, long recordCount, String referencedDataFile) {
        this.location = location;
        this.contentOffset = contentOffset;
        this.contentSizeInBytes = contentSizeInBytes;
        this.recordCount = recordCount;
        this.referencedDataFile = referencedDataFile;
    }

    public String location() {
        return location;
    }

    public long contentOffset() {
        return contentOffset;
    }

    public long contentSizeInBytes() {
        return contentSizeInBytes;
    }

    public long recordCount() {
        return recordCount;
    }

    public String referencedDataFile() {
        return referencedDataFile;
    }
}
