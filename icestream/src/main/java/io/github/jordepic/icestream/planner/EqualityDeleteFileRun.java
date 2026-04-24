package io.github.jordepic.icestream.planner;

import java.util.List;
import org.apache.iceberg.DeleteFile;

/**
 * An uninterrupted sequence of added equality delete files to convert to deletion vectors.
 * @param maxSeq - the highest sequence number of the available files
 * @param files - a list of data files to convert
 */
public record EqualityDeleteFileRun(long maxSeq, List<DeleteFile> files) implements FileRun {
    @Override
    public FileKind kind() {
        return FileKind.EQ_DEL;
    }
}
