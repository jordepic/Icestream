package io.github.jordepic.icestream.planner;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

/**
 * An uninterrupted sequence of added data files to integrate into the Cassandra index.
 *
 * <p>{@code deletesByFilePath} is keyed by {@link DataFile#location()} (DataFile itself uses
 * identity equality, so a path key is the only stable lookup). It carries the positional-delete /
 * DV files that apply to each data file in this run: only deletes whose sequence number falls
 * within the run's seq window AND that reference a file in {@code files} are included. Deletes
 * from future snapshots are deliberately excluded — the caller must not pre-apply them, since
 * doing so would cause later equality-delete-to-DV conversions to miss rows whose positions never
 * made it into the index.
 */
public record DataFileRun(long maxSeq, List<DataFile> files, Map<String, List<DeleteFile>> deletesByFilePath)
        implements FileRun {
    @Override
    public FileKind kind() {
        return FileKind.DATA;
    }

    public List<DeleteFile> deletesFor(DataFile file) {
        return deletesByFilePath.getOrDefault(file.location(), List.of());
    }
}
