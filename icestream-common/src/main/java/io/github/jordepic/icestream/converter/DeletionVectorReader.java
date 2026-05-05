package io.github.jordepic.icestream.converter;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Executor-side {@link Function} for {@code BaseDVFileWriter}'s previous-deletes loader.
 *
 * <p>Given a data file path, looks up its puffin DV in the broadcast {@link DvInfo} map, reads the
 * puffin bytes by {@code (offset, size)}, reconstructs a minimal {@code DeleteFile} for {@link
 * PositionDeleteIndex#deserialize}. Returns {@code null} when the data file has no existing DV
 * (no merge needed). Mirrors {@code ConvertEqualityDeleteFilesSparkAction.WriteDVs.loadExistingDv}.
 */
public final class DeletionVectorReader implements Function<String, PositionDeleteIndex>, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, DvInfo> dvsByDataFilePath;
    private final SerializableFileIoSupplier ioSupplier;

    public DeletionVectorReader(Map<String, DvInfo> dvsByDataFilePath, SerializableFileIoSupplier ioSupplier) {
        this.dvsByDataFilePath = dvsByDataFilePath;
        this.ioSupplier = ioSupplier;
    }

    @Override
    public PositionDeleteIndex apply(String dataFilePath) {
        DvInfo info = dvsByDataFilePath.get(dataFilePath);
        if (info == null) {
            return null;
        }
        try {
            InputFile inputFile = ioSupplier.io().newInputFile(info.location());
            byte[] bytes = new byte[Math.toIntExact(info.contentSizeInBytes())];
            try (SeekableInputStream stream = inputFile.newStream()) {
                stream.seek(info.contentOffset());
                IOUtil.readFully(stream, bytes, 0, bytes.length);
            }
            DeleteFile dvMetadata = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                    .ofPositionDeletes()
                    .withFormat(FileFormat.PUFFIN)
                    .withPath(info.location())
                    .withFileSizeInBytes(0)
                    .withRecordCount(info.recordCount())
                    .withContentOffset(info.contentOffset())
                    .withContentSizeInBytes(info.contentSizeInBytes())
                    .withReferencedDataFile(info.referencedDataFile())
                    .build();
            return PositionDeleteIndex.deserialize(bytes, dvMetadata);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read existing DV at " + info.location(), e);
        }
    }

    @FunctionalInterface
    public interface SerializableFileIoSupplier extends Serializable {
        FileIO io();
    }
}
