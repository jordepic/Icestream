package io.github.jordepic.icestream.indexer;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Reads the live rows of a single data file as {@link Record}s, independent of any snapshot.
 *
 * <p>The caller is responsible for passing only the positional-delete and DV files that apply to
 * the given data file. Equality-delete files must not be supplied: they indicate a caller bug, as
 * icestream only applies deletes that are part of the contiguous run currently being integrated.
 */
public final class DataFileReader {

    private DataFileReader() {}

    public static CloseableIterable<Record> read(
            FileIO io, DataFile dataFile, List<DeleteFile> positionAndDvDeletes, Schema projection) {
        Schema projectionWithPos = withRowPosition(projection);
        PositionDeleteIndex deletes = loadDeletes(io, dataFile, positionAndDvDeletes);
        InputFile in = io.newInputFile(dataFile.location());
        CloseableIterable<Record> all = Parquet.read(in)
                .project(projectionWithPos)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(projectionWithPos, fileSchema))
                .build();
        return CloseableIterable.filter(
                all, record -> !deletes.isDeleted((Long) record.getField(MetadataColumns.ROW_POSITION.name())));
    }

    private static Schema withRowPosition(Schema projection) {
        List<NestedField> fields = new ArrayList<>(projection.columns());
        fields.add(MetadataColumns.ROW_POSITION);
        return new Schema(fields);
    }

    private static PositionDeleteIndex loadDeletes(FileIO io, DataFile dataFile, List<DeleteFile> deletes) {
        if (deletes.isEmpty()) {
            return PositionDeleteIndex.empty();
        }
        DeleteLoader loader = new BaseDeleteLoader(file -> io.newInputFile(file.location()));
        return loader.loadPositionDeletes(deletes, dataFile.location());
    }
}
