package io.github.jordepic.icestream.indexer;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Reads the live rows of a single data file as {@link Record}s, independent of any snapshot.
 *
 * <p>Dispatches by {@link FileFormat}: parquet, avro, and orc are all supported. Iceberg 1.10.1
 * has no public format-agnostic reader for data files (FormatModelRegistry lands post-1.10),
 * so we switch here manually. Replace with FormatModelRegistry once 1.11 ships.
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
        CloseableIterable<Record> all = openForFormat(dataFile.format(), in, projectionWithPos);
        return CloseableIterable.filter(
                all, record -> !deletes.isDeleted((Long) record.getField(MetadataColumns.ROW_POSITION.name())));
    }

    private static CloseableIterable<Record> openForFormat(FileFormat format, InputFile in, Schema projection) {
        return switch (format) {
            case PARQUET -> Parquet.read(in)
                    .project(projection)
                    .createReaderFunc(fs -> GenericParquetReaders.buildReader(projection, fs))
                    .build();
            case AVRO -> Avro.read(in)
                    .project(projection)
                    .createResolvingReader(PlannedDataReader::create)
                    .build();
            case ORC -> {
                // ORC's projection layer can't see metadata columns; strip _pos before
                // .project(), then pass the full schema (with _pos) to the value reader so
                // GenericOrcReader's StructReader synthesizes the row position internally.
                Schema fileProjection = TypeUtil.selectNot(projection, MetadataColumns.metadataFieldIds());
                yield ORC.read(in)
                        .project(fileProjection)
                        .createReaderFunc(typeDesc -> GenericOrcReader.buildReader(projection, typeDesc))
                        .build();
            }
            default -> throw new UnsupportedOperationException("Unsupported data file format: " + format);
        };
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
