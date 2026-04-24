package io.github.jordepic.icestream.cassandra;

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

public final class IndexEncoding {

    private IndexEncoding() {}

    public static byte[] encodeStruct(Types.StructType type, StructLike value) {
        List<NestedField> fields = type.fields();
        if (fields.isEmpty()) {
            return new byte[0];
        }
        PartitionData record = new PartitionData(type);
        StructLike internal = new InternalRecordWrapper(type).wrap(value);
        for (int i = 0; i < fields.size(); i++) {
            record.set(i, internal.get(i, fields.get(i).type().typeId().javaClass()));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            new GenericDatumWriter<IndexedRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    public static int bucket(byte[] pkBytes, int bucketCount) {
        int hash = Hashing.murmur3_32_fixed().hashBytes(pkBytes).asInt();
        return Math.floorMod(hash, bucketCount);
    }
}
