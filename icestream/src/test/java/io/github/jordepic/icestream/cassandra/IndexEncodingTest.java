package io.github.jordepic.icestream.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

class IndexEncodingTest {

    @Test
    void encodeStruct_emptyStruct_returnsEmptyBytes() {
        Types.StructType type = Types.StructType.of();

        byte[] bytes = IndexEncoding.encodeStruct(type, new SimpleStruct());

        assertThat(bytes).isEmpty();
    }

    @Test
    void encodeStruct_isDeterministicForSameInput() {
        Types.StructType type = Types.StructType.of(
                NestedField.required(1, "id", Types.LongType.get()),
                NestedField.required(2, "name", Types.StringType.get()));

        byte[] first = IndexEncoding.encodeStruct(type, new SimpleStruct(42L, "alice"));
        byte[] second = IndexEncoding.encodeStruct(type, new SimpleStruct(42L, "alice"));

        assertThat(first).isEqualTo(second);
    }

    @Test
    void encodeStruct_fieldOrderAffectsOutput() {
        Types.StructType type = Types.StructType.of(
                NestedField.required(1, "a", Types.IntegerType.get()),
                NestedField.required(2, "b", Types.IntegerType.get()));

        byte[] left = IndexEncoding.encodeStruct(type, new SimpleStruct(1, 2));
        byte[] right = IndexEncoding.encodeStruct(type, new SimpleStruct(2, 1));

        assertThat(left).isNotEqualTo(right);
    }

    @Test
    void encodeStruct_nullFieldDiffersFromZeroField() {
        Types.StructType type = Types.StructType.of(NestedField.optional(1, "id", Types.LongType.get()));

        byte[] withNull = IndexEncoding.encodeStruct(type, new SimpleStruct((Object) null));
        byte[] withZero = IndexEncoding.encodeStruct(type, new SimpleStruct(0L));

        assertThat(withNull).isNotEqualTo(withZero);
    }

    @Test
    void bucket_isStableForSameInput() {
        byte[] input = "hello".getBytes(StandardCharsets.UTF_8);

        int first = IndexEncoding.bucket(input, 16);
        int second = IndexEncoding.bucket(input, 16);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void bucket_resultIsInRange() {
        for (int i = 0; i < 100; i++) {
            byte[] input = ("input-" + i).getBytes(StandardCharsets.UTF_8);

            int bucket = IndexEncoding.bucket(input, 8);

            assertThat(bucket).isBetween(0, 7);
        }
    }

    private static final class SimpleStruct implements StructLike {

        private final Object[] values;

        SimpleStruct(Object... values) {
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(int pos, Class<T> javaClass) {
            return (T) values[pos];
        }

        @Override
        public <T> void set(int pos, T value) {
            values[pos] = value;
        }
    }
}
