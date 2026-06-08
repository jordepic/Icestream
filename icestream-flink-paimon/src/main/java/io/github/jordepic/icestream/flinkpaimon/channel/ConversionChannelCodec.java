package io.github.jordepic.icestream.flinkpaimon.channel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;

/**
 * Java-serialization codec for the conversion channel's HTTP payloads ({@link ConversionRequest} in,
 * {@code List<TaskOutputs>} out). These carry Iceberg {@code Serializable} objects (DeleteFile etc.)
 * that aren't proto-friendly, so plain Java serialization over the wire is the simplest fit.
 */
final class ConversionChannelCodec {

    private ConversionChannelCodec() {}

    static byte[] toBytes(Serializable value) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException e) {
            throw new UncheckedIOException("channel serialize failed", e);
        }
        return bytes.toByteArray();
    }

    @SuppressWarnings("unchecked")
    static <T> T fromBytes(byte[] data, Class<T> type) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException(new IOException("channel deserialize failed", e));
        }
    }
}
