package io.github.jordepic.icestream.converter.writers;

import io.github.jordepic.icestream.converter.TaskOutputs;
import java.io.IOException;

/**
 * Engine-agnostic per-task writer for per-data-file scoped delete files. One instance per compute
 * task; receives matched {@code (path, pos, specId, partitionBytes)} tuples; on close emits a
 * single {@link TaskOutputs}.
 *
 * <p>Implementations: {@link PerTaskDvWriter} (V3 puffin DVs) and {@link PerTaskPosDeleteWriter}
 * (V2 file-scoped pos-delete files). Format-version dispatch happens once, at construction via
 * {@link PerTaskDeleteFileWriters#forTable}; nothing above this seam branches on V2/V3.
 *
 * <p>Existing per-data-file deletes are registered through {@link #registerExisting} before any
 * {@link #delete} call for the same path. Both engines stream this from upstream — Flink walks
 * the operator's serialized map at {@code open()}, Spark walks the cogroup tuples — so neither
 * has to materialize the full match set in memory.
 */
public interface PerTaskDeleteFileWriter extends AutoCloseable {

    void registerExisting(String dataFilePath, ExistingPerFileDeletes existing);

    void delete(String dataFilePath, long pos, int specId, byte[] partitionBytes);

    TaskOutputs finishAndClose() throws IOException;

    @Override
    void close() throws IOException;
}
