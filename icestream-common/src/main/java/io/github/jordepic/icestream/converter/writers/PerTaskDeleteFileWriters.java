package io.github.jordepic.icestream.converter.writers;

import org.apache.iceberg.Table;

/**
 * The single point in the codebase where format-version dispatches to a per-task writer. Every
 * caller (Spark mapPartitions, Flink operator open()) routes through here.
 */
public final class PerTaskDeleteFileWriters {

    private PerTaskDeleteFileWriters() {}

    public static PerTaskDeleteFileWriter forTable(Table table, int formatVersion, int taskId, long taskAttempt) {
        return switch (formatVersion) {
            case 3 -> new PerTaskDvWriter(table, taskId, taskAttempt);
            case 2 -> new PerTaskPosDeleteWriter(table, taskId, taskAttempt);
            default -> throw new IllegalArgumentException(
                    "icestream only supports v2 and v3 tables; got format-version=" + formatVersion);
        };
    }
}
