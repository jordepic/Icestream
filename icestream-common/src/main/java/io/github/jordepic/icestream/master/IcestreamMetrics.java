package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.planner.FileKind;
import java.time.Duration;
import org.apache.iceberg.catalog.TableIdentifier;

public interface IcestreamMetrics {

    void recordRunSuccess(TableIdentifier table, FileKind kind, int fileCount, Duration elapsed);

    void recordRunFailure(
            TableIdentifier table, FileKind kind, int fileCount, Duration elapsed, Throwable cause);

    IcestreamMetrics NOOP = new IcestreamMetrics() {
        @Override
        public void recordRunSuccess(TableIdentifier table, FileKind kind, int fileCount, Duration elapsed) {}

        @Override
        public void recordRunFailure(
                TableIdentifier table, FileKind kind, int fileCount, Duration elapsed, Throwable cause) {}
    };
}
