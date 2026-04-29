package io.github.jordepic.icestream.master;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Single-run dispatch contract used by {@link MasterLoop}. Implementations process at most one
 * file run per call and return whether work was applied.
 */
public interface RunProcessor {

    boolean processNextRun(TableIdentifier id, Table table);
}
