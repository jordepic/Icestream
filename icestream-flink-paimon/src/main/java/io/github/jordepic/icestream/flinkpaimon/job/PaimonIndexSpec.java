package io.github.jordepic.icestream.flinkpaimon.job;

import io.github.jordepic.icestream.flinkpaimon.index.PaimonIndex;
import java.io.Serializable;
import java.util.Map;

/**
 * Serializable recipe for rebuilding the {@link PaimonIndex} on a TaskManager — the committer needs
 * it to read the index table's latest snapshot id and confirm a DATA run's Paimon commit landed
 * before advancing the watermark.
 */
public record PaimonIndexSpec(String warehouse, String database, Map<String, String> options)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public PaimonIndexSpec {
        options = Map.copyOf(options);
    }

    public PaimonIndex load() {
        return PaimonIndex.create(warehouse, database, options);
    }
}
