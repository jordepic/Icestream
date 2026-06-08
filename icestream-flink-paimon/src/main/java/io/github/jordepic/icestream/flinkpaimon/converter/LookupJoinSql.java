package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.flinkpaimon.index.IndexTableSchema;

/**
 * Builds the Flink SQL that joins the eq-delete probe view to the Paimon index — shared by the batch
 * ({@link FlinkDeleteFileCreator}) and streaming ({@link StreamingFlinkDeleteFileCreator}) converters
 * so neither has to reach into the other for it.
 *
 * <p>The indexed lookup and the full-scan control differ only by the {@code FOR SYSTEM_TIME AS OF}
 * temporal hint (which makes the planner pick Paimon's {@code FileStoreLookupFunction} instead of a
 * regular hash join), so both come from one builder.
 */
final class LookupJoinSql {

    /** Temporary view the converters register the eq-delete probe rows under. */
    static final String EQ_DELETES_VIEW = "icestream_eq_deletes";

    private LookupJoinSql() {}

    /** Indexed point-lookup join — {@code FOR SYSTEM_TIME AS OF} → Paimon {@code FileStoreLookupFunction}. */
    static String lookupJoin(String indexFqn) {
        return build(indexFqn, true);
    }

    /** Full-scan regular join (no temporal hint) — the benchmark control that isolates the lookup win. */
    static String regularJoin(String indexFqn) {
        return build(indexFqn, false);
    }

    private static String build(String indexFqn, boolean temporal) {
        String join = temporal
                ? " LEFT JOIN " + indexFqn + " FOR SYSTEM_TIME AS OF eq.proc AS idx"
                : " LEFT JOIN " + indexFqn + " AS idx";
        return "SELECT eq.spec_id, eq.partition_key, idx." + IndexTableSchema.COL_DATA_FILE_PATH
                + ", idx." + IndexTableSchema.COL_POS
                + " FROM " + EQ_DELETES_VIEW + " AS eq"
                + join
                + " ON eq." + IndexTableSchema.COL_SPEC_ID + " = idx." + IndexTableSchema.COL_SPEC_ID
                + " AND eq." + IndexTableSchema.COL_PARTITION_KEY + " = idx." + IndexTableSchema.COL_PARTITION_KEY
                + " AND eq." + IndexTableSchema.COL_PK + " = idx." + IndexTableSchema.COL_PK
                + " WHERE idx." + IndexTableSchema.COL_DATA_FILE_PATH + " IS NOT NULL";
    }
}
