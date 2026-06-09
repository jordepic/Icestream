package io.github.jordepic.icestream.flinkpaimon.converter;

import io.github.jordepic.icestream.flinkpaimon.index.IndexTableSchema;

/**
 * Builds the Flink SQL that joins the eq-delete probe view to the Paimon index for the
 * {@link StreamingFlinkDeleteFileCreator}. The {@code FOR SYSTEM_TIME AS OF} temporal hint makes the
 * planner pick Paimon's {@code FileStoreLookupFunction} (an indexed point lookup) over a regular hash
 * join.
 */
final class LookupJoinSql {

    /** Temporary view the converter registers the eq-delete probe rows under. */
    static final String EQ_DELETES_VIEW = "icestream_eq_deletes";

    private LookupJoinSql() {}

    /** Indexed point-lookup join — {@code FOR SYSTEM_TIME AS OF} → Paimon {@code FileStoreLookupFunction}. */
    static String lookupJoin(String indexFqn) {
        return "SELECT eq.spec_id, eq.partition_key, idx." + IndexTableSchema.COL_DATA_FILE_PATH
                + ", idx." + IndexTableSchema.COL_POS
                + " FROM " + EQ_DELETES_VIEW + " AS eq"
                + " LEFT JOIN " + indexFqn + " FOR SYSTEM_TIME AS OF eq.proc AS idx"
                + " ON eq." + IndexTableSchema.COL_SPEC_ID + " = idx." + IndexTableSchema.COL_SPEC_ID
                + " AND eq." + IndexTableSchema.COL_PARTITION_KEY + " = idx." + IndexTableSchema.COL_PARTITION_KEY
                + " AND eq." + IndexTableSchema.COL_PK + " = idx." + IndexTableSchema.COL_PK
                + " WHERE idx." + IndexTableSchema.COL_DATA_FILE_PATH + " IS NOT NULL";
    }
}
