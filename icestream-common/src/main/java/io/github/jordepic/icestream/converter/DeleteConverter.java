package io.github.jordepic.icestream.converter;

import io.github.jordepic.icestream.planner.EqualityDeleteFileRun;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Resolves an {@link EqualityDeleteFileRun} into a {@link CommitPlan} by joining the run's
 * eq-delete pk values against the secondary index, grouping matches by data file, and producing
 * the per-data-file delete files (puffin DVs for V3, parquet/avro/orc pos-delete files for V2)
 * that {@link ConversionCommitter} will commit in a single {@code RowDelta}.
 *
 * <p>Implementations: {@code SparkDeleteFileCreator} (replica-aware Cassandra join via Spark
 * Cassandra connector), {@code StreamingFlinkDeleteFileCreator} (warm indexed lookup join against
 * Paimon via Flink Table API).
 *
 * <p>Pure: returns the plan, does not commit. The caller (TableProcessor) hands it to
 * {@link ConversionCommitter}. {@code ValidationException} from a downstream commit propagates
 * unmodified.
 */
public interface DeleteConverter {

    CommitPlan create(
            TableIdentifier icebergTableId, Table table, EqualityDeleteFileRun run, IcestreamTableConfig config);
}
