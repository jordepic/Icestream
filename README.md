# icestream

Asynchronous secondary-index service that efficiently converts Apache Iceberg
**equality deletes** into the table's preferred row-level delete format —
**deletion vectors** for V3 tables, **positional delete files** for V2 tables —
without blocking the writer that produced them. The format is picked per-table
from `format-version`. The conversion leverages a distributed index of iceberg
data files; the index storage and compute engine are pluggable, with two
shipping backends.

## Backends

Two production-ready implementations live under one repo, sharing the planner,
master loop, conversion committer, and per-task delete-file writers via a
common module. Each backend picks index storage + compute engine differently:

| | `icestream-spark-cassandra` | `icestream-flink-paimon` |
|---|---|---|
| **Index storage** | Apache Cassandra (one CQL table per iceberg table) | Apache Paimon primary-key table on the same object store as the iceberg warehouse |
| **Compute engine** | Apache Spark | Apache Flink (DataStream + Table API) |
| **Indexer write** | `repartitionByCassandraReplica` + token-aware upserts via the Spark Cassandra connector | Flink batch sink (`FlinkSinkBuilder.forRowData`) into the Paimon PK table; Paimon writer auto-compacts |
| **Eq-delete join** | Replica-aware repartition + `joinWithCassandraTable` (per-row predicate-pushed lookup) | Flink Table API `LEFT JOIN ... FOR SYSTEM_TIME AS OF eq.proc` against the Paimon catalog — planner picks Paimon's `FileStoreLookupFunction` for per-row indexed nested-loop probes via the `.lookup` SST sidecars |
| **Delete-file write** | On Spark executors via `mapPartitions(WriteDvFiles)` / `WritePosDeleteFiles` | On Flink TaskManagers via the `WriteDeleteFilesOperator` (`BoundedOneInput`) |
| **Deployment** | Spark-on-k8s with the master pod as the long-lived driver | Master pod + a long-lived Flink session cluster (`FlinkDeployment` CR managed by `flink-kubernetes-operator`); master submits per-fileRun jobs via `RestClusterClient` |

Both backends produce identical iceberg side effects — the same `RowDelta`
shape, snapshot summaries tagged `icestream-converted=true`, and the same
crash semantics.

## Why convert eq-deletes asynchronously?

Streaming sinks emit equality deletes because they don't know where the
affected rows physically live. They write `DELETE WHERE pk = ...` and let the
reader sort it out. Producers that work this way include:

- [Apache Flink Iceberg sink](https://iceberg.apache.org/docs/latest/flink-writes/#upsert)
  in upsert mode
- [RisingWave](https://docs.risingwave.com/integrations/destinations/apache-iceberg)
  Iceberg sink
- [Databricks Iceberg Kafka Connect sink](https://github.com/databricks/iceberg-kafka-connect)
  with `iceberg.tables.upsert-mode-enabled=true`

Eq-deletes are cheap on the write path and ruinous on the read path: every
scan must join the delete files against every potentially-matching data file.
Latency and read amplification grow with eq-delete count, fan-out grows with
partition breadth, and the cost is paid by every reader on every query until
something rewrites the deletes away.

A synchronous converter (commit eq-deletes + convert to DVs in the same
transaction) would: waste indexing work on writer contention, invert the
latency trade-off the streaming writer made by emitting eq-deletes, and let
slow conversions block ingestion. icestream amortizes the index build over
time, retries conversions without penalizing the writer, and absorbs
compaction-induced churn (rewrites, dangling deletes) without coupling them
to the producer's critical path.

### Prior art

- **[Apache Amoro](https://github.com/apache/amoro)** does this as part of
  its optimizer
  ([`AbstractRewriteFilesExecutor.equalityToPosition`](https://github.com/apache/amoro/blob/master/amoro-format-iceberg/src/main/java/org/apache/amoro/optimizing/AbstractRewriteFilesExecutor.java)).
  Re-reads each affected data file every run; no persistent index, no DV
  path.
- **[Mooncake-Labs/moonlink](https://github.com/Mooncake-Labs/moonlink)**
  maintains a similar pk → position mapping but synchronously inside a
  Postgres CDC pipeline — bounded by one Postgres instance's write rate.

icestream is the asynchronous, persistent-index version of the same idea.
Each conversion is an indexed lookup against the secondary index instead of a
full data-file rescan.

## Algorithm

Per polling sweep, for each table opted in via `icestream.primary-keys`:

1. Read the watermark from table properties:
   `(icestream.last-processed-sequence, icestream.last-processed-kind)` where
   kind is `DATA` (data files) or `EQ_DEL` (equality-delete files).
   Positional-delete files / DVs are not in the walk; they're consulted only
   at data-block read time so we don't index already-deleted rows.
2. Starting from the watermark, find the longest contiguous run of files of
   a single kind, possibly spanning multiple sequence numbers. Within the
   same sequence number, eq-delete files sort before data files.
3. Process the run, one run per call:

   **Data files**: read the files at the run's max seq (applying only
   pos-deletes / DVs at seq ≤ max), project to the pk schema, and
   upsert `(spec_id, partition_key, pk) → (data_file, pos)` into the
   secondary index. Indexing _at the run's max seq_ ensures any later
   eq-delete at a higher seq sees an index entry it can convert against.

   **Eq-delete files**: read them in the compute engine, join against the
   secondary index on `(spec_id, partition_key, pk)` to resolve
   `(data_file, pos)` pairs, group by data file, and merge with any existing
   row-level delete for that file. Commit as a single `RowDelta` — on V3 the
   merged file is a DV, on V2 it's a positional-delete file.
   `validateFromSnapshot` + `validateDeletedFiles` make the commit fail
   cleanly if a concurrent compaction rewrote any targeted file; we retry on
   the next snapshot.

4. Advance the watermark to `(run.maxSeq, run.kind)` only after the work
   commits.

Eq-delete files whose schema doesn't match the declared primary keys, or
whose partitioning doesn't match the table's, are skipped — they stay in the
table and are not part of the walk.

Idempotency falls out of the design: secondary-index writes are upserts on
`(spec_id, partition_key, pk)` (Cassandra) or PK upserts on the same key
(Paimon). A crash before the watermark advances replays the same rows. A
crash after a successful eq-delete → DV commit, before the watermark
advances, lands on the next pass with the eq-delete files already gone — the
planner simply moves on.

icestream commits are identifiable by `icestream-converted=true` in the
snapshot summary.

## Architecture

```
                      ┌────────────────────────────────────┐
                      │  Iceberg REST catalog (S3/MinIO)   │
                      └────────────────────────────────────┘
                              ▲                ▲
                  loadTable() │                │ RowDelta commit
                              │                │
        ┌─────────────────────┴────────────────┴──────────────────────┐
        │  icestream master                                           │
        │                                                             │
        │   MasterLoop ──► TableProcessor (one run per call)          │
        │                  │                                          │
        │                  ├─ SnapshotPlanner    (kind-homogeneous   │
        │                  │                      runs over the walk) │
        │                  │                                          │
        │                  ├─ DataIndexer        (Spark or Flink)    │
        │                  │                                          │
        │                  └─ DeleteConverter    (Spark or Flink)    │
        │                                                             │
        └──────────────────────────────────┬──────────────────────────┘
                                           │
                       backend-specific compute + storage:
                                           │
       ┌───────────────────────────────────┴───────────────────────────────┐
       │                                                                   │
       ▼ (icestream-spark-cassandra)                  (icestream-flink-paimon) ▼
┌───────────────────────┐                              ┌────────────────────────────────┐
│  Spark cluster        │                              │  Flink session cluster (k8s    │
│  (SparkSession,       │                              │   operator-managed)            │
│   in-pod driver)      │                              │   master submits per-fileRun   │
│                       │                              │   batch jobs via REST          │
│  ┌─────────────────┐  │                              │  ┌──────────────────────────┐  │
│  │ Cassandra       │  │                              │  │ Paimon catalog           │  │
│  │ (LOCAL_QUORUM)  │  │                              │  │  (filesystem on S3/MinIO,│  │
│  │  one CQL table  │  │                              │  │   one PK table per       │  │
│  │  per iceberg    │  │                              │  │   iceberg table)         │  │
│  │  table          │  │                              │  └──────────────────────────┘  │
│  └─────────────────┘  │                              │       (data + .lookup SSTs)    │
└───────────────────────┘                              └────────────────────────────────┘
```

### Index schema

Both backends key the secondary index on `(spec_id, partition_key, pk)` plus
the index-bucket configuration. The encoding of `partition_key` and `pk` is
the same avro `StructLike` byte representation, computed in
`io.github.jordepic.icestream.index.IndexEncoding`.

**Spark + Cassandra**:

| Role             | Column                       | Notes                                                    |
|------------------|------------------------------|----------------------------------------------------------|
| Partition key    | `spec_id`                    | Iceberg data-file `PartitionSpec` id.                    |
| Partition key    | `partition_key`              | Avro-encoded `StructLike` bytes (`blob`).                |
| Partition key    | `bucket`                     | `hash(pk_cols) mod N`, salting hot iceberg partitions.   |
| Clustering key   | `pk` (`serialized_delete_condition`) | Avro-encoded pk-column values (`blob`).          |
| Value            | `data_file_path`             | Full path of the data file.                              |
| Value            | `pos`                        | Row position in data file (`bigint`).                    |

**Flink + Paimon**:

| Role             | Column                       | Notes                                                    |
|------------------|------------------------------|----------------------------------------------------------|
| Partition col    | `spec_id` (`INT`)            | Iceberg data-file `PartitionSpec` id.                    |
| Partition col    | `partition_key` (`STRING`)   | Hex-encoded avro bytes — Paimon's lookup machinery requires Comparable PK columns, and Paimon's filesystem layout renders partition values into the path string, so hex preserves byte ordering and renders cleanly. |
| Primary key      | `(spec_id, partition_key, pk)` | Paimon requires the PK to include all partition cols.  |
| Value            | `data_file_path` (`STRING`)  | Full path of the data file.                              |
| Value            | `pos` (`BIGINT`)             | Row position in data file.                               |
| Bucketing        | `bucket-key = pk`, `bucket = N` | Fixed-bucket mode (lookup join requires `bucket > 0`).|
| Compaction       | `deletion-vectors.enabled = true`, `num-sorted-run.compaction-trigger = 2` | Writer auto-compacts; DV mode triggers lookup-friendly compaction without the per-checkpoint cost of `changelog-producer = lookup`. |

### Why this design

- **Async + persistent index**: amortize indexing over time; conversions
  become indexed lookups instead of full data-file rescans.
- **Two backends sharing one core**: planner, master loop, conversion
  committer, and per-task delete-file writers (`PerTaskDvWriter`,
  `PerTaskPosDeleteWriter`) live in `icestream-common`. Backends only
  differ in how they connect compute to index storage. New backends slot
  in by implementing `IndexBackend` + `DataIndexer` + `DeleteConverter`.
- **Engine-native indexed-NL join**:
  - Spark+Cassandra: the connector pushes the partition+pk equality into a
    Cassandra lookup, so Spark's stage is a stream of indexed point-probes,
    not a shuffle-heavy join.
  - Flink+Paimon: `FOR SYSTEM_TIME AS OF` against the Paimon catalog makes
    the planner pick `LookupJoin` against `FileStoreLookupFunction`, which
    probes the per-bucket `.lookup` SST sidecars per row. (Flink's planner
    only picks `LookupJoin` when the right side is a fixed-bucket PK
    table joined on the full PK; the converter test asserts this.)

## Architectural decisions

These are the load-bearing choices the codebase commits to. Most live in
`.claude/flink_paimon_port_plan.md` with longer reasoning; quick summary:

1. **Multi-impl module split.** `icestream-common` holds engine-agnostic
   logic (planner, schema, master loop, iceberg readers, conversion
   committer, `PerTaskDvWriter`/`PerTaskPosDeleteWriter`,
   `DeleteFileCreatorSupport`, `FileWorkItem`, `EnvConfig`,
   `LifecycleHooks`). The two backends implement common interfaces
   (`IndexBackend`, `DataIndexer`, `DeleteConverter`).
2. **Hard-cut property rename**: the bucketing knob is
   `icestream.index-buckets` (was `icestream.cassandra-buckets`); both
   backends route it into their respective storage's bucketing mechanism.
3. **Hex-encoded partition_key / pk in Paimon**: Paimon's lookup machinery
   needs Comparable PK columns and renders partition values into the path
   string. Hex is byte-order-preserving and path-safe.
4. **Paimon table partitioned by `(spec_id, partition_key)`** so the lookup
   join prunes to the matching iceberg partition per probe row.
5. **Driver-collect of `TaskOutputs`, not match rows**: per-data-file
   delete files are written on TaskManagers via `WriteDeleteFilesOperator`;
   only the `TaskOutputs` (one per task slot) make it back to the master
   pod, where `DeleteFileCreatorSupport.assemblePlan` produces the
   `CommitPlan`.
6. **Flink runtime: session mode on k8s via `flink-kubernetes-operator`.**
   Application mode (one cluster per fileRun) would dominate runtime with
   cluster-startup. Per-job submission against a long-lived session cluster
   is ~1-2 s; matches the master's 10 s polling cadence.
7. **Master pod is external to the Flink cluster**, talks via REST.
   Mirrors the Spark backend's external-driver-pod shape.
8. **Greenfield deployment assumed**: no Cassandra → Paimon migration
   tooling; pick a backend and stick with it.

## Configuration

### Per-table iceberg properties (universal)

| Property                       | Required | Default    | Meaning                                                  |
|--------------------------------|----------|------------|----------------------------------------------------------|
| `icestream.primary-keys`       | yes      | —          | Comma-separated pk columns. Presence opts the table in.  |
| `icestream.index-buckets`      | no       | `1`        | `N` for the per-partition bucket spread. Cassandra: salt. Paimon: `bucket = N` (must be > 0 for lookup join). |

State written by icestream (do not edit):

| Property                                     | Meaning                                                  |
|----------------------------------------------|----------------------------------------------------------|
| `icestream.last-processed-sequence`          | Watermark sequence number.                              |
| `icestream.last-processed-kind`              | `DATA` or `EQ_DEL` — kind of last processed run.         |
| `icestream.pinned-primary-keys`              | Snapshot of pk set at last successful commit.            |
| `icestream.pinned-index-buckets`             | Snapshot of bucket count at last successful commit.      |
| `icestream.last-indexed-paimon-snapshot`     | (Flink+Paimon only) snapshot the converter's lookup join pins to. |

Pinned-* properties exist so `SnapshotPlanner` detects a configuration change
between watermark commits and fails loud rather than silently corrupting the
index.

### Spark + Cassandra master process (env vars)

Read once at startup. Iceberg catalog comes from the SparkSession via
`spark.sql.catalog.<ICESTREAM_CATALOG_NAME>.*` Spark properties.

| Variable                                  | Required | Default     |
|-------------------------------------------|----------|-------------|
| `ICESTREAM_CATALOG_NAME`                  | no       | `iceberg`   |
| `ICESTREAM_CASSANDRA_HOST`                | yes      | —           |
| `ICESTREAM_CASSANDRA_PORT`                | no       | `9042`      |
| `ICESTREAM_CASSANDRA_LOCAL_DC`            | yes      | —           |
| `ICESTREAM_KEYSPACE`                      | no       | `icestream` |
| `ICESTREAM_POLL_INTERVAL_SECONDS`         | no       | `10`        |
| `ICESTREAM_IDLE_BACKOFF_SECONDS`          | no       | `60`        |
| `ICESTREAM_MAX_CONCURRENT_TASKS`          | no       | `4`         |

Per-table-only knob: `icestream.cassandra-partitions-per-host` (Spark write
partitions per Cassandra replica, default `10`).

### Flink + Paimon master process (env vars)

The iceberg catalog is built directly via `CatalogUtil.loadCatalog`. Free-form
catalog options arrive as `ICESTREAM_ICEBERG_OPT_*` / `ICESTREAM_PAIMON_OPT_*`
env vars (key normalization: lowercase, `_` → `.`).

| Variable                                  | Required | Default     |
|-------------------------------------------|----------|-------------|
| `ICESTREAM_ICEBERG_REST_URI`              | yes      | —           |
| `ICESTREAM_ICEBERG_WAREHOUSE`             | yes      | —           |
| `ICESTREAM_ICEBERG_OPT_*`                 | no       | —           |
| `ICESTREAM_PAIMON_WAREHOUSE`              | yes      | —           |
| `ICESTREAM_PAIMON_DATABASE`               | no       | `icestream` |
| `ICESTREAM_PAIMON_OPT_*`                  | no       | —           |
| `ICESTREAM_FLINK_MODE`                    | no       | `local`     |
| `ICESTREAM_FLINK_JM_HOST`                 | no       | `localhost` |
| `ICESTREAM_FLINK_JM_PORT`                 | no       | `8081`      |
| `ICESTREAM_FLINK_PARALLELISM`             | no       | `4`         |
| `ICESTREAM_POLL_INTERVAL_SECONDS`         | no       | `10`        |
| `ICESTREAM_IDLE_BACKOFF_SECONDS`          | no       | `60`        |
| `ICESTREAM_MAX_CONCURRENT_TASKS`          | no       | `4`         |

`ICESTREAM_FLINK_MODE=local` runs an in-pod `MiniCluster` (used by IT and
dev). `remote` submits via `RestClusterClient` to a long-lived session
cluster.

## Helm charts

One chart per backend.

```
helm install icestream-spark-cassandra ./icestream-spark-cassandra/helm \
  --set image.repository=icestream/icestream-spark-cassandra-master \
  --set image.tag=1.0-SNAPSHOT \
  --set appConfig.catalog.uri=http://iceberg-rest:8181 \
  --set appConfig.catalog.warehouse=s3://warehouse/ \
  --set appConfig.cassandra.host=cassandra \
  --set appConfig.cassandra.localDc=datacenter1
```

```
helm install icestream-flink-paimon ./icestream-flink-paimon/helm \
  --set image.repository=icestream/icestream-flink-paimon-master \
  --set image.tag=1.0-SNAPSHOT \
  --set appConfig.iceberg.restUri=http://iceberg-rest:8181 \
  --set appConfig.iceberg.warehouse=s3://warehouse/ \
  --set appConfig.paimon.warehouse=s3://warehouse/paimon/ \
  --set flinkSessionCluster.enabled=true
```

The Flink+Paimon chart provisions a `FlinkDeployment` CR — the
`flink-kubernetes-operator` must already be installed cluster-wide. The
master pod is a separate `Deployment` that talks to the operator-managed JM
service via REST.

## Limitations

These are deliberate v1 simplifications, not permanent constraints.

- **No schema/bucket migration.** Changing `icestream.primary-keys` or
  `icestream.index-buckets` after the index has been populated is fatal —
  the planner's pinned-config check fails loud. Live re-bucketing is
  follow-up work.
- **REST iceberg catalog only.**
- **Only fully-convertible eq-deletes are processed.** Schema mismatches or
  unpartitioned eq-deletes in partitioned tables are skipped silently and
  stay in the table.
- **Single-process master.** Horizontal scale-out (sharding tables across
  master instances) is not implemented.

## Tests

- `icestream-common` — engine-agnostic core; tested via the backend modules
  that depend on it.
- `icestream-spark-cassandra` — 86 unit tests (testcontainers Cassandra +
  in-process Spark).
- `icestream-flink-paimon` — 7 unit tests (in-process Paimon FilesystemCatalog
  + Flink `MiniCluster`); includes `lookupJoinPlanUsesPaimonFileStoreLookupFunction`
  asserting Flink's planner picks `LookupJoin` (not a fallback hash join).
- `icestream-it` — two end-to-end docker-compose ITs:
  `IcestreamSparkCassandraEndToEndIT` and `IcestreamFlinkPaimonEndToEndIT`,
  each running Kafka + Flink upsert ingestion + the corresponding backend
  for ~60 s and asserting that every snapshot tagged
  `icestream-converted=true` has the same visible row set as its parent.

## Module layout

```
icestream-common/                  pure logic + interfaces
  planner/                         SnapshotPlanner, FileRun, *Run, FileKind, State
  schema/                          IcestreamProperties, IcestreamTableConfig, PrimaryKeySchema
  indexer/                         DataFileReader, DataIndexer, FileWorkItem
  converter/                       ConversionCommitter, CommitPlan, TaskOutputs,
                                   PerPositionMatch, EqDeleteWorkItem, DvInfo,
                                   DeletionVectorReader, DeletionVectorLoader,
                                   ExistingPosDeleteLoader, DeleteConverter,
                                   DeleteFileCreatorSupport,
                                   writers/PerTaskDvWriter, writers/PerTaskPosDeleteWriter
  master/                          MasterLoop, TableProcessor, RunProcessor,
                                   IcestreamMetrics, EnvConfig, LifecycleHooks
  index/                           IndexEncoding, IndexBackend

icestream-spark-cassandra/         spark-on-k8s + Cassandra backend
  cassandra/CassandraIndex, SaltBucket, IndexRow, ...
  indexer/SparkDataFileIndexer
  converter/SparkDeleteFileCreator, Spark{Dv,PosDelete}FileStrategy,
            WriteDvFiles, WritePosDeleteFiles, SparkEqDeleteReader
  master/SparkMain
  helm/                            Spark driver-pod chart

icestream-flink-paimon/            flink-on-k8s + Paimon backend
  index/PaimonIndex, IndexTableSchema, PaimonCatalogFactory
  flink/FlinkContext
  indexer/FlinkDataFileIndexer, DataFileFlatMap
  converter/FlinkDeleteFileCreator, EqDeleteSourceFlatMap,
            WriteDeleteFilesOperator
  master/FlinkMain
  helm/                            FlinkDeployment + master Deployment chart

icestream-it/                      end-to-end docker-compose ITs
  IcestreamSparkCassandraEndToEndIT
  IcestreamFlinkPaimonEndToEndIT
  KafkaJsonProducer
```
