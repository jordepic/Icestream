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

#### Lookup-cache tuning (Flink + Paimon)

The lookup join materializes per-bucket `.lookup` SST sidecars on the TaskManager's local disk and
caches their pages in memory. Three Paimon toggles control how much stays resident vs. gets paged
in/out (set them as index-table options):

- `lookup.cache-max-memory-size` (default `256 mb`) — in-memory page cache over the lookup files.
  Raise it to keep more of the working set hot and avoid memory↔local-disk paging.
- `lookup.cache-file-retention` (default `1 h`) — how long an *idle* lookup file stays on local disk
  before it's deleted. Raise it so a bursty/idle converter doesn't drop its warm files between runs.
- `lookup.cache-max-disk-size` (default **unlimited**) — local-disk budget for the lookup files. Left
  unlimited, files accumulate until the physical disk fills (a hard failure). Set a budget ≤ free disk
  so Paimon LRU-evicts instead; on a huge index the evicted file is then re-materialized on next probe
  (rebuilt from the data file, or downloaded if remote lookup files are enabled).

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

These are the load-bearing choices the codebase commits to.

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
  --set image.repository=icestream/icestream-flink-paimon \
  --set image.tag=1.0-SNAPSHOT \
  --set appConfig.iceberg.restUri=http://iceberg-rest:8181 \
  --set appConfig.iceberg.warehouse=s3://warehouse/ \
  --set appConfig.paimon.warehouse=s3://warehouse/paimon/
```

The chart deploys a **single icestream pod** — one process runs the polling
control loop *and* owns the converter that drives the Flink job (there is no
separate "worker" pod and no RPC hop). The `--set flinkMode=…` axis picks where
the Flink job runs:

- **`remote`** (default, production): the pod submits one standing streaming
  converter job to a Flink session cluster (`FLINK_MODE=remote`) and hosts the
  conversion **channel server** on port 8090; the cluster's TaskManagers dial
  back to the pod's `Service` for the lookup work-in/results-out channel — so the
  channel's advertised URL is its in-cluster Service DNS. The chart also
  provisions a `FlinkDeployment` CR (`flink-kubernetes-operator` must be
  installed cluster-wide). All file reads and writes run distributed on the
  cluster; only the commit is centralized in the pod. Set
  `--set flinkSessionCluster.enabled=false` to point at a pre-existing
  `FlinkDeployment` (configure `appConfig.flink.sessionCluster.*`).
- **`local`** (dev/small): a single pod running the control loop + an in-pod
  `MiniCluster` (`FLINK_MODE=local`). No session cluster, no channel server,
  in-process transport.

In `remote` mode the **image must contain the shaded job jar** it ships to the
TaskManagers (`ICESTREAM_FLINK_JARS=/app/job/icestream-cluster.jar`). Build it
with the `cluster-jar` profile active so jib stages the jar into the image:

```
mvn -pl icestream-flink-paimon -am -DskipTests -Pcluster-jar package jib:dockerBuild
```

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
- `icestream-spark-cassandra` — unit tests run against a testcontainers
  Cassandra and an in-process Spark.
- `icestream-flink-paimon` — unit tests run in-process against Paimon's
  filesystem catalog and a Flink `MiniCluster`; includes
  `lookupJoinPlanUsesPaimonFileStoreLookupFunction` asserting Flink's planner
  picks `LookupJoin` (not a fallback hash join).
- `icestream-it` — two end-to-end docker-compose ITs:
  `IcestreamSparkCassandraEndToEndIT` and `IcestreamFlinkPaimonEndToEndIT`,
  each running Kafka + Flink upsert ingestion + the corresponding backend
  for ~60 s and asserting that every snapshot tagged
  `icestream-converted=true` has the same visible row set as its parent.

## Benchmark (Flink + Paimon, distributed cluster)

`ConversionBenchmark` (in `icestream-it`, `@Tag("benchmark")`) times the warm indexed
lookup-conversion against an isolated batch (full-scan) join as the eq-delete size **D** sweeps
from 1 to 1,000,000, to find where the point-lookup (O(D·log N)) loses to the full scan (O(N)).

**Setup for the run below:**

| | |
|---|---|
| Rows (N) | 550,000,000 |
| Iceberg data files | 64 (unpartitioned, format-version 2) |
| Iceberg table size | ~10 GB parquet |
| Index buckets | 8 (`bucket-key = pk`) |
| Index data-file format | parquet + zstd (Paimon defaults) |
| Paimon index size | **~17 GB**, of which **~9.4 GB** is the `.lookup` SST sidecars (the only part the lookup pages) |
| Cluster | Flink 2.0.0 standalone — 1 JobManager + **2 TaskManagers** (2 slots each), job parallelism 4; TM heap sized to hold the per-subtask cache |
| Compute split | warm lookup distributed on the cluster (reads + bucket-keyed lookup + keyed writes across both TMs); batch-join control on a local MiniCluster |

**Lookup-cache sweep** (`lookup.cache-max-memory-size`, which is **per lookup subtask** — at
parallelism 4 that's 2 subtasks per TM, so a 1 GB setting ≈ 2 GB of cache per TM). The index uses
radical lookup compaction + remote (object-store) lookup files, so a paged-out SST is downloaded, not
rebuilt (see [Lookup-cache tuning](#lookup-cache-tuning-flink--paimon)). Warm-lookup latency at three
cache sizes vs the batch-join baseline:

```
       D        D/N        256 MB        1 GB         2 GB       batch-join*
       1     0.0000%        0.13 s       0.15 s       0.30 s      ~130 s
      10     0.0000%        0.55 s       0.51 s       0.23 s        —
     100     0.0000%        0.81 s       0.82 s       0.83 s        —
   1,000     0.0002%        1.36 s       1.13 s       1.10 s        —
  10,000     0.0018%        6.22 s       1.94 s       2.12 s        —
 100,000     0.0182%       52.31 s      10.95 s      10.96 s        —
1,000,000    0.1818%      482.60 s      63.03 s      64.52 s      ~141 s
```
\* batch-join is the cache-independent local O(N) control, measured at D=1 and 1M (it's flat in D):
~130 s and ~141 s.

**Takeaways:**
- **With an adequately-sized cache, indexed lookup wins the entire D range.** At 1 GB, lookup beats
  batch from D=1 (855×) through D=1M (63 s vs ~141 s, 2.2×) — the crossover is off the chart, because
  even D=1M is only 0.18% of N.
- **The cache saturates at ~1 GB/subtask.** 256 MB → 1 GB is huge at the paging end (D=1M: 482 s →
  63 s, 7.7×; D=100k: 52 s → 11 s), but 1 GB → 2 GB is flat (within noise). The lookup only needs its
  **hot working set** resident — bloom filters + index blocks + the touched data pages, under 1 GB per
  subtask here — not the full ~2.35 GB SST slice. **Size the cache to the hot set, not the footprint.**
- **Small D is cache-insensitive** (D≤1k touch few SSTs); the cache only matters at the big-D paging
  end, where a too-small (256 MB) cache thrashes against the 9.4 GB SST set and can hand D=1M to batch.
- **Flake-free at every cache size.** The converter's channel transport is hardened against the
  long-poll connection resets that previously restarted the standing job and lost conversions; the
  correctness invariant (positions = D) held on every point.

**Index data-file format.** The data files are parquet + zstd (Paimon defaults). The format is
independent of lookup speed — the lookup probes the separate, format-agnostic `.lookup` SSTs — but it
transforms the index on disk: zstd compresses the heavily-repeating `data_file_path`/`pk` columns ~7×
vs the earlier avro/uncompressed layout (e.g. at 55M the index shrank 5.7 GB → 1.6 GB, 40 → 8 files,
with comparable reindex time). So parquet+zstd is a strict win — far smaller index, no cost to
warm-lookup speed — and is the default.

Run it (needs a running Flink 2.0.0 standalone cluster + the shaded job jar):

```
mvn -pl icestream-flink-paimon -am -DskipTests -Pcluster-jar package
mvn -pl icestream-it test -Pbenchmark -Dtest=ConversionBenchmark \
    -Dbench.remoteCluster=true -Dbench.jmPort=8081 \
    -Dbench.flinkJars=icestream-flink-paimon/target/icestream-flink-paimon-*-cluster.jar \
    -Dbench.warehouseDir=/path/to/shared/warehouse \
    -Dbench.rows=550000000 -Dbench.files=64 -Dbench.buckets=8 -Dbench.slots=4 \
    -Dbench.lookupMemCache="1 gb" \
    -Dbench.dSweep=1,10,100,1000,10000,100000,1000000 -Dbench.batchDValues=1,1000000
```

Notes:
- `bench.batchDValues` limits the (expensive, O(N)) batch-join control to a couple of D points — it's
  flat in D, so D=1 and 1M pin the baseline — while the lookup arm still sweeps every D. Lookup
  correctness is checked against the invariant `positions = D` on every point regardless.
- `lookupMemCache` is **per lookup subtask** (2 subtasks/TM at parallelism 4), so size the TaskManager
  heap to hold it. A literal zero-/sub-page cache is degenerate (returns wrong results); the smallest
  meaningful value is ≥ a cache page, and ~1 GB/subtask saturates the hot set at this scale.
- These numbers are flake-free thanks to the hardened channel transport (the source's long-poll retries
  transient connection resets instead of failing the task and restarting the standing job).

