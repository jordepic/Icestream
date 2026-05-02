# icestream

Asynchronous secondary-index service that efficeintly converts Apache Iceberg
**equality deletes** into the table's preferred row-level delete format — **deletion
vectors** for V3 tables, **positional delete files** for V2 tables — without
blocking the writer that produced them. The format is picked per-table from
`format-version`. The conversion leverages a distributed index of iceberg
data files, stored in Apache Cassandra.

## Why

Streaming sinks emit equality deletes because they don't know where the affected
rows physically live. They write `DELETE WHERE pk = ...` and let the reader sort
it out. Producers that work this way include:

- [Apache Flink Iceberg sink](https://iceberg.apache.org/docs/latest/flink-writes/#upsert)
  in upsert mode
- [RisingWave](https://docs.risingwave.com/integrations/destinations/apache-iceberg)
  Iceberg sink
- [Tabular / Databricks Iceberg Kafka Connect sink](https://github.com/databricks/iceberg-kafka-connect)
  with `iceberg.tables.upsert-mode-enabled=true`

This is cheap on the write path and ruinous on the read path: every scan must
join the delete files against every potentially-matching data file. Latency and
read amplification grow with eq-delete count, fan-out grows with partition
breadth, and the cost is paid by every reader on every query until something
rewrites the deletes away.

### Prior art

A handful of systems already convert equality deletes; the common shape is to
re-scan the data files every time you want to do it.

- **[Apache Amoro](https://github.com/apache/amoro)** does this as part of its
  optimizer — see
  [`AbstractRewriteFilesExecutor.equalityToPosition`](https://github.com/apache/amoro/blob/master/amoro-format-iceberg/src/main/java/org/apache/amoro/optimizing/AbstractRewriteFilesExecutor.java).
  It re-reads each affected data file, applies the eq-delete predicates inline,
  and emits positional delete files. Functionally complete, but every run
  re-derives `(file, pos)` from scratch. No DV path.
- **[Mooncake-Labs/moonlink](https://github.com/Mooncake-Labs/moonlink)**
  maintains a similar pk-to-position mapping, but does so **synchronously**
  inside its Postgres CDC pipeline. That's a strong fit for replicating a
  single Postgres node into Iceberg, and a poor fit for anything else: the
  whole design is bounded by one Postgres instance's write rate, and the
  conversion is on the writer's critical path.

icestream is the asynchronous, persistent-index version of the same idea. The
index is built once and maintained incrementally as snapshots arrive, so each
conversion is an indexed Cassandra lookup instead of a full data-file rescan.

## Why async

A synchronous variant would mean: writer commits eq-deletes, then converts them
to DVs in the same transaction. That's a bad fit for streaming:

- **Indexing work is wasted on contention.** A failed Iceberg commit means
  re-reading the data files and re-resolving positions for the next attempt —
  and writers contend with each other and with periodic compaction.
- **Compaction tail.** Resolving an eq-delete against a wide partition can take
  significantly longer than the writer's commit cadence; making the writer wait
  inverts the latency trade-off it chose by emitting eq-deletes in the first
  place.
- **Bounded blast radius.** Async means a slow / wedged converter degrades read
  performance but never blocks ingestion.

Async lets us amortize the index build over time, retry conversions without
penalizing the writer, and absorb compaction-induced churn (rewrites, dangling
deletes) without coupling them to the producer's critical path.

## Why Cassandra

The index has to scale with the underlying Iceberg tables — one row per indexed
data-file row across many partitions, growing linearly with ingest. Two
properties of Cassandra make it the right fit, and rule out a single-node
Postgres for anything beyond a toy deployment:

- **Horizontal scale by design.** Cassandra's partitioning is the storage
  model, not a bolt-on. Adding capacity is adding nodes; there's no central
  coordinator that becomes the bottleneck once tables get big or hot. A
  large Iceberg table with hundreds of millions of pk → `(file, pos)` rows
  per partition spec is unremarkable here.
- **Partition awareness over the wire to Spark.** The
  [Spark Cassandra connector](https://github.com/apache/cassandra-spark-connector)
  knows the cluster's tokenmap, so:
  - The data-file indexer uses `repartitionByCassandraReplica` to ship each
    `(spec_id, partition_key, bucket)` group _to the replica that owns it_
    before issuing a token-aware upsert. Writes are local; cross-node
    coordination drops out of the hot path.
  - The eq-delete → DV join pushes the partition-key equality down into
    Cassandra. The driver-side Spark stage is effectively a stream of
    indexed lookups, not a shuffle-heavy join.

Choosing a system whose partition layout maps directly onto our access pattern
turns the indexer from "Spark talking to a database" into "Spark co-located
with the database it's writing to," which is what gets us the throughput.

The other plausible answer is to skip the database entirely and persist the
index as files in the same object store as the table — partitioned, sorted,
queried via a custom format and reader. That probably is the long-term shape:
Iceberg itself is moving toward first-class secondary indexes, and once that
machinery exists in-tree, doing this in Cassandra will look like an
unnecessary external dependency. For now though, the file-format, lookup, and
maintenance code does not exist; building it from scratch is a project an
order of magnitude bigger than icestream, with no operational story for
out-of-table compaction. Cassandra trades that work for a runtime dependency,
which is the right trade today.

## Algorithm

Per polling sweep, for each table opted in via `icestream.primary-keys`:

1. Read the watermark from table properties:
   `(icestream.last-processed-sequence, icestream.last-processed-kind)`. **Kind**
   is one of `DATA` (data files) or `EQ_DEL` (equality-delete files) — the two
   file categories the walk considers. Positional-delete files and deletion
   vectors are not in the walk; they're consulted only at data-block read time
   so we don't index rows that are already physically deleted.
2. Starting from the watermark, find the longest contiguous run of files of a
   single kind, possibly spanning multiple sequence numbers. Within the same
   sequence number, equality-delete files are processed before data files —
   this is what makes a sequence like `(3 DATA, 4 EQ_DEL, 4 DATA)` produce
   the runs `[DATA@3]`, `[EQ_DEL@4]`, `[DATA@4]` rather than collapsing the
   two `DATA` blocks across the eq-deletes.
3. Process the run, one run per call:

   **If they are data files**: read the files at the run's max seq (applying
   only pos-deletes / DVs at seq ≤ max), project to the pk schema, bucket
   each row, and upsert `(spec_id, partition_key, bucket, pk) → (data_file,
   pos)` into Cassandra. Indexing _at the run's max seq_ ensures that any
   later eq-delete at a higher seq sees an index entry it can convert
   against.

   **If they are equality-delete files**: read them in Spark, join to
   Cassandra on `(spec_id, partition_key, bucket, pk)` (pushed down via the
   Spark Cassandra connector) to resolve `(data_file, pos)` pairs, group by
   data file, and merge with any existing row-level delete for that file.
   Commit as a single `RowDelta` — on V3 the merged file is a DV, on V2 it's
   a positional-delete file. The commit removes the converted eq-delete
   files plus any superseded delete files and adds the merged result.
   `validateDeletedFiles` makes the commit fail cleanly if a concurrent
   compaction rewrote any targeted file; we retry on the next snapshot.

4. Advance the watermark to `(run.maxSeq, run.kind)` only after the work
   commits.

Eq-delete files whose schema doesn't match the declared primary keys, or
whose partitioning doesn't match the table's, are skipped — they stay in the
table and are not part of the walk.

Idempotency falls out of the design: Cassandra writes are
`(data_file_path, pos)` upserts; a crash before the watermark advances replays
the same rows. A crash after a successful eq-delete → DV commit, before the
watermark advances, lands on the next pass with the eq-delete files already
gone — the planner simply moves on.

icestream commits are identifiable by `icestream-converted=true` in the snapshot
summary.

## Architecture

```
                      ┌────────────────────────────────────┐
                      │  Iceberg REST catalog (S3/MinIO)   │
                      └────────────────────────────────────┘
                              ▲                ▲
                  loadTable() │                │ RowDelta commit
                              │                │ (icestream-converted=true)
                              │                │
        ┌─────────────────────┴────────────────┴──────────────────────┐
        │  icestream master                                           │
        │                                                             │
        │   MasterLoop ──► TableProcessor (one run per call)          │
        │                  │                                          │
        │                  ├─ SnapshotPlanner   (kind-homogeneous     │
        │                  │                     runs over the walk)  │
        │                  │                                          │
        │                  ├─ DataFileIndexer   ──► Spark RDD ─┐      │
        │                  │  (read, project pk, bucket)       │      │
        │                  │                                   │      │
        │                  └─ DeleteFileCreator ──► Spark SQL ─┤      │
        │                     (read eq-deletes, join Cassandra,│      │
        │                      build CommitPlan)               │      │
        │                                                      │      │
        └──────────────────────────────────────────────────────┼──────┘
                                                               │
                          replica-aware repartition by         │
                       (spec_id, partition_key, bucket)        │
                                                               ▼
                                ┌──────────────────────────────────────┐
                                │  Cassandra (LOCAL_QUORUM R/W)        │
                                │  one table per (iceberg_table,       │
                                │                 eq-delete schema)    │
                                │                                      │
                                │  PRIMARY KEY (                       │
                                │    (spec_id, partition_key, bucket), │
                                │    pk_bytes                          │
                                │  )                                   │
                                │  + data_file_path, pos               │
                                └──────────────────────────────────────┘
```

**Partition key.** `(spec_id, partition_key, bucket)`. `spec_id` scopes rows
to the Iceberg partition spec they were indexed under, so partition-spec
evolution is self-healing — eq-deletes only mark rows indexed under the same
spec. `partition_key` is the encoded `StructLike`. `bucket = hash(pk) mod N`
salts hot partitions so a single Iceberg partition's index doesn't pile up on
one Cassandra replica; `N` is `icestream.cassandra-buckets`.

**Clustering key.** The encoded pk-column bytes — this is what the eq-delete →
DV join probes, so it's the natural sort key inside a partition.

**Spark side.** The indexer parallelizes data-file reads as an RDD, then uses
the Cassandra connector's `repartitionByCassandraReplica` to route writes to
the owning replica before issuing token-aware upserts. The converter side
relies on the connector's predicate pushdown to keep the eq-delete → Cassandra
join cluster-local on the same partition columns.

## Limitations

These are deliberate v1 simplifications, not permanent constraints.

- **No schema/bucket migration.** Changing `icestream.primary-keys` or
  `icestream.cassandra-buckets` after the index has been populated is fatal —
  the master crashes loud because the index would no longer match the bucket
  function or pk projection. Live migration (re-bucket / re-key in place) is
  the obvious follow-up.
- **REST catalog only.**
- **Only fully-convertible eq-deletes are processed.** Schema mismatches or
  unpartitioned eq-deletes in partitioned tables are skipped silently and stay
  in the table.
- **Single-process master.** Horizontal scale-out (sharding tables across
  master instances) is not implemented.

## Configuration

### Per-table (Iceberg table properties)

| Property                                    | Required | Default | Meaning                                                        |
|---------------------------------------------|----------|---------|----------------------------------------------------------------|
| `icestream.primary-keys`                    | yes      | —       | Comma-separated pk column names. Presence = opt-in.            |
| `icestream.cassandra-buckets`               | no       | `1`     | `N` for `bucket = hash(pk) mod N`. Increase to spread hot pks. |
| `icestream.cassandra-partitions-per-host`   | no       | `10`    | Spark write partitions per Cassandra replica.                  |

### Master process (environment variables)

Read once at startup by `Main.Config.fromEnv()`. Connection settings are
required; scheduling knobs default to values that suit a small deployment.

| Variable                          | Required | Default     | Meaning                                                   |
|-----------------------------------|----------|-------------|-----------------------------------------------------------|
| `ICESTREAM_CATALOG_URI`           | yes      | —           | Iceberg REST catalog URI.                                 |
| `ICESTREAM_WAREHOUSE`             | yes      | —           | Warehouse location passed to the catalog.                 |
| `ICESTREAM_CASSANDRA_HOST`        | yes      | —           | Cassandra contact point.                                  |
| `ICESTREAM_CASSANDRA_PORT`        | no       | `9042`      | Cassandra native port.                                    |
| `ICESTREAM_CASSANDRA_LOCAL_DC`    | yes      | —           | Local DC for `LOCAL_QUORUM` and the Spark connector.      |
| `ICESTREAM_KEYSPACE`              | no       | `icestream` | Keyspace that holds per-table index tables.               |
| `ICESTREAM_SPARK_MASTER`          | no       | `local[*]`  | Spark master URL.                                         |
| `ICESTREAM_POLL_INTERVAL_SECONDS` | no       | `10`        | Sleep between polling sweeps that enqueue ready tables.   |
| `ICESTREAM_IDLE_BACKOFF_SECONDS`  | no       | `60`        | Cooldown before re-polling a table that had no work.      |
| `ICESTREAM_MAX_CONCURRENT_TASKS`  | no       | `4`         | Worker pool size — tables processed in parallel per tick. |

### Master state (set by icestream, do not edit)

| Property                                    | Meaning                                                   |
|---------------------------------------------|-----------------------------------------------------------|
| `icestream.last-processed-sequence`         | Watermark sequence number.                                |
| `icestream.last-processed-kind`             | `DATA` or `EQ_DEL` — the kind of the last processed run.  |
| `icestream.pinned-primary-keys`             | Snapshot of the pk set at last successful commit.         |
| `icestream.pinned-cassandra-buckets`        | Snapshot of bucket count at last successful commit.       |

The pinned-* properties exist so the planner can detect a configuration change
between watermark commits and fail-loud rather than silently corrupting the
index.
