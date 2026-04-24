# IcestreamAsync

Async secondary index on a primary key for Iceberg v3 tables. Used to convert equality
deletes to deletion vectors (DVs).

## Scope

- Iceberg v3 tables only.
- REST catalogs only (initially).
- Tables opt in via `icestream.primary-keys = "col1,col2,..."`.
- Index bucketing via `icestream.cassandra-buckets = N`.

## Master

Long-lived Java process. Single `SparkSession` (FAIR scheduler) shared across work,
modeled on `iceberg-spark-maintainerv2`.

One polling loop, one table at a time. Global interval: **10s**.

Per iteration:
1. Traverse catalog namespaces recursively.
2. For each table with `icestream.primary-keys` set:
   - If not v3 → **fail loud**.
   - Process the table (below).

## Per-table processing

State (two table properties):
- `icestream.last-processed-sequence` (long, default `0`)
- `icestream.last-processed-fileKind` (`DEL` | `DATA`, default `DATA`)

Together they form the last-processed position in the walk order described below.

1. Load current snapshot. Collect data files + **equality-delete files only** whose
   walk key `(data_sequence_number, fileKind)` is strictly greater than
   `(last-processed-sequence, last-processed-fileKind)`. Positional-delete files and DVs
   are not part of the walk; they're consulted only at data-block read time.
2. Walk files in strict order using a synthetic key:
   - eq-delete file → `(seq, DEL)`
   - data file → `(seq, DATA)`
   - Ordering: `(N, DEL) < (N, DATA) < (N+1, DEL) < (N+1, DATA)`. This forces
     deletes-before-data at the same seq.
3. Group into fileRuns: each block is a maximal run of the same fileKind in walk order,
   regardless of seq. A run of data files at seqs 3,4,5 followed by deletes at seqs
   6,7,8 yields two fileRuns: `[data 3-5]`, `[deletes 6-8]`.
4. Process fileRuns in order:
   - **delete block**: attempt eq-delete → DV conversion (see below).
   - **data block**: integrate into Cassandra index.
5. After each successful block commit, set state to `(block's max seq, block's fileKind)`.

First-sight bootstrap: state defaults to `(0, DATA)` → full backfill over all currently
live files.

### Consistency

At-least-once into Cassandra. Upserts on `(data_file_path, pos)` are idempotent.

- Crash after Cassandra write, before property update → block reprocessed, upserts
  no-op.
- Crash after DV commit, before property update → next pass sees the eq delete is
  gone and treats it as processed.

### Fatal (no auto-recovery)

Between `last-processed-sequence` and current snapshot, abort with error if:
- `icestream.primary-keys` changed.
- `icestream.cassandra-buckets` changed.
- Table is not v3.

Operator intervention is required to resume.

## Index (Cassandra)

One table per `(iceberg_table, eq_delete_schema)`. Name: `tableIdentifier.toString()`
(+ schema disambiguator when needed). Master creates the table if absent.

Schema:

| Role             | Column              | Notes                                                  |
|------------------|---------------------|--------------------------------------------------------|
| Partition key    | `spec_id`           | Iceberg data-file `PartitionSpec` id.                  |
| Partition key    | `partition_key`     | Encoded partition values (Iceberg `StructLike` bytes). |
| Partition key    | `bucket`            | `hash(pk_cols) mod N`, where `N = cassandra-buckets`.  |
| Clustering key   | `pk`                | Encoded pk-column values (binary, fixed order).        |
| Value            | `data_file_path`    | Full path/URI of the data file. Needed by eq-delete→DV conversion to locate the DV target. |
| Value            | `pos`               | Row position in data file (int64).                     |

Cassandra config: single shared keyspace/contact-points config for all index tables;
Cassandra defaults for replication; `LOCAL_QUORUM` reads/writes.

### Data-block integration

Read the data files **at the table state corresponding to the block's highest seq
`M`** — i.e. apply only delete files with `data_sequence_number ≤ M`, and only
positional deletes + DVs, not eq deletes.

Why that state:
- Eq deletes at seq > `M` do not yet exist at this point-in-time view, so they can't
  be applied — which is what we want. If we applied them here, our later eq → DV pass
  would find nothing in Cassandra and would remove the eq-delete file with no DV to
  replace it.
- Eq deletes at seq ≤ `M` are never applied by us regardless — convertible ones were
  either converted-and-gone or will be processed in a later block, and unconvertible
  ones are none of our business. Excluding all eq deletes at read time keeps the index
  complete for our later passes.
- Positional deletes / DVs at seq ≤ `M` reflect rows already physically deleted at that
  point; applying them avoids indexing rows that are known-dead.
- Manual pk-deduplication in Spark is incorrect: two data files at the same seq may
  carry the same pk where one row is killed by a positional delete in one of them.
  Iceberg scan semantics resolve this correctly.

Upsert via the Spark-Cassandra connector (default write path) to maximize routing
pushdown.

## Eq-delete → DV conversion

Convertible iff:
- Schema matches `icestream.primary-keys` exactly.
- Delete file is partitioned (in a partitioned table) or unpartitioned (in an
  unpartitioned table).

Otherwise ignored.

### Flow

1. Spark reads eq-delete files in the block.
2. Join to Cassandra on partition key (`spec_id`, `partition_key`, `bucket`, `pk`),
   pushed down via the connector. Spec-id equality ensures eq deletes apply only to
   data indexed under the same partition spec — this makes partition-spec evolution a
   non-fatal, self-healing case.
3. Collect `(data_file_path, pos)` pairs.
4. Shuffle by `data_file_path`. For each, merge with the data file's existing DV (if
   any).
5. Write DV puffin files on executors.
6. Commit on the driver as a single `RowDelta`:
   - Remove the converted eq-delete files.
   - Remove any existing DVs that were merged into the new DVs.
   - Add the new (possibly merged) DVs.
   - Use `validateDeletedFiles` / `validateFromSnapshot` so the commit fails if any
     targeted eq-delete file **or** existing DV was removed concurrently.
   - Snapshot summary: `engine=icestream`.

If commit validation rejects: abort the block, re-load the current snapshot, and
restart the walk from `last-processed-sequence`. Progress is guaranteed — whatever
changed (eq delete removed, DV mutated) is reflected in the new snapshot; we either
won't see the file, or we'll re-merge against the new state.

## Components

Split for isolated unit testing. Each has a narrow API; the per-table orchestrator
glues them together.

1. **SnapshotPlanner** — `plan(table, lastProcessedState) → List<Block>`.
   - Validates v3, pk-set stability, bucket-count stability (fail loud otherwise).
   - Builds the walk over data + eq-delete files, groups into maximal same-fileKind
     fileRuns, filters delete files to the convertible subset.
   - Each `Block` carries `fileKind ∈ {DATA, DEL}`, `files`, `maxSeq`.

2. **DataFileIndexer** — `index(table, dataBlock, pkSchema, buckets, cassandra, spark)`.
   - Reads data files at the table state corresponding to the block's `maxSeq`,
     applying only positional deletes + DVs at seq ≤ `maxSeq`.
   - Upserts `(spec_id, partition_key, bucket, pk) → (data_file_path, pos)` rows.

3. **DeleteFileCreator** — `create(table, deleteBlock, pkSchema, buckets, cassandra,
   spark) → CommitPlan`.
   - Joins eq-delete values to Cassandra, collects `(data_file, pos)` pairs, merges
     per-data-file into new DVs.
   - Returns `CommitPlan { eqDeletesToRemove, existingDvsToRemove, newDvsToAdd }`.
   - Commit and validation are the orchestrator's job — the creator stays pure.

## Unit tests

Each component tested with in-process Spark + Iceberg Hadoop catalog + testcontainers
Cassandra. Shape: Spark-populate table in batches → run component → assert Cassandra
and/or subsequent iceberg metadata.

**SnapshotPlanner**
- Empty table → no fileRuns.
- Only data files → single `[DATA]` block spanning all seqs.
- Only eq-delete files (convertible) → single `[DEL]` block.
- Data at seqs 3-5, eq-deletes at 6-8 → two fileRuns.
- Interleaved data/eq-delete at every seq → singleton fileRuns alternating `DEL, DATA`.
- Resume from `(5, DEL)` — only walk-keys strictly greater included.
- Resume from `(5, DATA)` — only seqs ≥ 6 included.
- Unconvertible eq-delete (schema mismatch) → filtered out of delete block, does not
  split surrounding data block.
- Unpartitioned eq-delete in partitioned table → filtered out similarly.
- Positional-delete file + DV in snapshot → not in walk, do not split data fileRuns.
- Partition-spec evolution across seqs → planner permits (no crash).
- Fatal: non-v3 table with `icestream.primary-keys` set.
- Fatal: pk set changed since last processed.
- Fatal: bucket count changed since last processed.

**DataFileIndexer**
- Single data file, no deletes → one Cassandra row per data row, correct
  `(spec_id, partition_key, bucket, pk, data_file_path, pos)`.
- Data file with positional deletes at seq ≤ `maxSeq` → deleted positions not indexed.
- Data file with an existing DV → DV'd positions not indexed.
- Pos-delete at seq > `maxSeq` → not applied (row is still indexed).
- Eq-delete present in snapshot at seq > `maxSeq` → not applied; row indexed.
- Multiple data files same partition/pk → last writer wins in Cassandra (document,
  don't enforce).
- Bucket hash deterministic across rows with equal pk.
- `StructLike` bytes encoding round-trips for types: int, long, string, date,
  timestamp, binary.
- Partition-spec evolution: files at `spec_id=0` and `spec_id=1` coexist; rows with
  same pk under different specs land in different Cassandra partition keys.
- Large file (10k+ rows) → all positions indexed.

**DeleteFileCreator**
- Eq-delete hits rows in index → `CommitPlan.newDvsToAdd` contains correct bitmap
  positions per data file.
- Eq-delete with pks absent from index → empty `CommitPlan` (no-op).
- Eq-delete hits rows across multiple data files → one new DV per data file.
- Data file already has a DV → `existingDvsToRemove` includes it; `newDvsToAdd` is the
  merged bitmap.
- Data file DV exists but no new positions added → no-op for that data file.
- Multiple eq-delete files in one block → combined into one `CommitPlan`.
- Unconvertible eq-delete (schema mismatch) in block → excluded from plan.
- Unpartitioned eq-delete in partitioned table → excluded from plan.
- Spec-id scoping: eq-delete at `spec_id=1` does not mark rows indexed at `spec_id=0`.
- Null pk value in eq-delete → matches null pk entries in index (or skipped,
  consistent with indexer behavior).

**Orchestrator (integration-ish, but unit-scoped)**
- Two-block sequence (`[DEL] → [DATA]` at adjacent seqs): both applied, state advances
  to data block's maxSeq+fileKind, snapshot summary has `engine=icestream` on the DV
  commit.
- Commit validation rejects (simulate concurrent removal of target eq-delete) → block
  aborted, state unchanged, next `plan()` call on fresh snapshot produces a walk
  reflecting the concurrent change.
- Crash after Cassandra upsert, before state update → rerun indexes same rows
  idempotently; state advances correctly on retry.

## Observability

Structured logs only. No metrics exporter yet.

## Testing

End-to-end integration test (docker-compose):

- Iceberg REST catalog, Minio, Flink ingest (upsert), synthetic data source, Cassandra,
  Spark + this master.
- Run ~10 min. Flink flushes data and eq-deletes to a partitioned iceberg table every
  few seconds.
- Success: `SELECT *` is logically identical before vs. after the master runs; the only
  metadata diff is eq-deletes replaced by DVs. Commits produced by the master are
  identifiable via `engine=icestream` in snapshot summary.

## Deferred

See `.claude/todos.md`.
