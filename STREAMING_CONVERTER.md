# Autonomous per-table Flink+Paimon job — architecture

Status: implemented. The Flink+Paimon backend runs **one long-lived autonomous streaming job per
iceberg table**. There is no master control loop driving work, no separate driver, and no RPC
channel. `FlinkMain` is only a launcher/supervisor: it discovers icestream tables and submits one
`IcestreamTableJob` per table (to a local `MiniCluster` or a remote session cluster), restarting any
that die. Each job is self-driving — its parallelism-1 source *is* the control loop, its committer
owns all correctness, and the two coordinate entirely through committed store state. This note is the
durable record so the work can resume after a session restart.

> History: earlier designs split this into a `master` process and a separate `worker`, then collapsed
> to one process that ran the control loop and shipped each conversion to a warm Flink job over an
> HTTP **conversion channel** (a channel server in the driver; operators dialed back over HTTP;
> checkpoint-barrier alignment was the RPC completion signal; retries, advertised host, channel port).
> Indexing was a separate Flink batch job, and there was a streaming-vs-batch converter choice. **All
> of that is gone.** The channel, the driver, the per-request RPC, and the master loop have been
> replaced by an autonomous job whose source and committer coordinate through the iceberg watermark
> and the Paimon snapshot — no inbound port, no dial-back, no Service.

## Goal

Keep the Flink/Paimon libraries we want **warm** for the life of the job: the Paimon lookup with its
tiered cache (hot `.lookup` SSTs on local disk, cold data from object store), the distributed
`keyBy`, and the V2/V3 delete-file writers — plus, now, the Paimon **index writer**. The job hosts
all of that and reuses it across every run.

Critically: it is **one long-lived job per table**, NOT a fresh Flink job per run. A job-per-run
would spin up cold operators each time → the ~40 s Paimon SST rebuild we spent a long time
eliminating, and a cold index writer. The long-lived job keeps its operators (and lookup cache, and
index writer) warm across all runs. That is the single durable reason for a standing job.

We are **not** using Flink for exactly-once / checkpointing semantics. Correctness comes from a single
sequential source (strict ordering by construction) + at-least-once + idempotent commit.

## The launcher/supervisor (`FlinkMain`)

`FlinkMain` does not drive index/convert work. It:

- Builds the iceberg REST catalog, the `FlinkContext` (local `MiniCluster` or remote session cluster
  per `ICESTREAM_FLINK_MODE`), and the Paimon index catalog.
- Sweeps the catalog on `ICESTREAM_POLL_INTERVAL_SECONDS`, discovering icestream tables
  (`IcestreamCatalogScan`).
- For each table with no live job, initializes the Paimon index table and submits one
  `IcestreamTableJob` via `executeAsync` (`Supervisor.launch`).
- Tracks each job's `JobClient`; if a job has reached a globally-terminal state it is resubmitted on
  the next sweep. On shutdown it cancels all jobs.

The only HTTP that remains anywhere is Flink's own job submission to the session cluster's JobManager
REST endpoint (remote mode). The icestream pod makes **only outbound** connections — REST catalog,
object store, JobManager. There is no inbound port and no Service to dial back into.

## The autonomous job graph (`IcestreamTableJob`)

One job per table, submitted once and kept alive:

```
  IcestreamWalkSource (p1)  ──the control loop; emits one WorkUnit per checkpoint epoch──┐
        │                                                                                │
        ├── DATA branch                                                                  │
        │     WorkUnitToDataFiles (p1) ─ rebalance ─► DataFileFlatMap (read data files)  │
        │       ─► Paimon PK-index upsert sink (FlinkSinkBuilder, bucket-keyed)          │
        │       [keeps the index writer warm]                                            │
        │                                                                                │
        └── EQ_DEL branch                                                                │
              WorkUnitToEqDeleteFiles (p1) ─ rebalance ─► EqDeleteSourceFlatMap (read)   │
                ─► keyBy(Paimon bucket) ─► warm Paimon lookup join (FOR SYSTEM_TIME …)   │
                ─► matches keyBy(data_file_path)                                         │
              WorkUnitToExistingDeletes (p1) ─► keyBy(data_file_path) ──┐                │
                                                                        ▼                │
                  StreamingWriteDeleteFilesOperator (distributed, per-data-file writes)  │
                    ─► TaskOutputs ───────────────────────────────────────────┐         │
                                                                               ▼         ▼
                                                            RunCommitter (p1) ◄──── WorkUnit
                                                              commits the run +
                                                              advances the iceberg watermark
```

Both file reads and writes distribute across the cluster; only the source, the committer, and the
small `WorkUnit`-fan-out flatmaps are parallelism-1.

### The source IS the control loop (`IcestreamWalkSource`)

A parallelism-1 unbounded source that walks the `SnapshotPlanner` in strict `(seq, kind)` order and
emits exactly one `WorkUnit` per run:

1. `table.refresh()`, read the iceberg watermark from table properties (`IcestreamWatermark.read`).
2. **Gate**: if a run was already emitted and the watermark hasn't reached it yet, sleep
   `pollIntervalMs` and loop — do not emit anything else. **One run in flight at a time.**
3. Otherwise `planner.planNextRun(table, watermark)`. If empty, sleep `idleBackoffMs` and loop.
4. Build the `WorkUnit` for the run (it does all the live-table work here, so the emitted unit carries
   only Flink-serializable value types):
   - **DATA**: the per-file `FileWorkItem`s.
   - **EQ_DEL**: the per-file `EqDeleteWorkItem`s, the prior per-data-file deletes for the touched
     partitions (`ExistingPerFileDeleteLoader.collect`), and the eq-delete files the committer will
     later remove.
5. Emit the unit under the checkpoint lock (so its whole computation stays inside one checkpoint
   epoch — the following barrier triggers the writer flush / Paimon commit the committer keys off),
   record it as the in-flight position, loop.

The source keeps **no Flink-checkpointed state**. On restart it rebuilds the catalog and resumes from
the iceberg watermark.

### The committer owns correctness (`RunCommitter`)

A parallelism-1 terminal two-input operator. Input 1 is the `WorkUnit` control record for the epoch;
input 2 is the distributed writers' `TaskOutputs` slices (EQ_DEL only) — barrier alignment guarantees
all N slices have arrived before the barrier. On `notifyCheckpointComplete(K)` (after the epoch's
writer flush and Paimon sink commit):

- **EQ_DEL**: assemble the `CommitPlan` from the writer slices (`DeleteFileCreatorSupport.assemblePlan`)
  and commit the `RowDelta` via `ConversionCommitter.commit` — `validateFromSnapshot` against the
  run's starting snapshot + the `icestream-converted` tag — then advance the iceberg watermark to
  `(maxSeq, EQ_DEL)`.
- **DATA**: poll the Paimon index snapshot until it moves past the pre-run snapshot (the sink
  committed this epoch's rows), bounded by `ICESTREAM_PAIMON_COMMIT_TIMEOUT_MS`, then advance the
  watermark to `(maxSeq, DATA)`.

Advancing the watermark is what paces the source. A failure here restarts the job; the source
re-emits from the unadvanced watermark.

## Coordination: through committed state, not RPC

Source and committer never talk directly. The committer advances the iceberg watermark property; the
source polls it. The DATA branch's effect is visible as a Paimon snapshot; the committer polls that.
All coordination is through durably committed store state. There is no channel, no future, no
completion callback, no barrier-as-RPC-signal across processes.

## Correctness

- **Strict ordering by construction.** A single sequential source drives the whole walk, so the
  `EQ_DEL(N)` → must-see-all-`DATA<N` interleaving is correct without any cross-operator gating: an
  `EQ_DEL(N)` is only planned once the watermark sits at the last `DATA<N`, which the committer
  advanced to only *after* that data's Paimon index commit landed. A conversion therefore never reads
  a stale index.
- **At-least-once + idempotent commit.** The commit runs after the checkpoint, not inside it (no
  exactly-once / 2PC sink). The watermark advances only post-commit, so a crash after
  emit-before-commit re-emits the same run on restart; re-running a unit reproduces the same files and
  the commit is idempotent (index writes are PK upserts; the eq-delete `RowDelta` is gated by
  `validateFromSnapshot`, and orphaned delete files are cleaned later). This is exactly the
  at-least-once model the channel design used, minus the channel.
- **No Flink-checkpointed control state.** The source's only persistent state is the iceberg
  watermark it reads back from the table.

## Index settings / lookup-cache tuning

Unchanged and still load-bearing — the cache only earns its keep because the job is long-lived. The
per-bucket `.lookup` SSTs are materialized (or downloaded, with remote lookup files) and their pages
warmed *once*, then reused across every run the standing job serves. Toggles (set as index-table
options): `lookup.cache-max-memory-size` (in-memory page cache), `lookup.cache-file-retention` (idle
local-disk retention), `lookup.cache-max-disk-size` (local-disk budget, LRU-evict instead of filling
the disk). See the README's *Lookup-cache tuning* and *Benchmark* sections for the sizing data.

### Remote lookup files + the `LocalTableQuery` download patch

For huge indexes the index options are DV-off + `force-lookup` + `lookup-compact=radical` +
`lookup-wait` + `lookup.remote-file.enabled` + `level-threshold=1`, so each write does a radical
lookup compaction (L0→L1) that materializes lookup SSTs and uploads them to the warehouse. The read
path is the `org.apache.paimon.table.query.LocalTableQuery` **monkey patch** (same-FQN shadow; Paimon
never wires a `RemoteFileDownloader` on the read side) that constructs a `RemoteLookupFileManager`
over each bucket's `LookupLevels`, so a paged-out lookup file is **downloaded**, not rebuilt. (Verified
on the 55M index: 40 `.lookup` SSTs in the warehouse; the cluster jar's `LocalTableQuery` is ours.)
Effect: the mid-range cold-rebuild spikes (D=1k/10k) collapse from ~14–16 s to sub-2 s and the
warm-lookup curve becomes ~monotonic. This patch and its tuning are independent of the job topology
and carry over unchanged.

## What we are explicitly NOT doing

- No conversion channel, no RPC, no driver, no master loop, no inbound port / dial-back Service.
- No exactly-once / 2PC sink / savepoint semantics; correctness is sequential-source + at-least-once +
  idempotent commit.
- No new Flink job per run (the whole point is the warm lookup cache + warm index writer).
- No keyed-state index (we keep Paimon for the tiered local-cache + object-store fallback on huge
  tables).
- No multi-run pipelining: one run in flight per table at a time (the source gates on the watermark).
- No cross-table sharing of a job — one autonomous job per table.

## Proven on a real distributed cluster

The `remote` path was validated end to end on a downloaded **Flink 2.0.0** standalone cluster
(1 JobManager + 2 TaskManager JVMs, 2 slots each) at `localhost:8081`, dist at `~/data/flink-2.0.0`
(`numberOfTaskSlots: 2`). The distribution and warm-cache results below still characterize the same
operators (reads, bucket-keyed lookup, keyed writes) the autonomous job runs — only the wiring around
them changed (no channel).

- Build the shaded job jar: `mvn -pl icestream-flink-paimon -am -DskipTests -Pcluster-jar package`
  → `target/icestream-flink-paimon-1.0-SNAPSHOT-cluster.jar` (~162 MB).
- The reads + bucket-keyed lookup ran at parallelism 4, the `WriteDeleteFiles` writers at parallelism
  4, and the writer subtasks ran on **both** TaskManager JVMs — so reads, lookup, and writes genuinely
  spread across the two TM processes.
- A HASH exchange (the bucket `keyBy`) feeds the lookup `TableSourceScan` vertex, so each lookup
  subtask owns a disjoint bucket slice and caches only that slice (cache-locality; correctness
  unchanged — the lookup resolves any key).

Operational notes for `remote`:
- The warehouse must be on storage both the launcher pod and the TaskManagers can reach (a shared
  local path on one host, or object store for true multi-host).
- Bump `taskmanager.memory.jvm-metaspace.size` to ~1g (default 256m) — the 162 MB shaded jar loads
  many classes per `ChildFirstClassLoader`, and a long-lived session cluster OOMs Metaspace after
  several submissions otherwise.

### D-sweep crossover (5M rows, warmed once)

Warm distributed lookup (bucket-keyed) vs a local batch-join control:

```
         D        D/N     warm lookup    batch-join   speedup   winner
         1    0.0000%        0.41 s        6.71 s     16.35x   LOOKUP
        10    0.0002%        0.47 s        5.12 s     10.84x   LOOKUP
       100    0.0020%        0.41 s        4.44 s     10.89x   LOOKUP
     1,000    0.0200%        0.44 s        4.22 s      9.57x   LOOKUP
    10,000    0.2000%        3.27 s        4.43 s      1.35x   LOOKUP
   100,000    2.0000%        3.18 s        6.74 s      2.12x   LOOKUP
 1,000,000   20.0000%       16.52 s       11.19 s      0.68x   batch
```

Flat ~0.4 s warm floor up to D=1k, lookup ahead through 100k, batch overtaking only when D is a large
fraction of N. The standing-job warm cache is what keeps the floor flat — exactly the warmth the
autonomous per-table job preserves.

## Running against a real standalone Flink cluster

The only piece beyond the launcher is a Flink 2.0 distribution + the shaded job jar (the operators'
classes must be on the TaskManager classpath). Once those exist:

1. `bin/start-cluster.sh` → JM+TM on `localhost:8081` (separate JVMs); `bin/taskmanager.sh start` for
   a 2nd TM.
2. Run the launcher:
   `ICESTREAM_FLINK_MODE=remote ICESTREAM_FLINK_JM_HOST=localhost ICESTREAM_FLINK_JM_PORT=8081
   ICESTREAM_FLINK_JARS=/path/to/icestream-cluster.jar ICESTREAM_ICEBERG_REST_URI=…
   ICESTREAM_PAIMON_WAREHOUSE=… …`. It submits one autonomous `IcestreamTableJob` per discovered table
   to the cluster; each job walks the planner, indexes, converts, commits, and advances its own
   watermark. The launcher only resubmits a job if it dies — there is no per-run submission and no
   dial-back.
