# Long-lived warm Flink converter — architecture

Status: implemented. A single process runs the master control loop *and* owns the converter that
drives one long-lived Flink job; the job's operators reach the driver's conversion channel over an
in-process call (local MiniCluster) or HTTP (remote session cluster). Both paths are covered by tests
and the transport overhead is measured negligible. Remaining items (INDEX-in-streaming-job, marker,
hardening) are listed under Phasing. This note is the durable record so the work can resume after a
session restart.

> History: an earlier design split this into a `master` process and a separate `worker` process with
> an HTTP RPC hop between them (hop A). That split was collapsed — a separate worker was overkill and
> the RPC hop added code without buying isolation. The control loop and the converter driver now live
> in **one process**. The cross-JVM **channel** to the standing job's operators (hop B below) is the
> only HTTP that remains, and only when the job runs on a remote cluster.

## Goal

One process is the control plane with real authority, and it drives a **single long-lived Flink job**
as a *warm distributed worker*. The job hosts the Flink/Paimon libraries we want (Paimon lookup with
its tiered cache — hot lookup files on local disk, cold data from object store — the distributed
keyBy, and the V2/V3 delete-file writers). We are **not** using Flink for exactly-once / checkpointing
semantics; correctness lives in the driver.

Critically: it is **one long-lived job fed units of work**, NOT a new Flink job submitted per request.
A job-per-request would spin up cold operators each time → the ~40s Paimon SST rebuild we spent a long
time eliminating. The long-lived job keeps its operators (and lookup cache) warm across all requests.

## Responsibilities

**Driver process (all correctness):**
- Poll the iceberg catalog, discover icestream tables, plan runs in sequence order (`SnapshotPlanner`).
- Enforce ordering: index a DATA run at seq N (and confirm the Paimon index committed) before
  converting the EQ_DELETE at seq N — so a conversion never reads a stale index.
- Drive **index** and **convert** work by calling the indexer/converter directly (in-process; no RPC).
- Commit the conversion's `RowDelta` (`validateFromSnapshot`, `icestream-converted` tag).
- Advance the watermark (iceberg table properties).
- **Idempotent retry**: if a conversion times out (lost in transit / job restart), re-submit.
  Re-running a unit reproduces the same files; orphaned delete files are cleaned later. Watermark only
  advances after the commit. So at-least-once job + driver idempotency = correct; no Flink exactly-once.

**Long-lived Flink job (warm libraries, no correctness):**
- One job, a **channel-fed source** receiving bounded conversion requests:
  - **CONVERT unit**: read these eq-delete files, run the warm Paimon lookup join (refresh to the
    required index snapshot first), write per-data-file delete files. Reply: `TaskOutputs`.
- Warm Paimon lookup cache persists across requests (the whole point). On job restart the cache is
  cold; the driver just re-submits and eats a few cold conversions.

(Indexing today is a separate Flink batch job — `FlinkDataFileIndexer` — distributed in its own right;
folding it into the long-lived job as an INDEX unit is a Phasing item.)

## Transport — the conversion channel

The channel state (request queue, in-flight queue, pending futures) lives in the **driver JVM**, where
`create()` submits and awaits. The standing job's operators reach that state over **HTTP** — one
transport, whether the operators run in an in-JVM MiniCluster (loopback) or on a remote TaskManager
(network). The converter (`StreamingFlinkDeleteFileCreator`) hosts a `ConversionChannelServer` itself
and gives its source + writer a `RemoteConversionChannelClient` pointed at it; the operators dial
**out** to the driver's advertised URL, so we never discover TaskManager addresses. Endpoints:
`POST /channel/poll?jobKey&timeoutMs` (204 on timeout, else serialized `ConversionRequest`),
`POST /channel/inflight?jobKey&conversionId`, `POST /channel/complete?jobKey` (body: serialized
`List<TaskOutputs>`). `markInFlight` stays inside the source's checkpoint lock so a barrier can't
split mark-from-emit (the empty-barrier race). Keeping the in-JVM case on the same HTTP path (rather
than a direct-call shortcut) means every test exercises the exact transport we deploy.

The payloads (`ConversionRequest`, `TaskOutputs`, Iceberg `DataFile`/`DeleteFile`) are already
Java-`Serializable` but NOT proto-friendly (Iceberg internals), so the channel ships Java-serialized
objects (`ConversionChannelCodec`) over a small JDK `HttpServer`/`HttpClient` — no protobuf codegen,
reuses our Serializable types.

The converter binds `ICESTREAM_CHANNEL_PORT` (0 = ephemeral for local/tests) and advertises
`ICESTREAM_CHANNEL_ADVERTISED_HOST` (defaults to `localhost`; set to the channel Service DNS for a
remote session cluster). `FlinkMain` wires `FlinkContext.remote(jm,port,jars)` under
`ICESTREAM_FLINK_MODE=remote` and an in-pod MiniCluster otherwise.

### Channel resilience (the 550M flake fix)

The 550M scale test exposed a real bug — and it was NOT the checkpoint boundary (the suspected
end-of-request-marker problem). The source long-polls `/channel/poll`, so a long conversion makes
hundreds of polls; the JDK `HttpClient` pools connections and the JDK `HttpServer` closes idle ones,
so a reused-but-closed connection throws `IOException: HTTP/1.1 header parser received no bytes` /
`Connection reset`. That escaped the source operator → failed the task → **restarted the whole
standing job** (~8 restarts seen via the `_N` attempt suffixes) → lost the warm cache and the
in-flight conversion → `create()` returned an empty plan. Fast/low-D conversions barely poll during
the conversion so they survived; D≥100k (tens of seconds) hit a stale connection and flaked.

Fix — the warm job must never die from a transport hiccup:
- `RemoteConversionChannelClient` retries every call on `IOException` (`MAX_ATTEMPTS=8`), rebuilding
  the client each retry to shed the stale connection pool; `poll` swallows a final failure and
  returns null (re-poll).
- `ConversionRequestSource` wraps poll/emit so a transport error is logged and the loop continues —
  it never propagates. A request taken server-side but lost in transit just leaves its future to
  time out, and the driver re-submits it (idempotent). The long-poll was lengthened 1s→5s to cut
  connection churn.
- `CollectConversionOutputsOperator` swallows a `complete()` failure (logs) rather than fail the
  operator — same reasoning: never restart the warm job; rely on driver timeout-retry.

The end-of-request marker remains deferred — it's a clean-up (decouple flush from checkpoint timing,
enable pipelining), not a correctness fix; the scale flake was transport, not boundary.

### Distribution of reads and writes (implemented)

Goal: every file read and write distributes across the cluster; only the commit is centralized (on
the driver). Status per job:

- **INDEX job** (`FlinkDataFileIndexer`): already distributed — `DataFileFlatMap` reads data files at
  job parallelism (the p1 `fromCollection` only emits lightweight file descriptors), and Paimon's
  sink shards writes by bucket (`bucket-key=pk`).
- **CONVERT job** (streaming): now distributed end to end:
  - **eq-delete reads** — the p1 source emits the request; `RequestToEqDeleteFiles` splits it into one
    `EqDeleteWorkItem` per eq-delete file, `rebalance`d to the shared `EqDeleteSourceFlatMap` reader at
    job parallelism, so file reads fan out (one file per record).
  - **lookup join** — distributed and <b>cache-local</b>: probe rows are `keyBy`'d by their Paimon
    bucket (computed by Paimon's own `FixedBucketRowKeyExtractor` over the index `TableSchema`, so it
    matches the write-side assignment by construction — see `PaimonBucketKeySelector`). Each lookup
    subtask therefore owns a disjoint slice of buckets and caches only that slice, instead of every
    subtask caching every bucket it sees under round-robin. Cache-locality only; correctness is
    identical (the lookup resolves any key regardless of subtask).
  - **positional-delete writes** — match rows are `keyBy(data_file_path)` to `StreamingWriteDeleteFilesOperator`
    at job parallelism; each subtask owns disjoint data files (a positional delete file belongs to
    exactly one data file → no collisions). Each subtask flushes its slice at the checkpoint barrier
    and emits `TaskOutputs` downstream.
  - **collection** — a parallelism-1 `CollectConversionOutputsOperator` aggregates all writer subtasks'
    slices for the conversion and replies. It knows the conversion is complete across N writers via
    **aligned barriers**: each writer emits its slice during its own `prepareSnapshotPreBarrier(K)`, so
    by the time barrier K aligns at the collector it has received all N slices. So no explicit
    end-of-request marker is needed even with full fan-out — requires aligned checkpoints (the default).

Why this keeps the per-request boundary correct under fan-out: the p1 source emits one request per
checkpoint epoch under the checkpoint lock; FIFO + barrier alignment then bracket the whole distributed
computation (split → parallel read → lookup → parallel keyed write → collect) within that epoch.

Covered by `DistributedStreamingConversionTest` (4 data files, 2 eq-delete files, parallelism 4 →
correct per-file positions, unchanged by the bucket keyBy). Verified on the real cluster via the job
plan: a HASH exchange (bucket keyBy) now feeds the lookup `TableSourceScan` vertex (previously the read
and lookup were forward-chained), and the warm-conversion floor dropped 0.45 s → 0.23 s at 400k rows.

### Transport overhead (measured)

`ConversionBenchmark -Dbench.remoteChannel=true` times warm conversions through both channel clients
against the same warm index (job in a local MiniCluster either way, so the delta is pure transport).
At 400k rows / 8 files, warm-best was in-process **0.41 s** vs HTTP **0.37 s** — i.e. the per-conversion
transport cost is **within run-to-run noise** (a few HTTP loopback round-trips + serialization of a
small request/result, sub-10ms, dwarfed by the lookup join). For the target (a *same-host* standalone
cluster) the per-conversion networking *is* loopback, so this is a faithful proxy; a real cluster adds
only the one-time job-graph submission, not per-conversion cost.

## Components

Reused (already in `icestream-flink-paimon`): the streaming pipeline (`ConversionRequestSource`,
`RequestToEqDeleteFiles`, the shared `EqDeleteSourceFlatMap` reader, the lookup-join SQL,
`StreamingWriteDeleteFilesOperator`), `InProcessConversionChannel`, `DeleteFileCreatorSupport`,
`ConversionCommitter` (commit stays on the driver).

Channel:
- `RemoteConversionChannelClient` — the only client; reaches the driver's channel over HTTP (hardened
  with retries). The in-JVM case is just loopback, so there is no in-process shortcut to maintain.
- `ConversionChannelServer` — JDK `HttpServer` hosted by the converter; bridges `/channel/*` to the
  in-JVM `InProcessConversionChannel`.
- `ConversionChannelCodec` — Java-serialization codec for the request/response payloads.
- `FlinkMain` wiring: one `run(config)` builds the iceberg catalog, the `FlinkContext` (remote/local),
  the converter (which owns its channel server), and `TableProcessor`/`MasterLoop`.

Deferred/new:
- End-of-request marker carried through the pipeline; terminal operators flush per request.
- INDEX path folded into the long-lived job (today indexing is a separate batch call).

## Boundary detail (the bit to get right)

A request operates on a bounded file set, so the source emits its splits then an end-of-request marker
tagged with `requestId`. The marker bypasses the SQL join's WHERE filter (markers don't survive a
join), so it travels a side path / broadcast to the terminal operator(s). Each terminal operator, on
seeing `end(requestId)`, flushes the outputs it accumulated for that request and forwards them to a
parallelism-1 collector, which assembles all subtasks' outputs and completes the request's future.
Serial dispatch from the driver (one unit at a time) keeps this unambiguous. (Today the
checkpoint-barrier flush already provides this signal — see Distribution above — so the explicit marker
is a deferred clean-up.)

## What we are explicitly NOT doing
- No checkpoint-aligned flush, no 2PC sink, no watermark/event-time, no savepoint exactly-once.
- No new Flink job per request.
- No keyed-state index (we keep Paimon for the tiered local-cache + object-store fallback on huge tables).

## Phasing
1. **Local single-process proof** — done (`StreamingFlinkDeleteFileCreator` + loopback channel +
   benchmark): convert units, warm cache.
2. **Cross-JVM channel for remote session clusters** — done. The HTTP `RemoteConversionChannelClient`,
   `/channel/*` endpoints on `ConversionChannelServer`, `FlinkMain` remote wiring. Tested by
   `RemoteChannelConversionTest` (conversion driven entirely over the HTTP channel); since the in-JVM
   path is the same loopback HTTP, every streaming test exercises the production transport.
3. **Single-process collapse** — done. The separate `worker` process + the master↔worker RPC hop were
   removed; one process runs the control loop and owns the converter driver. Only the operator channel
   (hop B) remains, over HTTP.
4. **Still open**: add the INDEX unit to the long-lived streaming job (today it's a separate batch
   call); end-of-request marker + collector (nice-to-have — the checkpoint-barrier flush already
   provides the per-request signal); job restart/reconnect, request timeouts, health, backpressure.

## Proven on a real distributed cluster (2026-06-01)

Ran end to end on a downloaded **Flink 2.0.0** standalone cluster (1 JM + **2 TaskManager JVMs**,
2 slots each) at `localhost:8081`, dist at `~/data/flink-2.0.0` (`numberOfTaskSlots: 2`):

- Built the shaded job jar: `mvn -pl icestream-flink-paimon -am -DskipTests -Pcluster-jar package`
  → `target/icestream-flink-paimon-1.0-SNAPSHOT-cluster.jar` (~162 MB).
- Ran `ConversionBenchmark -Dbench.remoteCluster=true -Dbench.flinkJars=<jar>
  -Dbench.warehouseDir=/tmp/icestream-cluster -Dbench.jmPort=8081 -Dbench.slots=4
  -Dbench.rows=400000 -Dbench.files=8 -Dbench.streamRuns=4`.
- Both the INDEX job and the standing CONVERT job submitted to the cluster. Warm conversions:
  **0.45–0.66 s**, consistent with the in-process/loopback numbers — confirming the HTTP channel
  adds no meaningful per-conversion cost.
- **Verified distribution via the REST API**: the converter job's vertices were reads+lookup p=4,
  `WriteDeleteFiles` p=4, collector p=1; the writer's 4 subtasks ran on **both** TaskManager ids
  (`localhost:63402-…` and `localhost:63408-…`). So reads, lookup, and writes genuinely spread across
  the two TM processes while operators reached this JVM's channel over HTTP.

Reproduce: see the env block below; `bin/start-cluster.sh` then `bin/taskmanager.sh start` for a 2nd TM.
Bump `taskmanager.memory.jvm-metaspace.size` to ~1g (default 256m) — the 162 MB shaded jar loads many
classes per `ChildFirstClassLoader`, and a long-lived session cluster OOMs Metaspace after several job
submissions otherwise.

### D-sweep crossover on the cluster (5M rows, warmed once)

Warm distributed lookup (bucket-keyed) vs the local batch-join control — same D window as the earlier
in-process charts, so they align:

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

Same shape as in-process: flat ~0.4 s warm floor up to D=1k, lookup ahead through 100k, batch overtakes
by D=1M (20% of N). Crossover between 100k and 1M — consistent with the prior small-fraction-of-N finding.

### Remote lookup files + download monkey patch (55M, vs lazy rebuild)

Index options changed to DV-off + `force-lookup` + `lookup-compact=radical` + `lookup-wait` +
`lookup.remote-file.enabled` + `level-threshold=1`, so each write does a radical lookup compaction
(ForceUpLevel0Compaction, L0→L1) that materializes lookup ssts and uploads them to the warehouse. The
read path is the `org.apache.paimon.table.query.LocalTableQuery` **monkey patch** (same-FQN shadow;
Paimon never wires a `RemoteFileDownloader` on the read side) that constructs a `RemoteLookupFileManager`
over each bucket's `LookupLevels`, so a paged-out lookup file is **downloaded**, not rebuilt. (Verified:
40 `.lookup` ssts written to the warehouse; cluster jar's `LocalTableQuery` is ours.)

Effect on warm-lookup latency (55M, cluster), rebuild → download:

```
       D     before (rebuild)   now (download)
       1          0.26 s            0.25 s
      10          0.62 s            0.31 s
     100          0.61 s            2.99 s   (first-touch download)
   1,000         14.24 s            0.68 s   (~21x — was a cold rebuild)
  10,000         15.81 s            1.73 s   (~9x  — was a cold rebuild)
 100,000          6.15 s            6.58 s
1,000,000        50.68 s           48.59 s   (large-D regime, batch wins, unchanged)
```

The mid-range cold-rebuild spikes (D=1k/10k) collapse from ~14–16 s to sub-2 s, and the curve becomes
~monotonic (no more cache-thrash inversion). Residual D=100 blip is one-time first-touch download cost.

## Running against a real standalone Flink cluster

The only piece beyond the driver process is a Flink 2.0 distribution + a shaded job jar (the operators'
classes must be on the TaskManager classpath). Once those exist:
1. `bin/start-cluster.sh` → JM+TM on `localhost:8081` (separate JVMs).
2. Run the driver:
   `ICESTREAM_FLINK_MODE=remote ICESTREAM_FLINK_JM_HOST=localhost ICESTREAM_FLINK_JM_PORT=8081
   ICESTREAM_FLINK_JARS=/path/to/icestream-cluster.jar
   ICESTREAM_CHANNEL_ADVERTISED_HOST=<driver-host> ICESTREAM_CHANNEL_PORT=8090 ICESTREAM_ICEBERG_REST_URI=…
   ICESTREAM_PAIMON_WAREHOUSE=… …`. The standing job lands on the cluster; its source/writer dial back
   to the driver's `/channel/*` over HTTP. Correctness (ordering/commit/watermark/retry) stays in the
   driver process.
The warehouse must be on storage both the driver and the TaskManager can reach (a shared local path on
one host, or object store for true multi-host).
