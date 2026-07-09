# HFresh Search Optimization

Status: draft — experimentation branch, not for release.
Branch: `worktree-hfresh-search-optimization` (private; deployed only to experimentation VMs; unrestricted instrumentation allowed).

## Goal

Maximize search QPS per node at **unchanged recall** and ≤200ms latency on commodity
cloud disks (AWS EBS gp3), for deployments in the range of 1–3B × 256d vectors per
node. Search-only workload first; search-under-update comes later.

Non-goals / hard constraints:

- **No design changes.** Postings, centroid HNSW, LSM layout, and the on-disk
  format stay as they are. This is an optimization effort, not a redesign.
- **No recall impact.** Every change is either provably recall-invariant
  (parallelism, CPU work, dedup of reads) or adaptive with termination bounds
  tuned until benchmarks show recall parity.
- **No caching as a strategy.** Page cache and any in-process caches may help
  incidentally, but the design targets the cold/random-access case.
- **No deadline/drop mechanisms.** We optimize as much as reasonable, then
  evaluate whether it is enough.
- **Defaults unchanged**: `searchProbe=256`, `rescoreLimit=350` remain the
  defaults; adaptive mechanisms treat them as caps, not spend targets.
- **No vector duplication** (disk budget matters at 15B scale).

## Search path today (adapters/repos/db/vector/hfresh/search.go)

Per query, five phases:

1. **Centroid search** — in-memory HNSW (RQ8, EF=64) over centroids. Sub-ms, CPU-only.
2. **Candidate filtering** — `maxDist` pruning is effectively disabled
   (`MaxDistanceRatio` defaults to 10,000; faithful port of SPFresh, which also
   ships it disabled). `PostingSizes.Get` is an in-memory paged array. Net
   effect: all `searchProbe` postings are read.
3. **Posting reads** — `PostingStore.MultiGet` is a sequential loop of
   `SetRawList` calls: sharded lock → posting-version lookup (otter cache) →
   per-segment bloom test + segment-index lookup + `copyNode` of the full
   collection node + parse. Postings touched by `Append` since the last
   compaction are fragmented across active memtable, flushing memtable, and
   multiple segments — one posting can cost several IOs.
4. **Scan** — for every vector of every posting: version check (in-memory,
   sharded lock per call), visited dedup (replicas=4 ⇒ up to 4× duplicates),
   allowlist check, RQ1 decompress-copy into `[]uint64`
   (`FromCompressedBytesInto`), popcount distance, ResultSet insert.
5. **Rescore** — sequential loop over the top `rescoreLimit` RQ1 candidates:
   `vectorForId` reads the **full object** from the objects bucket by secondary
   key (each call acquires its own bucket consistent view), parses the vector
   out of storobj binary, computes the exact distance.

### Cost model at 256d (the frame for everything)

- RQ1 code ≈ 32B + 9B header + RQ metadata ≈ ~60B/vector → `maxPostingSize ≈ 800`.
- 256 probes × 48KB = **~12MB read** per query.
- 256 probes × ~400–800 vectors = **100–200k candidates scanned** per query
  ⇒ ~5–15ms pure CPU. At parallel=16 this alone can saturate a 16-core box —
  scan CPU is a first-class suspect at 256d, not a footnote.
- IOs per query: ~256 posting reads (× fragmentation multiplier) + ~350
  rescore reads ≈ **600+ random IOs**. At 256d the rescore loop is the single
  largest IO consumer.

The governing equations on EBS:

```
QPS ceiling ≈ provisioned IOPS ÷ IOs-per-query
QPS ceiling ≈ provisioned MB/s ÷ MB-per-query      (whichever binds first)
```

Realistic gp3 envelope per node (2–4 striped volumes, "reasonable" cost):
~50k IOPS, ~2GB/s. Today's ceiling: **~80 QPS/node** (IOPS-bound), or ~170
QPS (throughput-bound) even if IOPS were free. Latency is the easy part:
parallel reads at QD~32 put even 600 IOs at ~20–30ms wall clock. Only IO
*volume* reduction moves the QPS ceiling.

RQ1 at 256d is only 256 bits — the distance estimate is coarser than at
1536d, so rescoring carries more of the recall burden. `rescoreLimit` cannot
be cut blindly, only adaptively.

## What the SPFresh original does differently

Reference: local copy at `~/code/github.com/asdine/lab/SPFresh`
(AnnService/src/Core/SPANN/SPANNIndex.cpp, inc/Core/SPANN/ExtraDynamicSearcher.h,
ExtraSPDKController.h, Script_AE/iniFile/*).

1. **`MaxDistRatio` is disabled upstream too** (default 10000; benchmark inis
   set 1,000,000). Distance-ratio pruning is *not* how they control IO. Their
   levers: fewer probes + page cap + async IO + deadline.
2. **They probe 64 postings, not 256**, and cap each posting read at 4 pages
   (16KB): `SearchInternalResultNum=64`, `SearchPostingPageLimit=4`.
   Per-query IO ≈ 64 reads / ~1MB vs our ~600 reads / ~12.5MB
   (**10× IOs, 12× bytes**).
3. **Posting reads are batched async with a per-query deadline**
   (`db->MultiGet(postingIDs, &lists, remainLimit)`, SPDK batch=64; postings
   that miss the deadline are dropped). We deliberately do NOT adopt the
   deadline (recall-affecting); we do adopt the batched-parallel IO.
4. **No rescore phase exists upstream** — full vectors live in postings, scan
   distances are exact. The 350-random-read rescore loop is HFresh's own
   addition (the RQ1 tax). We own this problem entirely.
5. **Zero-copy scan** — distance computed directly on the posting buffer
   (`vectorInfo + m_metaDataSize`); no per-vector copy, no per-candidate
   error wrapping.
6. **Raw block store** — one in-memory block mapping, exactly 1 IO per posting,
   always. Our LSM path pays bloom checks, segment-index lookups, and possibly
   multiple segment reads per posting until compaction settles. The
   "segments per posting read" counter quantifies this gap.

## Phase 0 — Instrumentation & baseline

This branch is private and experimentation-only: instrument as heavily as
useful, without concern for production overhead or API cleanliness.

1. **Revisit `searchProfiler`** (`profile.go`, currently not wired into
   `SearchByVector`) and extend it to cover everything we need, then wire it
   into the search path. Required per-query data:
   - per-phase wall time: centroid search, filtering, posting read, scan, rescore;
   - postings requested / postings read / postings empty;
   - **segments touched per posting read** (fragmentation multiplier), plus
     memtable/flushing hits;
   - bytes read (postings; rescore objects);
   - candidates scanned; duplicates skipped (replica waste); deleted skipped;
     allowlist-filtered;
   - rescore fetches, rescore not-found (deleted race), rescore bytes;
   - ResultSet/topk sizes actually reached (to inform adaptive cutoffs later).
   Aggregates: histograms, not just means — the 200ms constraint is a tail
   constraint. Expose as Prometheus metrics AND a periodic structured log line
   (VMs may be scraped or not).
2. **Disk/rig baseline**: `fio` random-read at 4k/16k/48k block sizes,
   QD 1/8/32/64; record volume config (size, provisioned IOPS/throughput) and
   instance EBS bandwidth cap (instance family can bind below volume limits).
3. **Benchmark protocol**:
   - Local recall gate: dbpedia-1536-1M + nytimes-256-angular (real 256d data)
     via `weaviate-benchmarking ann-benchmark`, recall@10 across searchProbe
     sweep. Recall parity = no regression beyond run-to-run noise on both
     datasets at all probed settings.
   - 1B/256d rig: QPS/latency/IO counters under `iostat`, pprof CPU +
     off-CPU (fgprof). Ground truth for sanity checks can be generated with
     import-sphere's `step_gt` (~40min offline for 1k probes); random-vector
     recall is a weak signal (distance concentration), so it sanity-checks
     rather than gates.
   - Warm vs cold runs distinguished explicitly (drop caches / restart between
     cold runs).
4. **Exit criterion**: we know the CPU/IO split per phase, the fragmentation
   multiplier, and the duplicate ratio — before writing any optimization.

### Phase 0 implementation status

Instrumentation is in place on this branch:

- **`HFRESH_SEARCH_PROFILE=1`** enables the search profiler
  (`adapters/repos/db/vector/hfresh/profile.go`). Every 2000 queries it logs
  one `action=hfresh_search_profile` line describing **only that window**
  (`queries` = cumulative counter, `window` = queries in the line), so a
  benchmark sweep yields per-setting lines — read the last line of each 10k
  block. Fields: per-phase mean/p99 (centroid,
  filter, posting read, scan, rescore), `read_pct`, postings
  requested/read/empty, **`segments_per_posting`** (the fragmentation
  multiplier; 1.0 = fully compacted), memtable hits, posting KB, candidates
  scanned, `duplicate_pct` (replica waste), deleted/allowlist skips, rescore
  fetched/not-found, and result counts. Aggregation is lock-free
  (power-of-two-bucket histograms, `logHist`), so it is safe to leave on
  during benchmarks.
- **Per-phase Prometheus summaries** are always exported when metrics are on:
  `vector_index_durations_ms{operation="search", step="centroid|filter|posting_read|scan|rescore"}`.
- **`lsmkv.SetRawListWithStats`** reports, per posting read, how many disk
  segments contained the key, whether the memtables were involved, and the
  payload bytes; `PostingStore.MultiGetWithStats` threads this into the
  per-query stats.
- **`tools/dev/hfresh_rig_baseline.sh`** — run once per rig before
  benchmarking: host info + fio random-read matrix (4k/16k/48k × QD 1/8/32/64,
  `--direct=1`) on the data volume. 4k ≈ index lookups, 16k ≈ rescore object
  reads, 48k ≈ posting reads; QD1 ≈ today's sequential path, QD32/64 ≈ the
  Phase 1 parallel path.
- **`tools/dev/hfresh_bench_capture.sh`** — run during a benchmark window:
  iostat 1s samples + pprof CPU/goroutine/heap/mutex from
  `GO_PROFILING_PORT` (default 6060).

Benchmark recipe on the rig:

```
# once
./tools/dev/hfresh_rig_baseline.sh /var/lib/weaviate baseline_out

# per run
HFRESH_SEARCH_PROFILE=1 weaviate ... &
./tools/dev/hfresh_bench_capture.sh 180 run_out &
<drive load, e.g. ann-benchmark -q against the instance>
grep hfresh_search_profile weaviate.log | tail -1   # phase/IO breakdown
```

### First rig findings (GCP, 1B × 256d, n2-highmem-32, 4TB pd-ssd)

The first profiled run surfaced a bottleneck none of the local runs could
see, plus two data points that reshape the model:

1. **Lazy version-map population dominated everything** (fixed on this
   branch). `scan_mean=2.7s` for ~14.5k candidates at probe=16 — ~190µs per
   candidate vs ~90ns local. Cause: `VersionMap` filled its in-memory paged
   array lazily, so every first-touched vector ID during the scan fell back
   to a random LSM point read (~14.5k hidden IOs/query, dwarfing the ~85
   posting + 350 rescore IOs). At 1B IDs the lazy cache takes hours to warm.
   Fix: `VersionMap.Restore` bulk-loads versions at startup (mirrors
   `PostingSizes.Restore`). Note: main has the same flaw, so main-vs-branch
   comparisons before this fix mostly measured version-map misses.
2. **`duplicate_pct=3` on uniform random data** vs 58.6 on dbpedia-1M:
   replica IO waste is a property of clustered real-world data, not of the
   algorithm. Random-data rigs understate it; dbpedia-style data overstates
   nothing — both numbers are real, per workload.
3. **`segments_per_posting=3.84`** on the restored 1B index (vs 1.78 on a
   fresh local build) — compaction state on big indexes is worth ~2× on
   posting-read IO. Compaction-cadence work (Phase 3) gains priority.
4. Healthy signals: `read_mean=7.6ms` (pipeline hides posting IO even here),
   `rescore_mean=145ms` ≈ 350 fetches ÷ 16 workers × real disk latency —
   the phase behaves as modeled and is the next IO target (adaptive rescore).

## Phase 1 — Recall-invariant wins (land as ready, no gating)

1. **Concurrent posting reads**: bounded-parallelism `MultiGet`
   (~32 in flight, tunable), the analog of SPFresh's SPDK batching.
2. **Concurrent rescore** + **one shared consistent view** for all rescore
   fetches instead of one view acquisition per candidate.
3. **Zero-copy scan**: compute RQ1 distance on raw code bytes without the
   `[]uint64` conversion copy; hoist version-map page/lock acquisition out of
   the per-vector loop; remove per-candidate error wrapping from the hot loop.

Expected effect: latency from ~600 sequential IOs to tens of ms; QPS rises to
the actual disk ceiling.

### Phase 1 implementation status

1. **Done.** `PostingStore.MultiGetWithStats` fans posting reads out over
   bounded workers (strided assignment, order-preserving, per-worker stats
   merged after the barrier). Knob: `HFRESH_READ_CONCURRENCY` (default 16,
   1 = sequential). Applies to every `MultiGet` caller, including merges.
2. **Done.** The rescore phase collects the RQ1 top candidates and fans the
   full-vector fetches out over bounded workers, each with its own top-k and
   reusable read buffer, merged at the end. When the shard provides
   `GetViewThunk` + `TempVectorForIDWithViewThunk` (wired in
   `shard_init_vector.go`), all fetches of one query share a single
   consistent bucket view; otherwise it falls back to `VectorForIDThunk`
   (one view per fetch, still parallel). Knob: `HFRESH_RESCORE_CONCURRENCY`
   (default 16, 1 = sequential). Results are exact-distance based, so
   ordering is identical to the sequential path (verified by test).
3. **Done (pipeline).** Posting reads and the scan are now overlapped:
   `MultiGetStreamWithStats` delivers postings on a fully-buffered channel as
   they arrive, and `SearchByVector` scans each posting while the remaining
   reads are in flight. A single consumer preserves the visited-set and
   merge-enqueue semantics; results are order-independent because rescore
   recomputes exact distances. Profiler timing semantics changed with this:
   `scan_mean` is pure scan time, `read_mean` is the pipeline window minus
   scan time — i.e. time actually spent *waiting* on reads. On the rig, the
   scan cost should disappear inside the IO window (`read_mean` >> scan).
   Validated on the 1M sweep: p=1 read_mean 19.9ms→1.35ms (~15×),
   rescore_mean ~7ms→0.64ms (~11×); p=16 (CPU-saturated) QPS +8–20%,
   no regression.
4. **Zero-copy scan dropped — measured, not worth it.** `BenchmarkScanComponents`
   (scan_bench_test.go, M3 Max): per candidate at 1536d the RQ distance
   itself costs 39ns of the 44ns decode+distance total; the `[]uint64`
   decode copy is 6ns, version-map check 7ns, visited-set 2ns. The distance
   estimator (5-bit query code = five Hamming passes) dominates and cannot
   be cut without changing the estimator (recall risk). Zero-copy would buy
   ~3% end-to-end; skip.

## Phase 2 — Adaptive IO-volume cuts (each gated on recall parity)

1. **Adaptive rescore**: process candidates in RQ1-distance order; stop when
   the next candidate's estimated distance (plus a calibrated RQ error margin)
   cannot beat the current exact k-th distance. `rescoreLimit` stays as the cap.
   Expected: ~350 → ~100 fetches on average, full spend only on hard queries.
2. **Adaptive probe termination**: process postings in centroid-distance
   order; stop reading when the working top-`rescoreLimit` set stabilizes
   (no improvement over the last N postings, N tuned for recall parity).
   `searchProbe=256` stays as the cap.

Expected effect: IOs/query from ~600 → ~150–250 average ⇒ QPS ceiling ×2.5–4.
Both mechanisms also shrink per-query variance, which serves the 200ms tail.

## Phase 3 — Verdict & deferred candidates

Re-run the 1B rig, compare measured QPS/latency against the cost model,
decide whether it is enough. Deferred candidates, picked up only if Phase 0/3
counters say they matter:

- Posting-bucket fragmentation / compaction cadence (if segments-per-read ≫ 1).
- Replica-duplicate waste (if duplicate ratio is high at probe=256).
- Exposing `MaxDistanceRatio` as a tunable (upstream never used it; adaptive
  termination largely subsumes it).
- Search-under-update interference (split/merge/reassign + compaction share
  the IOPS budget) — explicitly out of scope for this phase.

## Out of scope

Deadline/drop mechanisms; posting format changes; vector duplication;
caching as a strategy; changing any user-facing default; any recall trade.
