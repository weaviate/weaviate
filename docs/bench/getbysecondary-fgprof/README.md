# GetBySecondaryBatch fgprof wall-clock profiles

Wall-clock (on-CPU + off-CPU) per-phase attribution for the batched secondary
resolver (gh#309), captured with `github.com/weaviate/fgprof`. fgprof samples
every goroutine at 99Hz, so it sees the off-CPU value-read (pread) wait that a
plain `-cpuprofile` misses. Each row's percentage is over the batch-attributed
samples only (samples whose stack touches a batch phase function or the batch
driver); unrelated process goroutines are excluded.

Artifacts per (shape, B): `<shape>-B<size>.pprof` (open with `go tool pprof`)
and `<shape>-B<size>.folded` (open with FlameGraph / speedscope). The `.pprof`
and `.folded` files come from two separate but identical profiling runs of the
same shape (fgprof binds one output format per session).

## warm, B=100

Iterations sampled: 12033. Batch-attributed samples: 918.

| Phase | Samples | % of batch-attributed |
|---|---:|---:|
| phase0_memtables | 5 | 0.5% |
| phase1_index_descents | 12 | 1.3% |
| phase2_value_reads | 843 | 91.8% |
| phase3_recheck | 51 | 5.6% |
| batch_driver | 7 | 0.8% |

## cold, B=100

Iterations sampled: 1220. Batch-attributed samples: 4277.

| Phase | Samples | % of batch-attributed |
|---|---:|---:|
| phase0_memtables | 3 | 0.1% |
| phase1_index_descents | 5 | 0.1% |
| phase2_value_reads | 4260 | 99.6% |
| phase3_recheck | 7 | 0.2% |
| batch_driver | 2 | 0.0% |

## warm, B=500

Iterations sampled: 2195. Batch-attributed samples: 1713.

| Phase | Samples | % of batch-attributed |
|---|---:|---:|
| phase0_memtables | 4 | 0.2% |
| phase1_index_descents | 6 | 0.4% |
| phase2_value_reads | 1682 | 98.2% |
| phase3_recheck | 20 | 1.2% |
| batch_driver | 1 | 0.1% |

## cold, B=500

Iterations sampled: 303. Batch-attributed samples: 4350.

| Phase | Samples | % of batch-attributed |
|---|---:|---:|
| phase0_memtables | 4 | 0.1% |
| phase1_index_descents | 13 | 0.3% |
| phase2_value_reads | 4321 | 99.3% |
| phase3_recheck | 7 | 0.2% |
| batch_driver | 5 | 0.1% |

## Methodology

- Fixture: the warm bench shape (`benchWarmShape`, 13-segment LTK hot-shard shape), page-cache primed before sampling.
- Sample window: 3s per profile at fgprof's 99Hz (~300 samples).
- warm: profiles a timed loop over the real `GetBySecondaryBatchWithView` (nil hook).
- cold: profiles a timed loop over `replaySecondaryBatchWithHook`, a faithful in-package replay of the same phase order calling the SAME production phase functions, with a `secondaryBatchReadHook` sleeping 250µs inside each phase-2 read goroutine.
- Phases are attributed by the phase-function symbol on each sampled stack; the four phase functions are call-tree siblings, so each batch sample maps to exactly one phase (the `batch_driver` row is batch time outside any single phase function).

## Caveat: cold-shape hook scope

The injected latency hook fires only at phase-2 value reads (its only seam), so in the
cold shape phases 0/1/3 still run warm. The cold profile therefore models the shape
where the device value-read cost dominates (the real cold-load concern); it does NOT
model cold index-page or cold recheck-descent latency. The per-phase split is coarse
(~300 samples): read it as "which phase owns the wall time", not as a precise ratio.
