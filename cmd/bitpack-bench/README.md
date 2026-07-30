# bitpack-bench

Standalone benchmark measuring the best-case performance of a **column-major,
bit-packed vector store with progressive elimination** as a candidate scan path
for filtered vector search.

This is an experiment, not a production code path. Scope of this first step:
DBPedia 1M, everything in memory, no filters, no disk, single-threaded — the
most favourable possible setting, to learn the ceiling before adding anything.

## Design

- Vectors are L2-normalized, optionally rotated with the same fast rotation
  RQ/BRQ use (`entities/vectorindex/compression.FastRotation`, 3 rounds),
  truncated to `-retained` dimensions (multiple of 64), and sign-bit packed
  (`compressionhelpers.BinaryQuantizer` encoding) into uint64 words.
- **Column-major layout**: a "block" is 64 dimensions = one uint64 word.
  Block `b` of every vector is contiguous: `codes[b*N + id]`. A scan over one
  block reads one word per vector sequentially, instead of pulling a full
  cache line per vector to use eight bytes of it.
- **Progressive elimination**: block 0 is one sequential pass over the whole
  column (Hamming vs. the query's block-0 word); keep the best `budgets[0]`.
  Each subsequent block accumulates Hamming over survivors only, then prunes
  to `budgets[b]` (last budget repeats once the list is exhausted).
- **Exact rescore**: final survivors are rescored with exact cosine distance
  against the float vectors; top k returned.
- In `schedule` mode, candidate selection is deliberately the simplest thing
  that works (full sort by distance, truncate). It is the baseline a later
  selection optimisation will be compared against.

## Modes

- `-mode=schedule` (default): progressive elimination with the budget
  schedule, sort-based selection (round-1 behaviour, kept reproducible).
- `-mode=full`: honest no-elimination baseline. Reads every block of every
  vector, accumulates full-width Hamming, selects the top `-rescore`
  (default 350) with a bucketed threshold — per-distance id lists plus a
  running threshold, no sort, no heap — then exact-rescores and takes the
  top k by insertion. This is the recall ceiling of the representation.
- `-mode=hybrid`: budget-schedule scan with per-block access-mode selection:
  a block is streamed (full sequential column read) when the survivor count
  is ≥ N/8 and gathered (random access to survivor words only) below that.
  Pruning uses the bucketed selector (no sort). Reports which blocks
  streamed/gathered, measured bytes at 64-byte-line granularity (sampled
  every 16th query, untimed), and bandwidth split by access mode.
- `-mode=rankcurve`: for each prefix depth (64, 128, …, retained), computes
  each sampled query's true neighbours' ranks by prefix-Hamming distance
  under both worst-case ties (all equal-distance vectors counted ahead) and
  expected-case ties (half the tie bucket), and reports p50/p95/p99/max of
  the per-query maximum — the smallest budget at each depth that retains all
  true neighbours. `-rank-queries` (default 400) controls the sample.

`-gen-quantile=q` (with schedule/hybrid modes) generates the budget schedule
from the expected-case rank curve at quantile q instead of taking
`-budgets`: an in-process `-rank-queries` sample, floored at the rescore
count, monotone nonincreasing.

## Centering

`-center` subtracts the dataset mean (computed once at build time) from base
vectors and queries before rotation and sign extraction. Motivation: ada002
embeddings are not centered; sign-quantizing against zero on uncentered data
biases every rotated bit. The exact rescore always uses the uncentered
normalized floats. Note the production `compressionhelpers.BinaryQuantizer`
thresholds against literal zero with no centering upstream.

## Data preparation (one-time)

The repo has no HDF5 reader, so an ann-benchmarks HDF5 file is converted once
to raw little-endian binary with `h5dump` (`brew install hdf5`):

```bash
./convert.sh ~/Documents/datasets/dbpedia-openai-1000k-angular.hdf5
# -> ~/Documents/datasets/dbpedia-openai-1000k-angular.bin/{train.f32,test.f32,neighbors.i32}
```

## Run

```bash
go build -mod=mod -o /tmp/bitpack-bench ./cmd/bitpack-bench
/tmp/bitpack-bench \
  -data ~/Documents/datasets/dbpedia-openai-1000k-angular.bin \
  -retained 1536 \
  -budgets 100000,20000,5000,1500,600,350 \
  -rotate=true \
  -rescore 0 \
  -queries 0 \
  -csv bitpack-bench-results.csv
```

Flags:

| flag | default | meaning |
|---|---|---|
| `-data` | dbpedia 1M bin dir | directory produced by `convert.sh` |
| `-mode` | `schedule` | `schedule`, `full`, or `rankcurve` |
| `-dims` | 1536 | input dimensionality |
| `-retained` | 1536 | retained dims after rotation (multiple of 64) |
| `-budgets` | `100000,20000,5000,1500,600,350` | survivors kept after each block; last value repeats (schedule mode) |
| `-rotate` | true | apply the fast rotation (off = plain truncation) |
| `-center` | false | subtract the dataset mean before rotation/sign extraction |
| `-seed` | RQ default | rotation seed |
| `-rescore` | 0 | exact-rescore window (0 = all survivors in schedule mode, 350 in full mode) |
| `-queries` | 0 | number of queries (0 = all) |
| `-rank-queries` | 400 | sampled queries in rankcurve mode |
| `-k` | 10 | result count / recall@k |
| `-csv` | `bitpack-bench-results.csv` | CSV file to append results to |

## Output

Per run: recall@10, p50/p95/p99 latency split by stage (block-0 pass,
remaining blocks + selection, exact rescore), average surviving candidates
after each block, store size (bytes/vector packed, total packed, float bytes
held for rescore), bytes read per query from the code store, effective scan
bandwidth, and single-threaded QPS. Printed to stdout and appended to the
CSV. `rankcurve` mode prints a per-depth rank table and writes its own CSV
schema.

Round-1 results used an earlier CSV schema without mode/center/bytes/QPS
columns (`results/dbpedia-1M-baseline.csv`); round-2 runs append to new
files.
