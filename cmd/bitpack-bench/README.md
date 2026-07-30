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
- Candidate selection is deliberately the simplest thing that works (full
  sort by distance, truncate). It is the baseline a later selection
  optimisation will be compared against.

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
| `-dims` | 1536 | input dimensionality |
| `-retained` | 1536 | retained dims after rotation (multiple of 64) |
| `-budgets` | `100000,20000,5000,1500,600,350` | survivors kept after each block; last value repeats |
| `-rotate` | true | apply the fast rotation (off = plain truncation) |
| `-seed` | RQ default | rotation seed |
| `-rescore` | 0 | exact-rescore window (0 = all final survivors) |
| `-queries` | 0 | number of queries (0 = all) |
| `-k` | 10 | result count / recall@k |
| `-csv` | `bitpack-bench-results.csv` | CSV file to append results to |

## Output

Per run: recall@10, p50/p95/p99 latency split by stage (block-0 pass,
remaining blocks, exact rescore), average surviving candidates after each
block, and store size (bytes/vector packed, total packed, float bytes held
for rescore). Printed to stdout and appended to the CSV.
