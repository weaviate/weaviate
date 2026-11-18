# v4.0: the posting-edge index read from disk, next to a real HFresh

What this run measures, why, and how it is read is in `bench-report.md` of the
package, section "v4.0: primer prototipo que lee de disco". This file is only
how to repeat it, on Linux in particular.

## What you need

- Go, and this repository checked out at the commit under test.
- The dataset as a directory with `meta.txt`, `train.f32`, `test.f32` and
  `neighbors.i32` (dbpedia-openai 1M, 1536 dimensions, cosine).
- A directory on the disk under test with some 20 GB free, and about 16 GB
  of memory for the two builds.
- `python3`, for the verdict and for dropping the page cache without root.

## One command

```bash
DATASET=/data/dbpedia-openai-1000k-angular.bin DISK=/mnt/nvme/v40 OUT=/tmp/v40 \
  adapters/repos/db/vector/postingedge/testdata/bench/v4.0/scripts/run_v40_disk.sh
```

It runs six stages in order; any of them can be named to run alone
(`run_v40_disk.sh timed verdict`):

| stage | what it does | timed |
|---|---|---|
| `bin` | builds the two benchmark binaries into `$WORK` | no |
| `write` | builds posting-edge in batch, writes its five cells under `$DISK/pe`, and applies the two checks that stop the run: disk returns what memory returns, and the bytes the store reads are within 25 % of the byte model | no |
| `hfresh` | builds HFresh with its default configuration under `$DISK/hf` and sweeps `searchProbe` | no |
| `count` | counts what the store reads per query for HFresh | no |
| `timed` | times every cell and HFresh, warm and cold, one process each | yes |
| `verdict` | writes `$OUT/disk-verdict.md` | no |

The results are JSON files in `$OUT`, and the logs and binaries in `$WORK`
(`$OUT/work` by default). `$OUT` must not be under `$DISK`.

## Cold measures

Before every cold batch the index is closed and the system's cache of its
files is dropped. `COLD` chooses how:

- `fadvise`, the default on Linux: `posix_fadvise(DONTNEED)` on every file
  under `$DISK`, through `scripts/drop_cache.py`. No root.
- `cmd`: your own command in `DROP`, for instance as root
  `COLD=cmd DROP='sync; echo 3 > /proc/sys/vm/drop_caches'`.
- `image`, the default on macOS: `$DISK` is the mountpoint of a disk image
  that is detached and attached again. A disk image serves one read at a
  time, so it hides what parallel reads buy; see "Enmienda 1" in the report.
- `none`: warm only.

After the drop the index is reopened and the index of every segment of the
store is read back, so that the data is cold and the lookups are not.

## A smoke first

Thirty thousand vectors, a few minutes, same code path:

```bash
DISK=/mnt/nvme/v40-smoke OUT=/tmp/v40-smoke QUERIES=500 COLDQ=40 \
  EXTRA_PE="-bench.limit 30000" EXTRA_HF="-bench.hfresh.limit 30000" \
  adapters/repos/db/vector/postingedge/testdata/bench/v4.0/scripts/run_v40_disk.sh
```

## What to keep in mind

- The store reads through its mapping (the default of the LSM store), in both
  indexes. Its read counts come from a separate pass with `pread`, the only
  mode it counts in.
- HFresh rescores 350 candidates with exact vectors from a bucket named
  `objects` that holds the vector alone; posting-edge rescores 64 from a file
  of its own. The rescore is reported apart for that reason.
- The HFresh package starts a pprof server on 127.0.0.1:6060 and a metrics
  server on :2112 in every test binary: do not run two of its binaries at
  once.
- Time on a quiet machine. `NIGHT=1` (macOS) waits for 23:00 with the machine
  awake and idle.
