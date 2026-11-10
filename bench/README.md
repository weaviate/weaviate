# TachiomFresh benchmarks

Scripts to rerun the TachiomFresh and HFresh + MUVERA benchmarks of this fork
and to compare and plot their results.

The benchmarks themselves are Go tests behind the build tag `nfcorpus`:

- `adapters/repos/db/vector/tachiomfresh/nfcorpus_test.go`: TachiomFresh
  (`TestNFCorpusFrontier`, `TestNFCorpusDynamics`, `TestNFCorpusFiltered`,
  `TestNFCorpusMemoryBreakdown`, `TestNFCorpusRoutingQuality`, ...).
- `adapters/repos/db/vector/hfresh/nfcorpus_muvera_test.go` and
  `nfcorpus_muvera_dynamics_test.go`: HFresh + MUVERA, the baseline
  (`TestNFCorpusMuvera`, `TestNFCorpusMuveraDynamics`).

The doc comment of each test says what it measures and how.

## What is in this folder

| File | What it does |
|---|---|
| `convert_beir_npy.py` | Converts a downloaded Hugging Face dataset folder into an MVC1 cache, the corpus file the tests read. It checks the ground truth against an exact MaxSim before it writes anything. |
| `export_token_ids.py` | Writes the token-id side file the TachiomFresh tests need. |
| `bench.sh` | Compiles and runs one test from the working tree, in a detached tmux session or in the foreground. |
| `bench-frozen.sh` | Runs one test from a test binary built earlier ("frozen"), so two versions can be measured side by side. |
| `queue-v7.0.sh` | An example queue: a B-A-A-B comparison of two frozen TachiomFresh binaries. |
| `compare_cells.py` | Compares two frontier result files cell by cell. |
| `plot_compare.py` | Pareto figures, build-time, memory figures and markdown tables, TachiomFresh against HFresh + MUVERA. |
| `plot_dynamics.py` | Figures and table of the update benchmark (`TestNFCorpusDynamics`, `TestNFCorpusMuveraDynamics`). |

## Layout

The scripts expect a project folder that holds the fork and the data next to
it:

```
$BENCH_HOME/
  <fork>/               this repository; bench/ is this folder
  datasets/
    raw/<dataset>_<encoder>/          the Hugging Face downloads
    <dataset>-<encoder>.bin           MVC1 caches (convert_beir_npy.py)
    <dataset>-<encoder>.tokenids.bin  token ids (export_token_ids.py)
  results/<root>/runs/<dataset>/...   test outputs (created by the scripts)
  results/<root>/plots/, tables/      figures and tables (plot scripts)
  indexes/                            saved indexes and the object store
  bin/                                frozen test binaries
```

The fork is the directory that contains `bench/`. `BENCH_HOME` defaults to
the fork's parent; set and export it to put the data elsewhere. The folder
name of the fork does not matter.

The shell scripts map a dataset argument to a cache file:

| Argument | Cache | Script |
|---|---|---|
| `nfcorpus`, `scidocs`, `fiqa` | `$BENCH_HOME/datasets/<dataset>-colbertv2.bin` | both |
| `nfcorpus-lateon`, `scidocs-lateon`, `fiqa-lateon` | `$BENCH_HOME/datasets/<dataset>-lateon.bin` | `bench-frozen.sh` only |
| `lotte` | `$BENCH_HOME/datasets/lotte-full.bin` | `bench.sh` only |

`lotte-full.bin` is an MVC1 cache of LoTTE lifestyle encoded with ColBERTv2.
No script in this folder writes it (see "Limits" below).

## Requirements

- Go (the version in `go.mod`), `tmux`, GNU coreutils (`timeout`,
  `md5sum`, `date -Is`).
- Python 3 with `numpy` (convert and token ids), `matplotlib` and `seaborn`
  (plots). `compare_cells.py` needs the standard library only. The plots use
  LaTeX for text if a `latex` binary is on `PATH`, else matplotlib's own
  Computer Modern fonts.
- A Hugging Face client to download the data, e.g. `huggingface_hub`
  (`hf` command; older releases call it `huggingface-cli`).

## Get a dataset

The data are on Hugging Face as `tuskanny/<dataset>_<encoder>`:

| | ColBERTv2 (`colbert-ir/colbertv2.0`) | LateOn (`lightonai/LateOn`) |
|---|---|---|
| NFCorpus | `tuskanny/nfcorpus_colbertv2` | `tuskanny/nfcorpus_lateon` |
| SCIDOCS | `tuskanny/scidocs_colbertv2` | `tuskanny/scidocs_lateon` |
| FiQA | `tuskanny/fiqa_colbertv2` | `tuskanny/fiqa_lateon` |

Each repository holds `documents.npy`, `doclens.npy`, `doc_ids.npy`,
`token_ids.npy`, `queries.npy`, `query_lens.npy`, `queries_ids.npy`,
`gt_top100.tsv`, `qrels.test.tsv` and a README that describes the encoding.

1. Download, for example:

   ```
   hf download tuskanny/nfcorpus_colbertv2 --repo-type dataset \
       --local-dir $BENCH_HOME/datasets/raw/nfcorpus_colbertv2
   ```

2. Convert to the MVC1 cache (the command lines of the script's usage doc):

   ```
   python3 bench/convert_beir_npy.py \
       -input $BENCH_HOME/datasets/raw/nfcorpus_colbertv2 -o $BENCH_HOME/datasets/nfcorpus-colbertv2.bin
   python3 bench/convert_beir_npy.py \
       -input $BENCH_HOME/datasets/raw/scidocs_colbertv2 -o $BENCH_HOME/datasets/scidocs-colbertv2.bin -expect-docs 25657
   ```

   `-expect-docs` is the dataset's document count, checked against the
   files; it defaults to 3633 (NFCorpus). FiQA takes `-expect-docs 57638`.
   The script prints the result of each of its seven checks and stops
   without writing the cache if the ground truth does not match an exact
   MaxSim. Keep its output next to the results if you want a record.

3. Write the token ids:

   ```
   python3 bench/export_token_ids.py \
       $BENCH_HOME/datasets/raw/nfcorpus_colbertv2 \
       $BENCH_HOME/datasets/nfcorpus-colbertv2.tokenids.bin
   ```

   Every TachiomFresh test fails without this file
   (`TACHIOMFRESH_TOKENIDS`); the HFresh tests do not read it.

## Run one benchmark

```
bench/bench.sh <dataset> <kind> <out-name> [VAR=value ...]
```

- `kind`: `frontier` (`TestNFCorpusFrontier`), `routing`
  (`TestNFCorpusRoutingQuality`), `dynamics` (`TestNFCorpusDynamics`),
  `muvera` (`TestNFCorpusMuvera`), `muvera-dynamics`
  (`TestNFCorpusMuveraDynamics`).
- `out-name`: the output file, `$BENCH_HOME/results/x86/runs/<dataset>/<out-name>.txt`.
  It may contain a subfolder (`v7.0/frontier`). `BENCH_RESULTS` replaces
  `results/x86` with another root under `$BENCH_HOME` (the object-store
  runs use `results/x86-lsm`).
- `VAR=value ...`: environment for the test. `TACHIOMFRESH_MVC1` is set by
  the script; pass everything else, with absolute paths (the test runs in
  the fork's directory). Values cannot contain spaces.

The script runs `go test -count=1 -tags=nfcorpus -run '<Test>$' -v -timeout 900m`
on the package, in a tmux session named after the output file (`tmux ls`
shows it). `BENCH_FOREGROUND=1` runs it in the calling shell instead, for
chaining in a queue. The first line of the output file is the full
environment and command, the second the start time, the last the end time.

Example:

```
export BENCH_HOME=/data/tachiom
BENCH_RESULTS=results/x86-lsm bench/bench.sh scidocs frontier mine/frontier \
    TACHIOMFRESH_TOKENIDS=$BENCH_HOME/datasets/scidocs-colbertv2.tokenids.bin \
    TACHIOMFRESH_MPS=64 TACHIOMFRESH_KC=4,8,16,32,64 TACHIOMFRESH_KDOCS=50,100,200,500,1000 \
    TACHIOMFRESH_INDEX_DIR=$BENCH_HOME/indexes/scidocs/mine \
    TACHIOMFRESH_OBJECT_STORE=$BENCH_HOME/indexes/objects
```

The main variables (the test files' doc comments have the details):

| Variable | Read by | Meaning |
|---|---|---|
| `TACHIOMFRESH_MVC1` | all | the MVC1 cache; set by the scripts |
| `TACHIOMFRESH_TOKENIDS` | TachiomFresh tests | the token-id side file; required |
| `TACHIOMFRESH_MPS` | all | comma-separated maxPostingSize values, one build each |
| `TACHIOMFRESH_KC`, `TACHIOMFRESH_KDOCS` | TachiomFresh | search grid: kCentroids and kDocsToScore |
| `TACHIOMFRESH_PROBE`, `TACHIOMFRESH_RESCORE` | HFresh + MUVERA | search grid: searchProbe and rescoreLimit |
| `TACHIOMFRESH_INDEX_DIR` | TachiomFresh frontier, routing, dynamics, filtered; `TestNFCorpusMuvera` | keeps each build in `<dir>/tachiomfresh-mps<k>` (or `hfresh-muvera-mps<k>`) and reuses it on the next run instead of rebuilding; unset, everything is in temp dirs. `TestNFCorpusFiltered` needs a saved index. |
| `TACHIOMFRESH_OBJECT_STORE` | both packages | serves the documents from an LSM objects bucket in `<dir>/<cache name>-objects`, read the way a shard reads them, instead of from the memory-mapped cache. Filled on first use and shared by later runs. |
| `TACHIOMFRESH_REASSIGN` | TachiomFresh | reassign variant: a neighbour count, or `off` |
| `TACHIOMFRESH_FILTERS`, `TACHIOMFRESH_FILTER_MODES` | `TestNFCorpusFiltered` | filter fractions in permille; modes `flat`, `widen` |
| `TACHIOMFRESH_VERIFY_GT` | `TestNFCorpusDynamics` | `1` brute-forces the ground truth after each delete phase |
| `TACHIOMFRESH_HEAP_PROFILE` | TachiomFresh | writes heap profiles to `<value>-<tag>.pprof` |

Unset grid variables fall back to each test's default grid. Builds run in
parallel (GOMAXPROCS-1 inserting goroutines); searches run on one core.

## Compare two versions: frozen binaries, B-A-A-B

To compare two versions, freeze each as a test binary in `$BENCH_HOME/bin/`:

```
cd <fork>
go test -c -tags=nfcorpus -o $BENCH_HOME/bin/tachiomfresh-<version>.test ./adapters/repos/db/vector/tachiomfresh/
go test -c -tags=nfcorpus -o $BENCH_HOME/bin/hfresh-<version>.test ./adapters/repos/db/vector/hfresh/
```

and run them with

```
bench/bench-frozen.sh <version> <dataset> <frontier|routing|dynamics|muvera|filtered> <out-name> [VAR=value ...]
```

which takes the same arguments and writes the same output as `bench.sh`.
The `muvera` kind runs `bin/hfresh-<version>.test`, every other kind
`bin/tachiomfresh-<version>.test`.

`queue-v7.0.sh` is a worked example. Run it in tmux:

```
tmux new -d -s queue-v70 bench/queue-v7.0.sh
```

It

1. freezes the working tree as `bin/tachiomfresh-v7.0.test`;
2. runs a build run of v7.0 on SCIDOCS ColBERTv2, maxPostingSize 64, which
   saves its index under `indexes/scidocs/v7.0-lsm/`;
3. runs the two binaries on the same search grid in the order v7.0, v6.0,
   v6.0, v7.0, each pass on a fresh copy of its own version's saved index,
   into `results/x86-lsm/runs/scidocs/baab-v6.0-v7.0/<n>-<version>.txt`.

The B-A-A-B order puts each version once early and once late, so a slow
drift of the machine over the queue weighs on both versions alike. The
queue logs its progress to `results/x86-lsm/runs/queue-v7.0.log`.

It expects, already in place: `bin/tachiomfresh-v6.0.test`, the v6.0 index
`indexes/scidocs/v6.0-lsm/tachiomfresh-mps64` (from an earlier build run of
that binary with `TACHIOMFRESH_INDEX_DIR=$BENCH_HOME/indexes/scidocs/v6.0-lsm`),
and the SCIDOCS cache and token ids. For another pair of versions, copy the
script and change the versions, the dataset and the grid.

## Read and compare results

Each measured search setting is one line in the output file:

```
cell index=tachiomfresh maxPostingSize=.. kCentroids=.. kDocsToScore=.. recall@10=.. qps=.. mean_ms=..
```

`go test -v` prefixes the lines it prints with `file.go:N: `; the Python
scripts strip that prefix. Build lines (`build index=...`) carry the build
time, RAM and disk of each build; `memory index=...` lines split the RAM
into its parts.

`compare_cells.py` compares two frontier files over the cells both have:

```
python3 bench/compare_cells.py <base frontier.txt> <new frontier.txt> [--name-base v6.0 --name-new v7.0]
```

It prints the recall@10 difference and QPS ratio per cell, their median,
quartiles and extremes, how many cells are higher and lower (overall and
per maxPostingSize), the fastest cell of each run at recall@10 >= 0.90,
0.95 and 0.99, and the build lines side by side. With a B-A-A-B queue, the
two passes of the same binary (1 and 4, 2 and 3) show how much QPS moves
between runs on that machine; compare the cross pairs against that.

The plot scripts read a results root laid out as the scripts write it. A
relative `--results` is taken under `$BENCH_HOME`. Run them with
`python3` but without `-I` (`plot_dynamics.py` imports `plot_compare.py`
from its own folder), and create `<results root>/tables/` first:

```
python3 bench/plot_compare.py scidocs --results results/x86-lsm [--version v7.0] [--final]
python3 bench/plot_dynamics.py scidocs --results results/x86-lsm [--version v7.0]
```

`plot_compare.py` looks for fixed file names under `runs/<dataset>/`:
`muvera.txt`, `muvera-v1.1.txt`, `muvera-v1.2.txt` (HFresh + MUVERA),
`frontier.txt` and `frontier-v2.0.txt` (TachiomFresh v1.0 and v2.0),
`<version>/frontier.txt` for later versions (each with an optional
`-extended.txt` companion), and `memory-breakdown.txt`. So name the
`out-name` of a run after the series it should appear as. `--version`
picks the newest TachiomFresh version on the figures (default: the newest
with a run). Figures go to `plots/<encoder>/<version>/<corpus>-*.png`,
tables to `tables/<dataset>-<version>-*.md` (`plots/<encoder>/final/` and
`final-` with `--final`).

`plot_dynamics.py` reads `runs/<dataset>/dynamics-hfresh.txt`,
`dynamics-v2.0.txt`, `v3.0/dynamics.txt`, ... and, with `--version v4.3`
or later, `v4.3/dynamics-mps128.txt` and `v4.4/dynamics-mps128.txt`, and
with `--version v7.0`, `v7.0/dynamics-mps128.txt`. The versions from v4.5
to v6.0 have no dynamics series.

## Limits

- `lotte` in `bench.sh` needs `$BENCH_HOME/datasets/lotte-full.bin`, which
  no script here produces; `convert_beir_npy.py` writes the same MVC1
  format, but from the Hugging Face layout above only.
- `bench.sh` knows the ColBERTv2 datasets and `lotte` only, and has no
  `filtered` kind; `bench-frozen.sh` knows the LateOn datasets and
  `filtered`, but not `lotte` or `muvera-dynamics`.
- Neither script adds `TACHIOMFRESH_TOKENIDS`; pass it with every
  TachiomFresh run.
- The plot scripts' series, labels and file names are those of the
  versions measured so far, up to v7.0; a new version needs entries in
  `plot_compare.py`. `--final` draws v6.0.
