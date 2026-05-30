# Conditional Write Load Harness

Workstream A — performance/load harness for conditional writes
(`insert_if_not_exists`, `update_if_exists`, unconditional baseline).

Parent task: `2026-05-30-0510` (pass-2 perf/scale hardening).

---

## Build

```bash
cd /path/to/weaviate
go build ./test/benchmark/conditional_writes/
# or to place the binary somewhere convenient:
go build -o /tmp/cw-bench ./test/benchmark/conditional_writes/
```

---

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8080` | Weaviate base URL |
| `--mode` | `insert_miss` | Workload (see Modes below) |
| `--class` | `CWBench` | Collection name (deleted+recreated on each run unless `--skip-setup`) |
| `--objects` | `10000` | Total write operations to perform |
| `--concurrency` | `16` | Concurrent writers |
| `--shards` | `1` | Desired shard count |
| `--rf` | `1` | Replication factor |
| `--tenants` | `0` | MT tenant count (0 = MT disabled) |
| `--cl` | `""` | Consistency level: `ONE`, `QUORUM`, `ALL`, or empty (server default) |
| `--output-format` | `json` | `json` \| `csv` \| `human` |
| `--output-file` | `""` | Write results to file (empty = stdout) |
| `--skip-setup` | false | Skip schema creation and pre-seeding |
| `--skip-teardown` | false | Leave the class in place after the run |
| `--hot-key-uuid` | `aaaaaaaa-...` | UUID hammered by all workers in `hot_key` mode |

---

## Modes

| Mode | What it does | Measures |
|------|-------------|---------|
| `insert_miss` | All-new UUIDs via `?condition=insert_if_not_exists` | Miss-path latency |
| `insert_hit` | Pre-seeded UUIDs via `?condition=insert_if_not_exists` | Hit-path (skip) latency |
| `insert_mixed` | 50/50 miss/hit interleaved | Mixed-path throughput |
| `update_exists` | Pre-seeded UUIDs via `?condition=update_if_exists` | Update-path latency |
| `unconditional` | Plain `POST /v1/objects` (no `?condition=`) | Baseline throughput |
| `hot_key` | All writers hammer ONE UUID (`insert_if_not_exists`) | Per-UUID-lock contention |

---

## Topology Matrix — per-topology invocation commands

> All commands assume the harness binary is at `/tmp/cw-bench`.
> Replace `--url` with the actual node address from `compose.ContainerURI(1)`
> or the manually-booted instance.
>
> For `--objects 1000000` runs expect runtime of several minutes;
> for `--objects 100000` expect < 2 min at typical throughput.

### 0. Point at a running cluster

The harness does NOT boot clusters. Point `--url` at any live Weaviate node.

For a `test/docker` compose cluster started by an acceptance test or a
standalone `docker compose up`, the default node-1 address is typically
`http://localhost:8080` for a single-node setup, or the host-mapped port
from `compose.ContainerURI(1)`.

```bash
export CW_URL=http://localhost:8080  # adjust per cluster
```

---

### Topology 1: Single-node (RF=1, 1 shard)

```bash
# insert_miss — 100K objects, 32 writers
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --output-format json \
  --output-file /tmp/results_single_node_insert_miss.json

# unconditional baseline (same params, no condition)
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode unconditional \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --output-format json \
  --output-file /tmp/results_single_node_unconditional.json

# hot_key contention (32 writers → 1 UUID)
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode hot_key \
  --objects 10000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --output-format json \
  --output-file /tmp/results_single_node_hot_key.json
```

---

### Topology 2: 1-shard / RF=3 (HA cluster, 3 nodes)

Boot a 3-node compose cluster first (or use `docker.New().WithWeaviateCluster(3).Start(ctx)`
from a test driver). Point `--url` at node-1's host-mapped port.

```bash
# insert_miss at QUORUM
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_rf3_quorum_insert_miss.json

# insert_miss at ALL
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 3 \
  --cl ALL \
  --output-format json \
  --output-file /tmp/results_rf3_all_insert_miss.json

# unconditional baseline at QUORUM
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode unconditional \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_rf3_quorum_unconditional.json

# insert_hit (all existing UUIDs)
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_hit \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_rf3_quorum_insert_hit.json
```

---

### Topology 3: Multi-shard / RF=3 (N shards, 3 nodes)

```bash
# insert_miss across 3 shards, RF=3
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 3 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_multi_shard_insert_miss.json

# 1M object run (production-scale)
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 1000000 \
  --concurrency 64 \
  --shards 3 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_multi_shard_1m.json

# hot_key contention across shards (UUID hashes to one shard; tests lock within shard)
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode hot_key \
  --objects 50000 \
  --concurrency 32 \
  --shards 3 --rf 3 \
  --cl QUORUM \
  --output-format json \
  --output-file /tmp/results_multi_shard_hot_key.json
```

---

### Topology 4: Multi-tenant (MT, RF=1, N tenants)

```bash
# 10 tenants, insert_miss
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --tenants 10 \
  --output-format json \
  --output-file /tmp/results_mt10_insert_miss.json

# 100 tenants, insert_miss
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode insert_miss \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --tenants 100 \
  --output-format json \
  --output-file /tmp/results_mt100_insert_miss.json

# update_exists across 10 tenants
/tmp/cw-bench \
  --url "$CW_URL" \
  --mode update_exists \
  --objects 100000 \
  --concurrency 32 \
  --shards 1 --rf 1 \
  --tenants 10 \
  --output-format json \
  --output-file /tmp/results_mt10_update.json
```

---

## Results format (JSON)

```jsonc
{
  "timestamp": "2026-05-30T04:00:00Z",
  "mode": "insert_miss",
  "url": "http://localhost:8080",
  "class": "CWBench",
  "objects_requested": 100000,
  "concurrency": 32,
  "shards": 1,
  "replication_factor": 1,
  "tenants": 0,
  "consistency_level": "QUORUM",
  "duration_s": 12.34,
  "ops_total": 100000,
  "ops_per_sec": 8103.0,
  "error_count": 0,
  "error_rate_pct": 0.00,
  "p50_ms": 3.21,
  "p95_ms": 8.44,
  "p99_ms": 15.02,
  "min_ms": 0.81,
  "max_ms": 201.3
}
```

Key fields:

| Field | Meaning |
|-------|---------|
| `ops_per_sec` | Throughput (writes completed / wall-clock seconds) |
| `error_rate_pct` | HTTP 5xx or network error rate as a percentage |
| `p50_ms` / `p95_ms` / `p99_ms` | Latency percentiles in milliseconds |
| `error_count` | Absolute number of failed requests |

The harness exits with code 2 if `error_rate_pct > 10`.

---

## Regression thresholds (suggested starting knobs)

These are initial knobs; the overseer should set them based on the first
baseline run and update this table.

| Metric | Regression if... |
|--------|-----------------|
| `p99_ms` (insert_miss, single-node) | > 3× unconditional baseline p99 |
| `ops_per_sec` (insert_miss) | < 80% of unconditional baseline throughput |
| `error_rate_pct` | > 1% in any steady-state run |
| `p99_ms` (hot_key, 32 writers) | > 500 ms (lock-convoy signal) |

Adjust thresholds after the first cluster run; the table above is a starting
point derived from typical Weaviate write latency at this scale.

---

## Collecting a full topology matrix run

A convenience shell script to run all four topologies sequentially
(single URL; replace `$CW_URL` with the actual endpoint):

```bash
#!/usr/bin/env bash
set -e
BIN=/tmp/cw-bench
URL=${CW_URL:-http://localhost:8080}
OUTDIR=${OUTDIR:-/tmp/cwbench-results}
mkdir -p "$OUTDIR"

for MODE in insert_miss insert_hit insert_mixed update_exists unconditional hot_key; do
  echo "=== mode=$MODE ==="
  $BIN --url "$URL" --mode "$MODE" \
    --objects 100000 --concurrency 32 --shards 1 --rf 1 \
    --output-format json \
    --output-file "$OUTDIR/${MODE}.json"
done

echo "Results written to $OUTDIR/"
ls -lh "$OUTDIR/"
```

---

## Notes for the overseer

1. Build the binary first: `go build -o /tmp/cw-bench ./test/benchmark/conditional_writes/`
2. Each run creates and destroys the `CWBench` class (unless `--skip-setup`/`--skip-teardown`).
   Runs are independent and safe to run sequentially on the same cluster.
3. For 1M-object runs, budget 5-20 minutes per topology depending on cluster throughput.
4. `--skip-setup --skip-teardown` is useful for repeated runs on a pre-seeded class
   (e.g. when varying concurrency while holding topology constant).
5. The harness does NOT require `docker`; it only speaks HTTP to a running Weaviate node.
