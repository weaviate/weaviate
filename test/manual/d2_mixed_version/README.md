# D2 TRUE mixed-version migration harness

Manual (operator-run) harness that closes the descope documented in
`test/acceptance/multi_node/bm25_v2_compat_test.go`: it builds a REAL
base-main (pre-D2, no V2 reader, no G4 reject-unknown guard) binary and runs
the old<->new migration scenarios the segment-level G4 unit test cannot, in a
real OrbStack cluster.

It is in `test/manual/` (not an automated acceptance package) because it needs
a separately-built base-main image and does container-swap rolling upgrades
that the testcontainer harness does not model.

## What it proves

1. **The danger (reader-ahead-of-writer necessity).** A base-main binary
   opening a V2 segment does NOT silently mis-decode to plausible-but-wrong
   BM25 scores; it FAILS at shard init with a gob preamble mismatch
   (`load property lengths: decode property lengths: gobenc: fast decode
   failed (preamble mismatch), gob fallback also failed: gob: decoding into
   local type *map[uint64]uint32, received remote type unknown type`), which
   makes the collection unqueryable on that node ("tried to browse
   non-existing index"). The SAME V2 segment is read correctly by the D2
   binary (controlled swap on the same volume). This is why the V2 reader +
   G4 guard must ship AHEAD of any V2 writer.
2. **Rolling upgrade base-main -> D2 (reader-ahead order) is safe.** All-base
   (V0) -> roll each node to D2 with write-new OFF (reads V2, writes V0) ->
   enable write-new fleet-wide. BM25 scores stay identical across every node
   at every stage, including a >65535-token doc (uint16 boundary, lossless).
3. **V0/V2 coexistence.** After write-new, nodes carry both V0 (version
   byte 0) and V2 (version byte 1) searchable segments simultaneously, read
   in parity by the D2 binary.
4. **Downgrade D2 -> base-main is a one-way door.** Same mechanism as (1):
   once V2 segments exist, a pre-V2-reader binary cannot open them.

## Build the base-main image

```bash
git -C <d2-worktree> worktree add --detach /tmp/wv-base-main 92e3d5a2
cd /tmp/wv-base-main
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" \
  -o /tmp/base-main-weaviate-linux ./cmd/weaviate-server
mkdir -p /tmp/base-main-imgctx && cp /tmp/base-main-weaviate-linux /tmp/base-main-imgctx/weaviate-linux
# Dockerfile.thin mirrors the repo runtime stage (FROM alpine:3.22 + apk + COPY)
docker build --platform linux/arm64 -t weaviate-test:base-main-preguard \
  -f /tmp/base-main-imgctx/Dockerfile.thin /tmp/base-main-imgctx
git -C <d2-worktree> worktree remove --force /tmp/wv-base-main
```

The D2 image (`weaviate-test:d2-bm25-c75c4bbe` or rebuild via
`make weaviate-image`) is the new binary.

## Isolation

Project `d2prep1`, network subnet `10.98.0.0/16`, ports 8490-8492 /
50490-50492. Tear down only own resources:

```bash
docker compose -p d2prep1 -f docker-compose.mixed.yml down -v
docker compose -p d2prep1 -f docker-compose.singlenode.yml down -v
```

## Run

```bash
# Scenario 2 (rolling upgrade, full staged sequence):
bash rolling_upgrade.sh

# Scenario 1 + 3 (the danger / downgrade), binary-swap on one volume:
SOLO_IMAGE=weaviate-test:d2-bm25-c75c4bbe SOLO_WRITE_V2=true \
  docker compose -p d2prep1 -f docker-compose.singlenode.yml up -d
python3 driver.py create --node http://localhost:8490
python3 driver.py ingest --node http://localhost:8490
python3 driver.py wait   --node http://localhost:8490
# then stop + swap to base-main on the same volume and observe the failure:
docker compose -p d2prep1 -f docker-compose.singlenode.yml stop
docker compose -p d2prep1 -f docker-compose.singlenode.yml rm -f
SOLO_IMAGE=weaviate-test:base-main-preguard \
  docker compose -p d2prep1 -f docker-compose.singlenode.yml up -d
docker logs d2prep1-solo 2>&1 | grep 'failed to reload local index'
```

`driver.py` drives ingest + cross-node BM25 probes over REST; the per-node
scores are the migration-safety evidence (a mis-decode shows as divergent or
absent scores).
