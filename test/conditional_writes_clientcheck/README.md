# Conditional-writes client-side verifier (Python)

A reusable Python verification stage that exercises Weaviate's conditional
write endpoint from a real SDK vantage. Parameterized by cluster URL so it
can be pointed at any running single-node or multi-node cluster.

## Quick start

```bash
cd test/conditional_writes_clientcheck

# Sync dependencies (creates .venv via uv)
uv sync

# Run Phase 1 (insert_if_not_exists) against a local single-node cluster
uv run verify.py --url http://localhost:8080 --grpc localhost:50051

# Run Phase 2 (version-CAS / If-Match) against a local single-node cluster
uv run verify.py --url http://localhost:8080 --grpc localhost:50051 --mode version

# Run against a 3-node RF3 cluster (point at any one node)
uv run verify.py --url http://node1:8080 --grpc node1:50051 --rf 3
uv run verify.py --url http://node1:8080 --grpc node1:50051 --rf 3 --mode version

# Increase concurrency for the burst test
uv run verify.py --url http://localhost:8080 --grpc localhost:50051 \
    --burst-threads 20 --burst-uuids 10
```

## Arguments

| Flag | Default | Description |
|---|---|---|
| `--url` | `http://localhost:8080` | REST base URL of any cluster node |
| `--grpc` | `localhost:50051` | gRPC address `host[:port]` |
| `--rf` | `1` | Replication factor for the test collection |
| `--mode` | `insert_if_not_exists` | Check suite (`insert_if_not_exists` or `version`) |
| `--burst-threads` | `10` | Concurrent threads in the burst test |
| `--burst-uuids` | `5` | Distinct UUIDs the burst threads race on (Phase 1 only) |

## What it checks (Phase 1: insert_if_not_exists)

1. **Cluster readiness**: `client.is_ready()` via the Python client.
2. **Collection create**: via Python client (`RF=<--rf>`, no vectorizer).
3. **First write -> 201 + outcome=inserted**: `POST /v1/objects?condition=insert_if_not_exists`.
4. **Repeat same UUID -> 200 + outcome=skipped**: not an overwrite, not an error.
5. **Aggregate count == 1**: verified via `collection.aggregate.over_all()`.
6. **Concurrent burst**: N threads each race to insert the same K UUIDs;
   asserts each UUID has exactly one insert winner and total count is correct.

## What it checks (Phase 2: version-CAS / If-Match, `--mode version`)

1. **Cluster readiness**: `client.is_ready()` via the Python client.
2. **Collection create**: via Python client (`RF=<--rf>`, no vectorizer).
3. **Unconditional insert + ETag check**: `POST /v1/objects` (no condition);
   then `GET /v1/objects/{class}/{id}` and verify `ETag: "1"` (version 1).
4. **PUT with matching If-Match -> 200, version bumps**:
   `PUT /v1/objects/{class}/{id}` with `If-Match: "1"` returns HTTP 200 and
   the response `ETag` header contains `"2"`.
5. **PUT with stale If-Match -> 412**:
   `PUT /v1/objects/{class}/{id}` with the old `If-Match: "1"` (version
   already at 2) returns HTTP 412 Precondition Failed; the object is
   unchanged (GET still returns ETag `"2"`).
6. **Concurrent burst**: `--burst-threads` threads each PUT the same object
   with the current ETag simultaneously; exactly ONE thread wins (HTTP 200)
   and the rest receive HTTP 412; the final version is bumped by exactly 1.

Exit code 0 = all checks passed. Exit code 1 = one or more checks failed.
Exit code 2 = `weaviate-client` not installed (run `uv sync` first).

## Which write-path is used

**The Python client does NOT yet have native `?condition=` or `If-Match` support.**
First-class Python-client conditional and If-Match support is tracked as a
separate-repo follow-on in the `weaviate/weaviate-python-client` repository
(adding a `conditional=` keyword argument to `collection.data.insert()` and
an `if_match=` keyword to `collection.data.replace()`).

This script bridges the gap by issuing the conditional write via **httpx**
directly to the same REST endpoints that the Python client talks to:

- Phase 1: `POST /v1/objects?condition=insert_if_not_exists`
- Phase 2: `PUT /v1/objects/{class}/{id}` with `If-Match: "N"` header

The Python client is still used for:

- Cluster connect (`weaviate.connect_to_custom(...)`)
- Readiness check (`client.is_ready()`)
- Schema management (`client.collections.create(...)`, `.delete(...)`)
- Aggregate count queries (`collection.aggregate.over_all(total_count=True)`)

Both the Python client and httpx target the same `--url`, so this is a
genuine client-side Python verification.
