# Conditional-writes client-side verifier (Python)

A reusable Python verification stage that exercises Weaviate's conditional
write endpoint from a real SDK vantage. Parameterized by cluster URL so it
can be pointed at any running single-node or multi-node cluster.

## Quick start

```bash
cd test/conditional_writes_clientcheck

# Sync dependencies (creates .venv via uv)
uv sync

# Run against a local single-node cluster
uv run verify.py --url http://localhost:8080 --grpc localhost:50051

# Run against a 3-node RF3 cluster (point at any one node)
uv run verify.py --url http://node1:8080 --grpc node1:50051 --rf 3

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
| `--burst-uuids` | `5` | Distinct UUIDs the burst threads race on |

## What it checks (Phase 1: insert_if_not_exists)

1. **Cluster readiness**: `client.is_ready()` via the Python client.
2. **Collection create**: via Python client (`RF=<--rf>`, no vectorizer).
3. **First write -> 201 + outcome=inserted**: `POST /v1/objects?condition=insert_if_not_exists`.
4. **Repeat same UUID -> 200 + outcome=skipped**: not an overwrite, not an error.
5. **Aggregate count == 1**: verified via `collection.aggregate.over_all()`.
6. **Concurrent burst**: N threads each race to insert the same K UUIDs;
   asserts each UUID has exactly one insert winner and total count is correct.

Exit code 0 = all checks passed. Exit code 1 = one or more checks failed.
Exit code 2 = `weaviate-client` not installed (run `uv sync` first).

## Which write-path is used

**The Python client does NOT yet have native `?condition=` support.**
First-class Python-client conditional support is tracked as a separate-repo
follow-on in the `weaviate/weaviate-python-client` repository (adding a
`conditional=` keyword argument to `collection.data.insert()`).

This script bridges the gap by issuing the conditional write via **httpx**
directly to the same REST endpoint (`POST /v1/objects?condition=<condition>`)
that the Python client talks to. The Python client is still used for:

- Cluster connect (`weaviate.connect_to_custom(...)`)
- Readiness check (`client.is_ready()`)
- Schema management (`client.collections.create(...)`, `.delete(...)`)
- Aggregate count queries (`collection.aggregate.over_all(total_count=True)`)

Both the Python client and httpx target the same `--url`, so this is a
genuine client-side Python verification.

## Extending for Phase 2 (version-CAS / If-Match)

Pass `--mode version` to run the version-CAS check suite. The suite is
currently stubbed with a FAIL placeholder; implement `run_version_cas()`
in `verify.py` when Phase-2 conditional writes (optimistic concurrency via
`If-Match`) are implemented on the server side.

The `--mode` flag and the `run_version_cas()` function stub make the
structure extensible without restructuring the script.
