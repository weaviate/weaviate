# Usage Limits / Free-Tier Guardrails

Server-side guardrails that bound the resources a single Weaviate instance can consume. Designed for the upcoming Weaviate Cloud Free Tier; usable in any deployment that fits the supported deployment shape (see *Scope* below).

All limits are **opt-in**: env vars unset means no enforcement.

> Source of truth for the design: the [RFC](https://www.notion.so/35870562ccd681ce9356e47fa7a37935). This file is the codebase-internal pointer that explains *what is implemented* and *where the hooks live*; the RFC has the full rationale and out-of-scope discussion.

## Environment variables

| Variable | Type | Default | Effect |
|---|---|---|---|
| `USAGE_LIMITS_ERROR_MESSAGE` | string | `"{limit} count limit of {value} reached for this instance."` | Operator-overridable template for the user-facing error message. Placeholders: `{limit}` (resource type) and `{value}` (configured threshold). |
| `MAXIMUM_ALLOWED_OBJECTS_COUNT` | int | `-1` (unlimited) | Cap on live object count, summed across all loaded local shards (node-wide). Checked on every single + batch insert at the storage chokepoint. |
| `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` | int | `-1` (unlimited) | Cap on number of collections. Pre-existing env var; behavior preserved. |
| `MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on tenants per multi-tenant collection. Checked at tenant-create time only. |
| `MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on `desiredCount` of a class create request's `shardingConfig`. Config-time check. |

All values are **runtime-overrideable** via the existing runtime overrides YAML file (see `RUNTIME_OVERRIDES_*`). Field names in the YAML are the lowercase-snake-case forms of the env-var names.

## Where each check fires

| Limit | Hook | File |
|---|---|---|
| Objects (single) | `Shard.PutObject` (top of function, before LSM write) | `adapters/repos/db/shard_write_put.go` |
| Objects (batch) | `Shard.PutObjectBatch` (top of function) | `adapters/repos/db/shard_write_batch_objects.go` |
| Collections | `usecases/schema/class.go` `AddClass()` | `usecases/schema/class.go` |
| Tenants | `usecases/schema/tenant.go` `AddTenants()` | `usecases/schema/tenant.go` |
| Shards | `usecases/schema/class.go` `AddClass()` (sharding-config validation) | `usecases/schema/class.go` |

The object check sits at the **storage chokepoint** rather than at the use-case layer. That covers both writes that arrive locally and writes that were forwarded from another node — both converge at `Shard.PutObject{,Batch}` on the home node for RF=1. The use-case layer (`usecases/objects/`) does not enforce the object cap; that hook was deliberately removed when we moved the chokepoint deeper.

The schema-side limits (collections, tenants, shards) stay at the use-case layer because that's the single coordinator path — `AddClass`/`AddTenants` go through RAFT, no forwarded-write concern.

## Counter source

The object count is **node-wide** across local shards: the manager sums each loaded shard's `bucket.CountAsync()` (`adapters/repos/db/lsmkv/bucket.go`) on every enforced write. Each `CountAsync()` is O(1), so a node with a small handful of collections (the Free-Tier shape) costs a handful of atomic reads on the hot path.

We deliberately don't route through `UsageForIndex` — that path triggers other usage-module computations beyond a count.

## Error response

When any limit is hit, Weaviate returns:

- **HTTP**: `429 Too Many Requests` with body
  ```json
  {
    "errorCode": "USAGE_LIMIT_EXCEEDED",
    "limit": "objects",
    "value": 10000,
    "message": "Object count limit of 10000 reached for this instance."
  }
  ```
- **gRPC**: `codes.ResourceExhausted` with `errdetails.ErrorInfo` carrying the same `limit`/`value`/`message` fields under `Reason="USAGE_LIMIT_EXCEEDED"`, `Domain="weaviate.usagelimits"`.

The structured fields (`errorCode`, `limit`, `value`) are stable contract regardless of the `USAGE_LIMITS_ERROR_MESSAGE` template — only the human-facing `message` text changes.

### Batch behavior

When a batch insert would exceed `MAXIMUM_ALLOWED_OBJECTS_COUNT`, the **shard-slice is rejected** as a unit:

- **Single-shard collections (Free Tier):** whole-batch rejection. No partial fill.
- **Multi-shard collections:** `Index.putObjectBatch` partitions a client batch by shard *before* forwarding, so the chokepoint sees one slice per shard. A single client batch can therefore produce per-shard partial success on multi-shard collections — accepted under our current scope (see *Scope* below).

## Scope

Supported deployment shapes (where the cap is meaningful and exact):

- **Single-node clusters** (the Free Tier sandbox case) — there is no other node.
- **Namespaced clusters in phase 1** — a namespace's collections/shards are pinned to a single node, so the per-namespace sum is local.

**Out of scope:**

- **RF > 1.** The replicated write path bypasses `Shard.PutObject{,Batch}` (it goes through `shard_replication.go`'s `preparePutObject{,s}` → `s.putOne` / `s.putBatch` directly). Supporting RF>1 would require either dropping the check one level deeper or a smarter scheme like a lease-based quota.
- **Hypothetical multi-node, non-namespaced, RF=1, single-shard clusters where collections are distributed across nodes.** Each node would only see its local slice of the count, so the effective cap stacks (`cap × min(N_collections, N_nodes_with_shards)`). Not a deployment shape we ship the cap in.
- **Phase-2 namespaces** that spread a namespace's collections across nodes — same problem as the previous bullet.
- **Cluster-wide enforcement.** Reserved for the future namespaces work; not API-stubbed here.

## Backward-compat note: collections-limit status code

The pre-existing `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` enforcement previously returned **HTTP 422 Unprocessable Entity** with a free-text "maximum number of collections" message. As of this release it returns **HTTP 429** with the structured `USAGE_LIMIT_EXCEEDED` body described above. Clients matching on the prior 422 status or message text must adapt.

## Accepted imperfections

- **Object count via async path.** Counts come from `CountAsync` and exclude the in-memory memtable, so during fast bulk imports the count lags slightly behind on-disk state. Bounded by in-flight write volume between count refreshes; self-corrects on the next flush. Sync counting on every write would scan the entire memtable — wasteful at the 10K free-tier scale, fatal at 10M+ scale.
- **Per-shard-slice batch rejection** on multi-shard collections (see *Batch behavior*). Single-shard collections (Free Tier) see whole-batch rejection unchanged.
- **Tenants checked at create time only**, not on subsequent multi-tenancy config changes.
