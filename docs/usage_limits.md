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

### Required pairing: `REPLICATION_MAXIMUM_FACTOR=1`

The object/tenant/shard caps only work in the RF=1 deployment shape (see *Scope* below). When **any** of `MAXIMUM_ALLOWED_OBJECTS_COUNT`, `MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION`, or `MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION` is set, you must also set `REPLICATION_MAXIMUM_FACTOR=1`. Startup fails otherwise. `REPLICATION_MAXIMUM_FACTOR` also caps the per-class `replicationConfig.factor` for new classes, so the invariant holds at runtime too.

`MAXIMUM_ALLOWED_COLLECTIONS_COUNT` is **not** part of the linkage — it predates this RFC and tying it would break existing operators.

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

The object count is **node-wide** across all local tenants. Per-tenant decision is atomic under `shardCreateLocks.RLock(name)` — a transition is never observed as both or neither:

- **HOT** (loaded) contributes via `shard.ObjectCountAsync()` → `bucket.CountAsync()` (`adapters/repos/db/lsmkv/bucket.go`) — O(segments), atomic counter reads, no I/O.
- **COLD** (data on disk, shard not loaded) contributes from an in-memory cache (`coldObjects` on `*Index`, `adapters/repos/db/usage_limits.go`). Pure RAM read.
- **FROZEN** contributes 0 — data lives in the cloud.

The cache closes an abuse vector that would let an account deactivate tenants to dodge the cap. Allocated only when `partitioningEnabled` (multi-tenant); single-tenant indexes never see it.

We deliberately don't route through `UsageForIndex` — that path triggers other usage-module computations beyond a count.

### Cold-tenant cache lifecycle

The per-Index cache is maintained by hooks at every transition that changes the local on-disk state of a tenant:

| Transition | Hook | Sites |
|---|---|---|
| HOT → COLD | `cacheColdCountFromShard` | `Index.UnloadLocalShard`; `Migrator.UpdateTenants` cold branch; `Migrator.ShutdownShard` |
| COLD → HOT | `dropColdObjectCount` | `Index.initLocalShardWithForcedLoading`; `Index.getOptInitLocalShard` |
| Startup, tenant COLD on disk | `cacheColdCountFromDisk` | `Index.initAndStoreShards` |
| Unfreeze (cloud download) | `cacheColdCountFromDisk` | `Migrator.unfreeze` success branch |
| Tenant deleted | `dropColdObjectCount` | `Index.dropShards`; `Migrator.frozen` |

`cacheColdCountFromShard` snapshots `ObjectCountAsync` before shutdown; `cacheColdCountFromDisk` reads the same per-segment counters from on-disk metadata without loading the shard.

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
- **Lazy-but-not-yet-loaded HOT shards contribute 0** until first access loads them. Matches `ForEachLoadedShard` semantics; bounded — first read or write triggers the load.
- **Cold-cache snapshot excludes the memtable** (`cacheColdCountFromShard` reads `ObjectCountAsync`). Under-counts by at most one memtable's worth per tenant for the COLD period; reactivation drops the cache and the loaded shard counts accurately.
- **Cold-cache can over-count by one tenant on partial-failure deletion** (`shard.drop` fails mid-cleanup; cache drop is intentionally skipped because on-disk state is then unknowable). Stale entry clears at the next lifecycle transition.
- **Per-shard-slice batch rejection** on multi-shard collections (see *Batch behavior*). Single-shard collections (Free Tier) see whole-batch rejection unchanged.
- **Tenants checked at create time only**, not on subsequent multi-tenancy config changes.
- **Schema-side caps are not transactional with RAFT.** Read-check-write is not atomic across the RAFT-replicated `AddClass`/`AddTenants` call, so two concurrent creates can both pass the check. Bounded overshoot; next request is correctly rejected.

---

# Configuration Restrictions

A second class of opt-in guardrails that constrain **what kind** of class an operator's tenants may create — distinct from the usage limits above, which cap **how much** state they can produce. Like usage limits, these are unset by default; existing deployments are unaffected.

## Environment variables

| Variable | Type | Default | Effect |
|---|---|---|---|
| `ALLOWED_VECTOR_INDEX_TYPES` | comma-separated list | unset (no restriction) | Allow-list for class `vectorIndexType` and named-vector `vectorConfig[*].vectorIndexType`. Valid entries: `hnsw`, `flat`, `dynamic`, `hfresh`. |
| `ALLOWED_COMPRESSION_TYPES` | comma-separated list | unset (no restriction) | Allow-list for the compression configured on a class's vector index. Valid entries: `none`, `pq`, `sq`, `rq-1`, `rq-8`, `bq` (same names accepted by `DEFAULT_QUANTIZATION`). Hfresh classes are exempt — hfresh has no compression knobs. |
| `RESTRICTIONS_ERROR_MESSAGE` | string | `"{value} is not allowed for {restriction}. Allowed values: {allowed}."` | Operator-overridable template for the user-facing message. Placeholders: `{restriction}`, `{value}`, `{allowed}`. |

All three are **runtime-overrideable** via the runtime overrides YAML (`allowed_vector_index_types`, `allowed_compression_types`, `restrictions_error_message`).

## Cross-field rules

Validated at startup in `Config.Validate()` (`usecases/config/config_handler.go`):

1. Each entry must be one of the canonical valid values.
2. **Single-entry allow-list:** the matching default (`DEFAULT_VECTOR_INDEX` / `DEFAULT_QUANTIZATION`) must either be unset (in which case it is seeded to the single value) or match it.
3. **Multi-entry allow-list:** the matching default must be explicitly set and present in the list.
4. **Hfresh + compression invariant:** `ALLOWED_VECTOR_INDEX_TYPES=hfresh` (only) paired with a non-empty `ALLOWED_COMPRESSION_TYPES` is rejected at startup — hfresh has no compression. Compression alongside hfresh in a *mixed* allow-list (e.g. `hfresh,hnsw`) is allowed because the non-hfresh members still need a compression policy.

## Common shapes

```yaml
# Force everyone to a single vector index type.
ALLOWED_VECTOR_INDEX_TYPES=hfresh
# DEFAULT_VECTOR_INDEX is seeded to "hfresh"; DEFAULT_QUANTIZATION and
# ALLOWED_COMPRESSION_TYPES must remain unset.
```

```yaml
# Allow hfresh + hnsw with a forced compression on the hnsw side.
ALLOWED_VECTOR_INDEX_TYPES=hfresh,hnsw
DEFAULT_VECTOR_INDEX=hfresh                      # must be set: multi-entry list
ALLOWED_COMPRESSION_TYPES=rq-8
DEFAULT_QUANTIZATION=rq-8                        # seeded if unset
```

```yaml
# Maximum performance, cost no object: hnsw only, no compression.
ALLOWED_VECTOR_INDEX_TYPES=hnsw
ALLOWED_COMPRESSION_TYPES=none
# Defaults seeded to "hnsw" and "none" respectively.
```

## Where each check fires

| Restriction | Hook | File |
|---|---|---|
| Vector index type (legacy + named) | `Handler.validateVectorIndexType` | `usecases/schema/class.go` |
| Compression (legacy + named) | `Handler.validateAllowedCompression` (invoked from `validateVectorSettings`) | `usecases/schema/class.go` |

The compression check inspects user-supplied config only; the default compression applied later (in `enableQuantization`) is guaranteed by startup validation to be in the allow-list, so a request that arrives with no compression block still produces a compatible class.

## Error response

When a class create/update violates a restriction:

- **HTTP**: `422 Unprocessable Entity` with body
  ```json
  {
    "errorCode": "CONFIG_NOT_ALLOWED",
    "restriction": "compression",
    "value": "pq",
    "allowed": ["rq-8"],
    "message": "pq is not allowed for compression. Allowed values: rq-8."
  }
  ```
- **gRPC**: `codes.FailedPrecondition` with `errdetails.ErrorInfo` carrying the same fields under `Reason="CONFIG_NOT_ALLOWED"`, `Domain="weaviate.restrictions"`.

The `errorCode`, `restriction`, `value`, and `allowed` fields are stable wire contract; the `message` is rendered from `RESTRICTIONS_ERROR_MESSAGE` and varies across deployments. Example operator override:

```
RESTRICTIONS_ERROR_MESSAGE=Invalid config: {value} for {restriction} is not allowed on this tier — please upgrade.
```

## Accepted imperfections

- **Compression detection is based on the parsed user config only.** A class submitted with `{"pq": {"enabled": false}}` is treated identically to a class with no compression block at all — both fall through to the default, which startup validation already vetted against the allow-list. The only way to *opt out* of all compression is `skipDefaultQuantization: true`, which the validator surfaces as the value `none`.
- **Hfresh classes bypass the compression check entirely.** That includes named-vector entries whose `vectorIndexType` is `hfresh`.
