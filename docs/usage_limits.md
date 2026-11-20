# Usage Limits / Free-Tier Guardrails

Server-side guardrails that bound the resources a single Weaviate instance can consume. Designed for the upcoming Weaviate Cloud Free Tier; usable in any deployment that fits the supported deployment shape (see *Scope* below).

All limits are **opt-in**: env vars unset means no enforcement.

> Source of truth for the design: the [RFC](https://www.notion.so/35870562ccd681ce9356e47fa7a37935). This file is the codebase-internal pointer that explains *what is implemented* and *where the hooks live*; the RFC has the full rationale and out-of-scope discussion.

## Environment variables

| Variable | Type | Default | Effect |
|---|---|---|---|
| `USAGE_LIMITS_ERROR_MESSAGE` | string | `"{limit} count limit of {value} reached for this instance."` | Operator-overridable template for the user-facing error message. Placeholders: `{limit}` (resource type) and `{value}` (configured threshold). |
| `MAXIMUM_ALLOWED_OBJECTS_COUNT` | int | `-1` (unlimited) | Cap on live object count per node, summed across the node's loaded shards, or only the namespace's shards on namespaced clusters. Checked on every single + batch insert at the storage chokepoint, except on the target of a `MOVE` that is not yet `READY` (see *Accepted imperfections*). |
| `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` | int | `-1` (unlimited) | Cap on number of collections. Cluster-global when `NAMESPACES_ENABLED=false`; per namespace when `NAMESPACES_ENABLED=true`. |
| `MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on tenants per multi-tenant collection. Checked at tenant-create time only. |
| `MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION` | int | `-1` (unlimited) | Cap on `desiredCount` of a class create request's `shardingConfig`. Config-time check. |

All values are **runtime-overrideable** via the existing runtime overrides YAML file (see `RUNTIME_OVERRIDES_*`). Field names in the YAML are the lowercase-snake-case forms of the env-var names.

### Required pairing: `REPLICATION_MAXIMUM_FACTOR=1`

On clusters with namespaces disabled, the object/tenant/shard caps need the RF=1 deployment shape (see *Scope* below). There, when **any** of `MAXIMUM_ALLOWED_OBJECTS_COUNT`, `MAXIMUM_ALLOWED_TENANTS_PER_COLLECTION`, or `MAXIMUM_ALLOWED_SHARDS_PER_COLLECTION` is set, you must also set `REPLICATION_MAXIMUM_FACTOR=1`. Startup fails otherwise. `REPLICATION_MAXIMUM_FACTOR` also caps the per-class `replicationConfig.factor` for new classes, so the invariant holds at runtime too.

Namespaced clusters (`NAMESPACES_ENABLED=true`) are exempt and boot with the caps at any `REPLICATION_MAXIMUM_FACTOR`.

`MAXIMUM_ALLOWED_COLLECTIONS_COUNT` is **not** part of the linkage — it predates this RFC and tying it would break existing operators.

## Where each check fires

| Limit | Hook | File |
|---|---|---|
| Objects (single) | `Shard.PutObject` (top of function, before LSM write) | `adapters/repos/db/shard_write_put.go` |
| Objects (batch) | `Shard.PutObjectBatch` (top of function) | `adapters/repos/db/shard_write_batch_objects.go` |
| Objects (replicated single) | `Shard.preparePutObject` (before the commit task is stored) | `adapters/repos/db/shard_replication.go` |
| Objects (replicated batch) | `Shard.preparePutObjects` (before the commit task is stored) | `adapters/repos/db/shard_replication.go` |
| Collections | `usecases/schema/class.go` `AddClass()` | `usecases/schema/class.go` |
| Tenants | `usecases/schema/tenant.go` `AddTenants()` | `usecases/schema/tenant.go` |
| Shards | `usecases/schema/class.go` `AddClass()` (sharding-config validation) | `usecases/schema/class.go` |

The object check sits at the **storage chokepoint** rather than at the use-case layer. A write to a single-replica shard with no active replica movement converges at `Shard.PutObject{,Batch}`, whether it arrived locally or was forwarded from another node. Any other write, one during a replica movement included, is checked at `Shard.preparePutObject{,s}` on every write replica but the target of a `MOVE` that is not yet `READY`. Repair and change-log replay writes skip the check, because they copy objects the cluster already accepted. The use-case layer (`usecases/objects/`) does not enforce the object cap; that hook was deliberately removed when we moved the chokepoint deeper.

The schema-side limits (collections, tenants, shards) stay at the use-case layer because that's the single coordinator path — `AddClass`/`AddTenants` go through RAFT, no forwarded-write concern.

## Counter source

The object count is **node-wide** across local shards: the manager sums each loaded shard's `bucket.CountAsync()` (`adapters/repos/db/lsmkv/bucket.go`) on every enforced write. Each `CountAsync()` is O(segments-per-shard) — it walks the live segment list and sums each segment's already-loaded net-additions counter, no I/O. For the Free-Tier shape (few shards, few segments) that's a handful of atomic reads on the hot path.

On namespace-enabled clusters the chokepoint passes the (namespace-qualified) class name and `CheckObjects` extracts the namespace; the counter then sums only indices in that namespace. The cap is applied **per namespace per node**, not cluster-wide. A plain (non-qualified) class name sums all indices (NS-disabled clusters; the global slice on NS-enabled clusters that have no namespaced classes yet).

We deliberately don't route through `UsageForIndex` — that path triggers other usage-module computations beyond a count.

The collection count is **schema/RAFT-backed** and checked twice. `AddClass()` fails fast on the node-local schema via `schemaReader.CollectionsCount(namespace)`, which can lag the leader. The authoritative check runs in the schema FSM's `PreApplyFilter` before the `AddClass` commits. When `NAMESPACES_ENABLED=false` the namespace selector is empty and the response is the cluster-global `len(s.classes)`. When `NAMESPACES_ENABLED=true` the selector is the caller's `principal.Namespace` (namespaced creates already require a namespaced principal) and the count is restricted to stored class names whose `<namespace>:` prefix matches. Aliases live outside this map and do not consume the cap.

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

Supported deployment shapes (where the cap is meaningful):

- **Single-node clusters** (the Free Tier sandbox case) — there is no other node.
- **Namespaced clusters, at any replication factor.** Each node checks `MAXIMUM_ALLOWED_OBJECTS_COUNT` against the objects it holds for the namespace, so an object in an RF=3 collection counts once on each of the three nodes holding its replicas. A collection or tenant gets a replica on the namespace's first home node when it is created or explicitly unfrozen. The exceptions are a first home node that is not a storage candidate, and a receiving node that has not yet applied the namespace's current `home_nodes` (see *Accepted imperfections*). So the first home node counts every object in the namespace once, and its cap acts as the namespace's cap. Its refusal fails a write past the cap where the consistency level needs its yes. *Accepted imperfections* names the levels that commit such a write without it, among them the default `QUORUM` at a factor of 3 or more.

Running a namespaced cluster above factor 1:

- **Update a namespace's `home_nodes`, or set a replication factor above 1, only once every node runs this version.** An older node applies a tenant add that this version refuses.
- **Raise `REPLICATION_MAXIMUM_FACTOR` on every node before creating a collection above the old value, or a namespace without `home_nodes`.** A node still at the old value lists such a collection but leaves it unloaded until it restarts with the new value. A namespace created through it gets the old number of home nodes. While a node still at the old value is the leader, creating such a collection returns an error although the create committed. Every tenant add to it fails the same way. A `QUORUM` write to such a collection at RF=2 fails where a node still at the old value holds one of its replicas.
- **A namespace cannot drop a home node lost for good while the shorter list would fall below `REPLICATION_MINIMUM_FACTOR` or below a collection's replication factor.** To drop it, first lower each such collection's factor. Where that factor already equals `REPLICATION_MINIMUM_FACTOR`, every node must first restart with a lower minimum. The node can instead stay listed until it rejoins under its name, or a new node can be swapped into each namespace's list in its place.
- **With `REPLICATION_MAXIMUM_FACTOR` at or above the storage-node count, every namespace created without `home_nodes` lists every node.** Removing a node for good then takes one `home_nodes` update per namespace, because no endpoint updates them in bulk. Before each update, lower any collection whose factor equals that namespace's home-node count.

**Out of scope:**

- **RF > 1 on clusters with namespaces disabled.** Every write replica except the target of a `MOVE` that is not yet `READY` checks the cap at prepare, but each counts only its own node's objects, so the cap is not exact across replicas.
- **Hypothetical multi-node, non-namespaced, RF=1, single-shard clusters where collections are distributed across nodes.** Each node would only see its local slice of the count, so the effective cap stacks (`cap × min(N_collections, N_nodes_with_shards)`). Not a deployment shape we ship the cap in.

## Backward-compat note: collections-limit status code

The pre-existing `MAXIMUM_ALLOWED_COLLECTIONS_COUNT` enforcement previously returned **HTTP 422 Unprocessable Entity** with a free-text "maximum number of collections" message. As of this release it returns **HTTP 429** with the structured `USAGE_LIMIT_EXCEEDED` body described above. Clients matching on the prior 422 status or message text must adapt.

## Accepted imperfections

- **Object count via async path.** Counts come from `CountAsync` and exclude the in-memory memtable, so during fast bulk imports the count lags slightly behind on-disk state. Bounded by in-flight write volume between count refreshes; self-corrects on the next flush. Sync counting on every write would scan the entire memtable — wasteful at the 10K free-tier scale, fatal at 10M+ scale.
- **Cold lazy-load shards are skipped from the sum.** Including them wouldn't force a load (counts can be read from on-disk segment metadata), but it would add a directory walk + per-segment metadata read per cold shard on every write — unacceptable on the hot path. Effect: accounts with dormant tenants may sit slightly under-counted. Future option: cache cold counts in memory at load time.
- **Per-shard-slice batch rejection** on multi-shard collections (see *Batch behavior*). Single-shard collections (Free Tier) see whole-batch rejection unchanged.
- **Tenants checked at create time only**, not on subsequent multi-tenancy config changes.
- **Concurrent writes near the cap can overshoot it.** Each write checks the count before it lands, so writes racing each other can all pass the check.
- **Schema-side caps are not transactional with RAFT.** Read-check-write is not atomic across the RAFT-replicated `AddClass`/`AddTenants` call, so two concurrent creates can both pass the check. Bounded overshoot; next request is correctly rejected.
- **Repair writes past the cap.** A write can commit without one replica, because that replica was down, unreachable or refused it at its cap. Repair then copies the write onto that replica, past its cap.
- **A `MOVE`'s target skips the cap until the `MOVE` is `READY`.** A put the target refused could otherwise commit on the source alone at `ONE`, and the `MOVE` then deletes the source's copy. So from `INTEGRATING` on, the target's yes alone commits a put at `ONE`, and from `DEHYDRATING` on, when the target is the shard's only write replica, at every consistency level. From `READY` on the target counts only its own node's objects, so a namespace whose shards a `MOVE` split across nodes is capped per node. Nothing times the window out: it lasts until every node in the RAFT configuration has reported `INTEGRATING` and then `DEHYDRATING`, so a node that is down holds it open until it returns or is removed from the cluster. A `MOVE` cannot be cancelled once its target has joined the sharding state; only force-deleting the operation ends the window early.
- **One replica's yes can commit past another's cap.** Whenever a shard has more than one write replica, the coordinator commits once the consistency level's count of replicas say yes. That covers a namespaced collection above factor 1, a shard during a movement or after a `COPY`, and a cap set by runtime override on an RF>1 cluster. So at `ONE`, one replica's yes commits a write another refused at the cap.
- **Missed deletes keep counting.** A replica that was down while objects were deleted still counts them when it returns. Until async replication deletes them there (its hashtree init plus one hashbeat cycle), it refuses capped puts to the shards it holds, and a put whose consistency level needs its yes fails. With async replication disabled, or a class on the `NoAutomatedResolution` deletion strategy, it lasts until an operator deletes the objects on that replica.
- **In a namespace mixing replication factors, the first home node can hold more objects than the cap.** The first home node holds a replica of every collection, so it reaches the cap first. A write it refuses still commits at a level that does not need its yes: `ONE` at a factor of 2 or more, and the default `QUORUM` at 3 or more. Repair then copies the write onto it past the cap. On three home nodes with an RF=1 and an RF=3 collection, the first home node can reach about twice the cap at the default level. That happens when the RF=1 collection fills first. Take H home nodes holding one collection at each factor from 1 to H, all written at `ONE`. When the collections fill in ascending order of factor, the k-th home node can reach about (H−k+1) times the cap, so the first home node can reach about H times.
- **Five operations leave a collection or tenant without a replica on the first home node.** Nodes that count only part of the namespace then decide its writes, so the namespace can hold up to the cap on each node:
  - A collection create, tenant add or explicit unfreeze while the first home node is not a storage candidate.
  - A tenant add, explicit unfreeze or collection create received by a node that has not yet applied a `home_nodes` change. The leader admits a tenant's node list while it is still `home_nodes` entries in order, and a collection's shard while each of its replicas is on a `home_nodes` entry. It judges both against the `home_nodes` it has applied.
  - A `home_nodes` update that changes the first entry. Existing shards stay where they are, and collections and tenants created once the receiving node has applied the update are placed on the new first entry.
  - The automatic activation of a `FROZEN` tenant by a read, a write or the object-TTL sweep.
  - A replica movement off the first home node.
- **A tenant add or explicit unfreeze with a node list shorter than the replication factor is appended to the RAFT log when the leader has not yet applied the factor raise or the collection.** The node that received the request sends every one of the namespace's `home_nodes` that is a storage candidate. So the list is short only when fewer of them than the factor were candidates there, and that node had not yet applied the factor raise or the collection. The leader refuses such a list before the append, but it judges against the factor it has applied. So a factor raise or collection create it has not yet applied lets the entry through. So does an unfreeze of a tenant it holds `FREEZING` whose freeze completes before the entry applies. Every node refuses such an entry at apply, logs "apply command" at Error for it and counts it in `weaviate_cluster_store_fsm_apply_failures_total`. It does both again each time it replays the entry.
- **A refused put waits for the slowest replica.** Where the limit refusals alone make the consistency level unreachable, the coordinator still waits for every write replica's prepare answer before it returns the 429. So a stalled replica delays the 429 by up to the coordinator's 20-second deadline.

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
