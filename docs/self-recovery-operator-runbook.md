# Operator runbook: recovering a wiped node via log-replay self-recovery

**Scenario:** a healthy multi-node cluster (e.g. 3 nodes); you intentionally start one
node from scratch — drop its data and attach a **fresh persistent volume** — and want it
to fully re-hydrate when it restarts on the empty disk.

**How it works:** a fresh PV means the node's *entire* data dir (including its RAFT state)
is gone. On restart it rejoins the existing cluster as a **wiped joiner via plain RAFT log
replay** — no operator-forced snapshot required — catches its schema up, and pulls each
missing shard from a healthy peer using the same file-copy engine as replica movement.

See also `docs/self-recovery.md` for the design, metrics, and limitations.

## Preconditions (one-time, cluster-wide)

These two flags must already be set on **every** node. Mixed on/off causes RAFT FSM apply
divergence — roll them out everywhere first, *then* operate.

| Env var | Value | Why |
|---|---|---|
| `SELF_RECOVERY_ENABLED` | `true` | Enables the startup hook that pulls a missing shard dir from a peer instead of creating it empty. |
| `REPLICA_MOVEMENT_ENABLED` | `true` | **Required.** Starts the replication engine that processes the copy ops, and exposes the `/replication/*` observability API. (This is the "shard movement" enable.) |

Also confirm:

- The affected collections have **replication factor ≥ 2** (a peer holds the data).
- The cluster is **HEALTHY** and the surviving nodes keep **RAFT quorum** while this node is down.
- The **whole** data dir will be missing — a fresh PV satisfies this. Recovering a single
  deleted shard dir on an otherwise-intact node is **not** supported.

## Optional tuning to speed up the copy

Everything defaults to a conservative value. Raise on the relevant node(s); the simplest
is to set them **cluster-wide**, since any node can be a copy source or target.

| Env var | Default | What it does | Set on |
|---|---|---|---|
| `SELF_RECOVERY_CONCURRENCY` | `10` (max `32`) | How many of this node's shards recover **in parallel** | recovering node |
| `REPLICATION_ENGINE_MAX_WORKERS` | `10` | Concurrent copy **operations** ("shard movement engine workers") | recovering node |
| `REPLICATION_ENGINE_FILE_COPY_WORKERS` | `10` | Parallel **file** transfers within a single shard copy ("file copy workers") | recovering node |
| `REPLICATION_ENGINE_FILE_COPY_CHUNK_SIZE` | `1048576` (**1 MB, in bytes**) | Bytes per gRPC stream chunk; bigger = fewer round-trips on large LSM segments | **source** peers (read on the serving side) |
| `SELF_RECOVERY_BARRIER_TIMEOUT` | `3m` | Not a speed knob — the no-progress fallback before a stuck joiner loads eagerly. Leave default. | recovering node |

**Chunk-size ceiling:** each chunk is one gRPC message, capped by `GRPC_MAX_MESSAGE_SIZE`
(default **~100 MB** / `104858000`). Stay well under it. Memory cost is roughly
`FILE_COPY_WORKERS × chunk_size × concurrent ops`, so don't go to extremes.

A reasonable "go faster" block (a node with many/large shards on fast disk + network):

```bash
SELF_RECOVERY_ENABLED=true
REPLICA_MOVEMENT_ENABLED=true
SELF_RECOVERY_CONCURRENCY=16
REPLICATION_ENGINE_MAX_WORKERS=16
REPLICATION_ENGINE_FILE_COPY_WORKERS=16
REPLICATION_ENGINE_FILE_COPY_CHUNK_SIZE=16777216   # 16 MB (bytes); well under the ~100 MB gRPC cap
```

If the cluster keeps taking **writes during** recovery and you want the node to converge on
those too, keep async replication on for the collections (`replicationConfig.asyncEnabled: true`)
and do **not** set `ASYNC_REPLICATION_DISABLED` — it heals the delta the file-copy didn't capture.

## Step-by-step

1. **Pre-flight.** `GET /v1/nodes?output=verbose` → all nodes `HEALTHY`. Confirm the two
   required flags are set on every node and the collections are RF ≥ 2.
2. **Apply tuning** (optional): add the env block above to the StatefulSet / pod spec.
3. **Take the node down**, preserving its **identity** — same `CLUSTER_HOSTNAME`, RAFT node
   id, and `RAFT_JOIN` peer list — so it rejoins as the *same* member (e.g. scale the
   StatefulSet replica to 0, or delete the pod).
4. **Swap the volume:** delete the old PVC and provision/attach a **new empty PV** at
   `PERSISTENCE_DATA_PATH`. The data dir must come up empty.
5. **Restart the node.** It finds no RAFT state → joins the existing cluster via log replay
   → catches the schema up → the self-recovery hook re-hydrates each shard from a peer.
6. **Monitor:**
   - `GET /v1/nodes?output=verbose` — the node's shards go `status: RECOVERING` →
     `loaded: true` with matching `objectCount`.
   - `GET /v1/replication/replicate/list?targetNode=<node>` — in-flight `SELF_RECOVERY` ops
     (`REGISTERED → HYDRATING → FINALIZING → READY`).
   - `/metrics` — `weaviate_self_recovery_in_progress`,
     `weaviate_self_recovery_completed_total{result="success"}`,
     `weaviate_self_recovery_duration_seconds`.
   - Cluster reads at `consistency=ONE` keep working throughout — the recovering replica is
     excluded from the read/write set until it is ready.
7. **Done when** every shard on the node is `loaded: true` and object counts match the peers.
   The pre-existing data arrives via the file-copy; any writes that landed during recovery
   converge shortly after via async replication.

## If something gets stuck (debug port, gated on `SELF_RECOVERY_ENABLED`)

- Shard stuck `RECOVERING`: `POST /debug/self-recovery/restart?collection=X&shard=Y` —
  abandons the attempt and re-pulls from scratch (valid only while `RECOVERING`).
- No peer has the data (catastrophic loss): `POST /debug/self-recovery/accept-empty?collection=X&shard=Y`
  — accept an empty shard. Confirm via logs/metrics that all peers truly have no data first.
- Alert on `weaviate_self_recovery_no_data_empty_total` (catastrophic) and
  `weaviate_self_recovery_giveup_total` (retries exhausted).

## Notes on naming (vs. common shorthand)

- The "shard movement" enable is **`REPLICA_MOVEMENT_ENABLED`**.
- `REPLICATION_ENGINE_FILE_COPY_CHUNK_SIZE` is in **bytes** (default exactly 1 MB), not a multiplier.
