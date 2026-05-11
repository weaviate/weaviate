# Shard Self-Recovery

When a node restarts and finds shard directories missing on disk that the
schema says it should be hosting, it pulls those shards from a healthy
peer using the same file-copy machinery as scale-out replication. The
hook fires only at startup; runtime shard creation (new collections,
empty-source replica adds) keeps today's "create empty dir" behavior.

Disabled by default. Operators opt in per the rollout discipline below.

## Enabling

Set on every node:

```
SELF_RECOVERY_ENABLED=true
SELF_RECOVERY_CONCURRENCY=10    # default; per-node parallelism
SELF_RECOVERY_BARRIER_TIMEOUT=3m  # default; wiped-joiner no-progress fallback window (see "How the trigger is scoped")
REPLICA_MOVEMENT_ENABLED=true                  # required for /replication/* observability
```

**Rollout discipline.** Mixed-version clusters with the flag on cause
RAFT FSM apply divergence (older nodes don't know the `SELF_RECOVERY`
transfer type and reject it). Same caveat as `REPLICA_MOVEMENT_ENABLED`.
**Upgrade every node first, then enable the flag.**

## Operator-visible state during recovery

| Surface | Signal |
|---|---|
| `GET /nodes?output=verbose` | shard reports `status: "RECOVERING"`, `loaded: false` |
| `GET /replication/replicate/list?targetNode=<self>` | in-flight `SELF_RECOVERY` op with state `REGISTERED`/`HYDRATING`/`FINALIZING`/`READY` |
| `/metrics` (Prometheus) | series listed below |
| Structured logs | `event=self_recovery.{started\|peer_probe\|op_registered\|completed\|failed\|empty_fallback\|accept_empty\|restart}` |

### Metrics

| Metric | Type | Labels | Question it answers |
|---|---|---|---|
| `weaviate_self_recovery_in_progress` | gauge | — | how many recoveries are running on this node? |
| `weaviate_self_recovery_started_total` | counter | `source_node` | how many were kicked off, by source peer? |
| `weaviate_self_recovery_completed_total` | counter | `result` (success\|failure\|empty_fallback\|cancelled) | terminal outcomes — `empty_fallback` is its own bucket so the benign bootstrap-window case does not inflate failures |
| `weaviate_self_recovery_duration_seconds` | histogram | `result` (same label set as `completed_total`) | end-to-end recovery time |
| `weaviate_self_recovery_no_data_empty_total` | counter | — | catastrophic-wipe occurrences (post-bootstrap; alert on this) |
| `weaviate_self_recovery_no_data_during_bootstrap_total` | counter | — | empty-fallback during the RAFT bootstrap window (likely a class added during this node's downtime — benign; informational) |
| `weaviate_self_recovery_unreachable_peer_total` | counter | `peer` | peer reachability problems |
| `weaviate_self_recovery_giveup_total` | counter | — | retries exhausted |
| `weaviate_self_recovery_accept_empty_total` | counter | — | operator escape-hatch invocations |
| `weaviate_self_recovery_submit_dropped_total` | counter | — | submissions dropped because the in-process worker queue was full (will retry on next restart) |

Per-(collection, shard) drill-down is available via `/replication/replicate/list`
and the structured logs.

### Suggested alerts

`for:` is a Prometheus alert-rule field (not PromQL), so the
"stuck for too long" example needs the full alert-rule form below.
The other two can be expressed as plain PromQL expressions if you
prefer to plug them into your alerting tool directly.

```yaml
groups:
  - name: weaviate-self-recovery
    rules:
      # Catastrophic-wipe candidate — investigate immediately.
      - alert: WeaviateSelfRecoveryEmptyFallback
        expr: rate(weaviate_self_recovery_no_data_empty_total[5m]) > 0
        labels: {severity: critical}
        annotations:
          summary: "self-recovery materialised an empty shard (no peer had data)"

      # Recovery taking too long — likely a peer-reachability or copier issue.
      - alert: WeaviateSelfRecoveryStuck
        expr: weaviate_self_recovery_in_progress > 0
        for: 1h
        labels: {severity: warning}
        annotations:
          summary: "self-recovery in progress for >1h"

      # Retries exhausted — manual intervention required.
      - alert: WeaviateSelfRecoveryGiveUp
        expr: rate(weaviate_self_recovery_giveup_total[15m]) > 0
        labels: {severity: critical}
        annotations:
          summary: "self-recovery gave up after exhausting retries"
```

## Operator escape hatches

The `/debug/self-recovery/*` endpoints (and the test-only
`POST /debug/raft/snapshot`) are registered **only when
`SELF_RECOVERY_ENABLED=true`**. They live on the profiling/debug port,
like the other `/debug/*` handlers.

| Endpoint | When to use |
|---|---|
| `POST /replication/replicate/{id}/cancel` | abandon one in-flight op (any transfer type) |
| `POST /debug/self-recovery/restart?collection=X&shard=Y` | abandon current SELF_RECOVERY attempt for the shard, erase partial `.recovering/` state, start fresh (probe re-randomises source peer selection). **Valid only while the shard is `RECOVERING`** — if the live `<shard>/` directory already exists (recovery completed, or empty-fallback ran) it returns `409 Conflict`; cancel any in-flight op and remove the directory by hand if you really want to re-pull. |
| `POST /debug/self-recovery/accept-empty?collection=X&shard=Y` | declare "no recoverable data exists, accept empty shard". Confirm via metrics/logs that all peers report no data first. |

If retries are exhausted (`weaviate_self_recovery_giveup_total` ticks),
the shard is left in `RECOVERING`; use `restart` to try again from
scratch, or `accept-empty` to accept the loss. (Recoveries are also
retried automatically on the next node restart.)

When the in-process worker queue overflows (a node missing thousands of
shards at once — `weaviate_self_recovery_submit_dropped_total` ticks),
the affected shard falls back to the pre-existing behavior — an empty dir
created at startup, backfilled object-by-object by async replication —
rather than being stranded in `RECOVERING`. The dropped recovery is
re-attempted on the next restart.

## Runbook: `no_data_empty_total > 0` after a restart

This counter increments when **all probed peers definitively reported no
data** for a shard the orchestrator was trying to recover. Either:

1. **Catastrophic full-cluster wipe** (every replica of the shard lost data).
2. **Genuinely-new shard added while the node was offline** (e.g. an
   empty-source `AddReplicaToShard` applied during the node's catchup).
   Benign.

To distinguish, find the structured log line:

```
event=self_recovery.empty_fallback collection=... shard=... probed_peers=[...]
```

- If you recognise the (collection, shard) as one with real data, this
  is case 1 — restore from backup.
- If it's a recently-created or empty replica, case 2 — no action.

To restore from backup: shut down (or quiesce) the node, run the Backup
module's restore for the affected collection, restart.

## How the trigger is scoped

Recovery is triggered by the **startup DB-load pass** — the one-shot
`reloadDBFromSchema` → `ReloadLocalDB` that a node runs once it has
caught its schema up to the cluster's committed state. It fires
regardless of *how* the node caught up: a RAFT snapshot install and a
replay of committed log entries both converge on the same load pass, and
a shard folder found missing during it triggers recovery either way.

### Wiped-node log-replay rejoin (no operator-forced snapshot)

A node that comes up with **no local RAFT state** and the feature enabled is
treated as a *wiped-joiner candidate* (`Store.wipedJoinerCandidate`). It reports
ready eagerly (readiness is not deferred — fresh-cluster formation must not be
gated on a wiped-joiner watcher), but Apply forces every replayed entry
"schema-only" so no shard folders are materialised mid-catch-up. When the node
joins an existing cluster, the leader returns its committed index at join in
`JoinPeerResponse.leader_commit_index` — the **catch-up barrier**: every
pre-existing class's `ADD_CLASS` has an index at or below it. `Store.SetJoinBarrier`
records it, and once Apply has applied up to the barrier it runs the single load
pass (`finishWipedJoinerReload`, on the Apply thread) — which installs
`RecoveringShard` wrappers (excluded from cluster reads while they re-hydrate
from peers, the same way the snapshot-Restore path behaves) instead of
materialising empty shards. This covers the log-replay rejoin path **without** an
operator-forced snapshot; the `POST /debug/raft/snapshot` step is now only a test
convenience for forcing the InstallSnapshot path deterministically.

`Store.watchWipedJoiner` handles the cases the barrier doesn't: a node that
**formed a fresh cluster** (no barrier — nothing to recover) clears the
schema-only suppression once it has caught up to the tiny committed index, and a
joiner that can't reach its barrier (an older leader that supplied no index, or an
unreachable cluster) falls back to an eager load after a no-progress timeout. A
node with intact data, a feature-off node, and a metadata-only voter keep the
legacy eager-ready behaviour. The whole
mechanism is gated on `SELF_RECOVERY_ENABLED`; off ⇒ startup is unchanged. An
older leader (pre-`leader_commit_index`) returns 0, so a new joiner falls back to
the legacy path — recovery on log-replay rejoin engages once both ends are upgraded.

Runtime collection/tenant creation is **not** part of the load pass: a
genuinely new shard has its folder created at creation time, so it is
never mistaken for a wiped one and never triggers recovery.

## Limitations

**Per-shard recovery requires the whole index/data to be missing, not just one
shard folder.** Recovery fires only when a shard is *first built* during the
tagged startup load pass. On a wiped node (whole data dir gone) every shard is
first built there, so each missing one recovers. But on an **otherwise-intact
node** where a single shard folder was deleted, `DB.Open` (which runs before the
tagged pass) rebuilds that shard **empty** from the sharding state first, so the
tagged pass then sees the dir present and skips it — the shard comes up empty
rather than recovering. Recovering a single lost shard on an intact node is not
supported; the supported recovery unit is a wiped data dir.

There is also a brief window during a wiped node's log-replay catch-up
where the node reports `Ready` but its local DB is not yet built (it is
built in one pass at the end of catch-up). Schema replay is fast and the
freshly-rejoined node is not yet in the read rotation for its shards;
the window is the same order as the pre-existing "Ready during replay"
behavior.

**A `RecoveringShard` panics if a non-routed code path touches it.**
While a shard is `RECOVERING`, an in-memory `RecoveringShard` wrapper
sits in the index's shard map with its load blocked. The replication FSM
read filter excludes it from cluster reads/writes for all consistency
levels, and the "loaded" shard accessors skip it — but any maintenance
loop or admin operation that iterates *all* shards and calls a data-path
method (e.g. `Store()`, `addProperty`/`updateProperty` via `ForEachShard`)
will hit `mustLoad` and **panic the node** with a "shard is recovering
from a peer; this code path must not touch a recovering shard" message
rather than failing gracefully. Avoid such operations while a shard on
the node is recovering. (Hardening every such call site is deferred; the
panic message is intentionally explicit so the crash is unambiguous.)

## Maintenance mode

When the node is in maintenance mode, the orchestrator does not start new
recoveries — `Submit` declines the work, and a missing-dir shard
discovered at startup falls back to the normal init path (empty dir +
async-rep backfill) rather than being parked in `RECOVERING`.
Already-running recoveries run to completion. To pause an in-flight one,
cancel via the endpoint above.

## Downgrade safety

If a node is downgraded to a binary without SELF_RECOVERY support, any
leftover `<shard>.recovering/` directories sit unused on disk until
re-upgrade (which sweeps them at startup). To clean manually:

```
rm -rf <data_root>/<collection>/<shard>.recovering
```

(Only safe when the live `<shard>/` exists alongside; otherwise the dir
holds an in-flight recovery to resume.)
