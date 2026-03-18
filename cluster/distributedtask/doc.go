//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package distributedtask coordinates long-running operations (e.g. reindexing) across a
// Weaviate cluster. All task state lives in the Raft log, so it survives node restarts and
// leader elections without external storage.
//
// # Architecture
//
// Three components work together:
//
//   - [Manager] is the Raft state machine. It owns the canonical task state and is the only
//     component that mutates it. All writes go through Raft Apply (see cluster/store_apply.go).
//
//   - [Scheduler] runs on every node. It polls the Manager for the current task list, starts
//     and stops local work via a [Provider], and submits cleanup requests when tasks expire.
//
//   - [Provider] is the extension point. Each task namespace (e.g. "reindex", "compaction")
//     registers a Provider that knows how to execute that type of work locally.
//
// # Unit tracking
//
// Every task declares a set of named [Unit] items (e.g. one per shard). Each
// unit progresses independently through PENDING → IN_PROGRESS → COMPLETED/FAILED.
// The task finishes when all units reach a terminal state. Units are always
// required when creating a task.
//
// # Unit assignment
//
// Units start unassigned (empty NodeID). The [Scheduler] treats unassigned units as
// belonging to any node, so all nodes will start the task. The first node to report progress
// for a unit claims it — subsequent updates from other nodes are rejected. This means
// assignment is implicit and driven by the Provider implementation, not prescribed by the
// framework.
//
// # Failure semantics
//
// When any unit fails, the entire task immediately transitions to FAILED. In-flight
// units on other nodes are NOT waited for — their subsequent completion reports are
// rejected. This fail-fast approach avoids wasting cluster resources on doomed work.
//
// # Group-level finalization
//
// If the [Provider] implements [UnitAwareProvider], the Scheduler fires per-group
// callbacks as groups complete — even while the task is still STARTED:
//
//  1. OnGroupCompleted — fires per group when all units in that group reach a terminal
//     state. Receives the groupID and only the local unit IDs in that group. Fires
//     mid-flight for tasks with explicit groups, enabling per-tenant atomicity for MT reindex.
//     When no explicit GroupID is set, all units belong to default group "" and
//     OnGroupCompleted fires once when all units are terminal (identical to old behavior).
//
//  2. OnTaskCompleted — fires once per node after ALL units reach terminal state.
//     Use this for global operations (e.g. Raft schema update). Since Raft deduplicates,
//     the schema update happens exactly once even though OnTaskCompleted fires on every node.
//
// Both callbacks fire on FINISHED and FAILED tasks so providers can finalize (success) or
// rollback (failure) based on task.Status. Both fire exactly once per task lifecycle.
//
// # Four journey examples
//
// Journey 1: Spread work across any node (no finalization needed).
//
// A data-cleanup provider distributes 1000 files as units. Any node can claim any
// file. No finalization needed — each unit is independent.
//
//	unitIDs := []string{"file-001", "file-002", ..., "file-1000"}
//	raft.AddDistributedTask(ctx, "cleanup", taskID, payload, unitIDs)
//
// The Provider's StartTask iterates units, processes unclaimed ones, and reports
// completion. No UnitAwareProvider needed.
//
// Journey 2: Per-shard work, global finalize (behavior unchanged).
//
// A repair/rebuild provider recreates an index with the same configuration (e.g.,
// fixing a corrupted HNSW or rebuilding blockmax segments). Because the behavior
// is unchanged, shards can swap independently as they finish — there's no need to
// wait for all shards before swapping, since queries produce the same results
// regardless of which format a shard is currently serving from.
//
// Unit IDs are opaque strings. The Provider defines them at task creation time
// and stores any shard→subUnit mapping in the task payload:
//
//	payload := ReindexPayload{
//	    ShardMap: map[string]string{  // unitID → shardName
//	        "su-0": "shard-S1",       // nodeA's replica of S1
//	        "su-1": "shard-S1",       // nodeB's replica of S1
//	        "su-2": "shard-S2",       // ...
//	    },
//	}
//	unitIDs := []string{"su-0", "su-1", "su-2", ...}
//
// Node assignment is automatic: the first node to report progress for a unit
// claims it (Unit.NodeID is set). The Provider's StartTask iterates units,
// checks which local shards it owns, and claims the corresponding units.
//
// Each shard swaps its bucket pointers immediately upon completing its own reindex
// (inside the StartTask goroutine, before calling RecordSubUnitCompletion).
//
// OnGroupCompleted: no-op — each shard already swapped during its own processing.
// OnTaskCompleted: optional — e.g., log completion or flip a cosmetic schema flag.
//
// Journey 3: Per-shard work, per-shard finalize after barrier (behavior changes).
//
// A tokenization-change provider reindexes every shard with new tokenization config
// (e.g., WORD → TRIGRAM). Because the behavior changes, consistency matters: if some
// shards serve old tokenization while others serve new, queries return mixed results.
// ALL shards must finish reindexing before ANY shard swaps to the new format.
//
// Unit IDs and shard mapping work the same way as Journey 2 — the Provider
// defines IDs at creation time and stores the mapping in the task payload.
// The framework only cares about Unit.NodeID for ownership tracking.
//
// During StartTask, each shard reindexes into new segments but does NOT swap yet.
// It reports progress and completion, but the old segments remain active for queries.
//
// OnGroupCompleted: fires on each node AFTER all units across all nodes finish
// (since all units share the default group ""). Receives localGroupUnitIDs —
// the Provider looks up the shard mapping from task.Payload to know which local
// shards to swap. Atomically swaps bucket pointers for each local shard. This is
// a local operation, no Raft needed.
//
// OnTaskCompleted: submits a Raft schema update to change the tokenization config.
// Because Raft deduplicates, the schema update happens exactly once even though
// OnTaskCompleted fires on every node.
//
// The barrier guarantee ensures NO shard swaps until ALL shards finish reindexing.
//
// Journey 4: Per-tenant work with per-tenant finalize (MT reindex with groups).
//
// A multi-tenant reindex provider creates one group per tenant. Each tenant's replicas
// are units in that group. As each tenant's group completes, OnGroupCompleted fires
// mid-flight — the provider atomically swaps that tenant's bucket pointers without
// waiting for other tenants. This provides per-tenant atomicity: if tenant A's group
// completes while tenant B is still reindexing, tenant A starts serving new data
// immediately.
//
//	specs := []UnitSpec{
//	    {ID: "t1__nodeA", GroupID: "tenant-1"},
//	    {ID: "t1__nodeB", GroupID: "tenant-1"},
//	    {ID: "t2__nodeA", GroupID: "tenant-2"},
//	    ...
//	}
//	raft.AddDistributedTaskWithGroups(ctx, "reindex", taskID, payload, specs)
//
// OnGroupCompleted: fires per-tenant as each tenant's replicas all finish.
// Atomically swaps bucket pointers for the local replicas of that tenant.
//
// OnTaskCompleted: fires once when ALL tenants finish. Updates schema with new
// tokenization config. If any tenant failed, task.Status == FAILED — provider
// skips schema update but already-swapped tenants remain valid (independent).
//
// # Progress throttling
//
// Unit progress updates go through Raft consensus. To prevent flooding the log,
// the [Scheduler] wraps the [TaskCompletionRecorder] in a [ThrottledRecorder] that
// forwards progress for each unit at most once per 30 seconds. Completion and
// failure calls are never throttled.
//
// # Adding a new task type
//
// To add a new kind of distributed task:
//
//  1. Define a namespace constant (e.g. "my-reindex").
//  2. Implement [Provider] (or [UnitAwareProvider] if you need group-level callbacks).
//  3. Register the provider in configure_api.go's MakeAppState, keyed by your namespace.
//  4. Create tasks via the Raft endpoint [cluster.Raft.AddDistributedTask], passing
//     unit IDs (at least one unit is always required).
//
// See [ShardNoopProvider] for a complete working example used by acceptance tests.
//
// # Provider idempotency contract
//
// After a node crash, the [Scheduler] re-launches tasks that still have non-terminal
// units. The [Provider] MUST handle re-invocation idempotently:
//
//   - Units in IN_PROGRESS state will be re-delivered to the same node that claimed
//     them. The provider must detect partially-completed work (e.g. via sentinel files)
//     and either resume or restart the unit safely.
//
//   - Units in PENDING state (unclaimed) may be delivered to any node. The provider
//     must tolerate being asked to process a unit that another node is also attempting
//     to claim — only one node's first progress update will succeed. Providers that use
//     per-replica assignment (UnitToNode metadata) avoid this race entirely, since each
//     unit is deterministically assigned to exactly one node at creation time.
//
//   - The framework does NOT re-assign units claimed by a crashed node to other nodes.
//     The crashed node must eventually restart for its IN_PROGRESS units to complete.
//     If a node is permanently lost, the task must be cancelled manually.
//
// Typical idempotency patterns:
//
//   - Check for a completion sentinel file before starting work
//   - Use atomic file operations (write-to-temp + rename) for crash safety
//   - Store progress checkpoints that allow resuming from the last known good state
package distributedtask
