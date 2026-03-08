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
// # Tracking modes
//
// A task uses one of two mutually exclusive tracking modes, chosen at creation time:
//
// Legacy (node-level): each node reports "I'm done" via RecordNodeCompletion. The task
// finishes when all cluster nodes have reported. Use this for operations where every node
// must participate (e.g. a cluster-wide config change).
//
// Sub-unit: the task declares a set of named [SubUnit] items (e.g. one per shard). Each
// sub-unit progresses independently through PENDING → IN_PROGRESS → COMPLETED/FAILED.
// The task finishes when all sub-units reach a terminal state. Use this for operations
// where work is divided into independent pieces that may be distributed unevenly across
// nodes.
//
// # Sub-unit assignment
//
// Sub-units start unassigned (empty NodeID). The [Scheduler] treats unassigned sub-units as
// belonging to any node, so all nodes will start the task. The first node to report progress
// for a sub-unit claims it — subsequent updates from other nodes are rejected. This means
// assignment is implicit and driven by the Provider implementation, not prescribed by the
// framework.
//
// # Failure semantics
//
// When any sub-unit fails, the entire task immediately transitions to FAILED. In-flight
// sub-units on other nodes are NOT waited for — their subsequent completion reports are
// rejected. This fail-fast approach avoids wasting cluster resources on doomed work.
//
// # Per-node sub-unit finalization
//
// If the [Provider] implements [SubUnitAwareProvider], the Scheduler fires two callbacks
// when a sub-unit task reaches a terminal state (FINISHED or FAILED):
//
//  1. OnSubUnitsCompleted — fires on each node that owns sub-units. The callback receives
//     only the sub-unit IDs assigned to that node. Skipped on nodes with no local sub-units.
//     Use this for per-shard finalization (e.g. atomically swapping bucket pointers).
//
//  2. OnTaskCompleted — fires after OnSubUnitsCompleted on every node, regardless of whether
//     it owns sub-units. Use this for global operations (e.g. Raft schema update). Since Raft
//     deduplicates, the schema update happens exactly once even though OnTaskCompleted fires
//     on every node.
//
// Both callbacks fire on FINISHED and FAILED tasks so providers can finalize (success) or
// rollback (failure) based on task.Status. Both fire exactly once per task lifecycle.
//
// # Three journey examples
//
// Journey 1: Spread work across any node (no finalization needed).
//
// A data-cleanup provider distributes 1000 files as sub-units. Any node can claim any
// file. No finalization needed — each sub-unit is independent.
//
//	subUnitIDs := []string{"file-001", "file-002", ..., "file-1000"}
//	raft.AddDistributedTask(ctx, "cleanup", taskID, payload, subUnitIDs)
//
// The Provider's StartTask iterates sub-units, processes unclaimed ones, and reports
// completion. No SubUnitAwareProvider needed.
//
// Journey 2: Per-shard work, global finalize (behavior unchanged).
//
// A repair/rebuild provider recreates an index with the same configuration (e.g.,
// fixing a corrupted HNSW or rebuilding blockmax segments). Because the behavior
// is unchanged, shards can swap independently as they finish — there's no need to
// wait for all shards before swapping, since queries produce the same results
// regardless of which format a shard is currently serving from.
//
// Sub-unit IDs are opaque strings. The Provider defines them at task creation time
// and stores any shard→subUnit mapping in the task payload:
//
//	payload := ReindexPayload{
//	    ShardMap: map[string]string{  // subUnitID → shardName
//	        "su-0": "shard-S1",       // nodeA's replica of S1
//	        "su-1": "shard-S1",       // nodeB's replica of S1
//	        "su-2": "shard-S2",       // ...
//	    },
//	}
//	subUnitIDs := []string{"su-0", "su-1", "su-2", ...}
//
// Node assignment is automatic: the first node to report progress for a sub-unit
// claims it (SubUnit.NodeID is set). The Provider's StartTask iterates sub-units,
// checks which local shards it owns, and claims the corresponding sub-units.
//
// Each shard swaps its bucket pointers immediately upon completing its own reindex
// (inside the StartTask goroutine, before calling RecordSubUnitCompletion).
//
// OnSubUnitsCompleted: no-op — each shard already swapped during its own processing.
// OnTaskCompleted: optional — e.g., log completion or flip a cosmetic schema flag.
//
// Journey 3: Per-shard work, per-shard finalize after barrier (behavior changes).
//
// A tokenization-change provider reindexes every shard with new tokenization config
// (e.g., WORD → TRIGRAM). Because the behavior changes, consistency matters: if some
// shards serve old tokenization while others serve new, queries return mixed results.
// ALL shards must finish reindexing before ANY shard swaps to the new format.
//
// Sub-unit IDs and shard mapping work the same way as Journey 2 — the Provider
// defines IDs at creation time and stores the mapping in the task payload.
// The framework only cares about SubUnit.NodeID for ownership tracking.
//
// During StartTask, each shard reindexes into new segments but does NOT swap yet.
// It reports progress and completion, but the old segments remain active for queries.
//
// OnSubUnitsCompleted: fires on each node AFTER all sub-units across all nodes finish.
// Receives localSubUnitIDs — the Provider looks up the shard mapping from
// task.Payload to know which local shards to swap. Atomically swaps bucket
// pointers for each local shard. This is a local operation, no Raft needed.
//
// OnTaskCompleted: submits a Raft schema update to change the tokenization config.
// Because Raft deduplicates, the schema update happens exactly once even though
// OnTaskCompleted fires on every node.
//
// The barrier guarantee ensures NO shard swaps until ALL shards finish reindexing.
//
// # Progress throttling
//
// Sub-unit progress updates go through Raft consensus. To prevent flooding the log,
// the [Scheduler] wraps the [TaskCompletionRecorder] in a [ThrottledRecorder] that
// forwards progress for each sub-unit at most once per 30 seconds. Completion and
// failure calls are never throttled.
//
// # Adding a new task type
//
// To add a new kind of distributed task:
//
//  1. Define a namespace constant (e.g. "my-reindex").
//  2. Implement [Provider] (or [SubUnitAwareProvider] if you need sub-unit tracking).
//  3. Register the provider in configure_api.go's MakeAppState, keyed by your namespace.
//  4. Create tasks via the Raft endpoint [cluster.Raft.AddDistributedTask], passing
//     sub-unit IDs if using sub-unit tracking (nil for legacy mode).
//
// See [ShardNoopProvider] for a complete working example used by acceptance tests.
package distributedtask
