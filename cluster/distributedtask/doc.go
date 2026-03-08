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
// If the [Provider] implements [SubUnitAwareProvider], the Scheduler calls OnTaskCompleted
// exactly once when the task reaches a terminal state, regardless of success or failure.
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
// See [DummyProvider] for a complete working example used by acceptance tests.
package distributedtask
