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

package distributedtask

import (
	"context"
	"time"
)

// TasksLister is an interface for listing distributed tasks in the cluster.
type TasksLister interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*Task, error)
}

// SchedulerNotifier is implemented by the [Scheduler] and consumed by the
// [Manager] to request an immediate scheduling cycle after a task-state
// change applies via Raft. Without this hook the scheduler only reacts on
// its periodic tick (default 1 minute, see
// DefaultDistributedTasksSchedulerTickInterval), so a barrier opening from
// the last unit's terminal transition is staggered across nodes by up to
// one tick interval. Wake() is non-blocking and may be called from
// performance-sensitive RAFT-apply paths; implementations must coalesce
// rapid-fire calls and never block the caller.
type SchedulerNotifier interface {
	Wake()
}

// TaskCleaner is an interface for issuing a request to clean up a distributed task.
type TaskCleaner interface {
	CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// TaskFinalizer is an interface for issuing a request to transition a task
// from [TaskStatusFinalizing] to [TaskStatusFinished]. The [Scheduler] calls
// this from its tick after [Provider.OnTaskCompleted] returns successfully so
// the FSM-level FINISHED state lines up with "every post-completion callback
// committed cluster-wide" (not just "every unit terminal"). Idempotent at the
// FSM layer — every node's scheduler issues this independently after its
// local OnTaskCompleted returns; only the first commit actually flips the
// status. See the FINALIZING godoc on [TaskStatusFinalizing] for the
// underlying race this discipline fixes.
type TaskFinalizer interface {
	MarkDistributedTaskFinalized(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// PostCompletionAckRecorder is the RAFT-apply hook the [Scheduler] uses
// to publish one node's OnGroupCompleted result (success or failure)
// after its callbacks have returned for every local group in a task.
// The scheduler gates [TaskFinalizer.MarkDistributedTaskFinalized] on
// having an ack from every node that has local units in the task, and
// transitions the task to FAILED if any ack reports failure — which
// makes [UnitAwareProvider.OnTaskCompleted] skip the cluster-wide
// schema flip on that path.
//
// The recorded state is stored on the [Task] (see [Task.PostCompletionAcks])
// and survives RAFT snapshot/restore so a node restart during the
// FINALIZING window does not lose the cluster's collected acks.
//
// See 0-weaviate-issues#214 Gap A for the crash-safety bug this hook
// closes: without it, a node whose RunSwapOnShard silently failed could
// still let the cluster-wide schema flip commit, leaving that replica
// serving wrong-tokenization data with no operator signal.
type PostCompletionAckRecorder interface {
	RecordDistributedTaskPostCompletionAck(
		ctx context.Context,
		namespace, taskID string,
		taskVersion uint64,
		nodeID string,
		success bool,
		errMsg string,
	) error
}

// TaskCompletionRecorder is an interface for recording the completion of a distributed task.
type TaskCompletionRecorder interface {
	RecordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error
	RecordDistributedTaskUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error
	UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error
}

// TaskHandle is an interface to control a locally running task.
type TaskHandle interface {
	// Terminate is a signal to stop executing the task. If the task is no longer running because it already finished,
	// the method call should be a no-op.
	//
	// Terminated task can be started later again, therefore, no local state can be removed.
	Terminate()

	// Done returns a channel that is closed when the task's goroutine exits, whether due to
	// completion, failure, or termination. The scheduler uses this to detect dead handles
	// and allow re-launch of tasks that still have pending work.
	Done() <-chan struct{}
}

// Provider is an interface for the management and execution of a group of tasks denoted by a namespace.
type Provider interface {
	// SetCompletionRecorder is invoked on node startup to register TaskCompletionRecorder which
	// should be passed to all launch tasks so they could mark their completion.
	SetCompletionRecorder(recorder TaskCompletionRecorder)

	// GetLocalTasks returns a list of tasks that provider is aware of from the local node state.
	GetLocalTasks() []TaskDescriptor

	// CleanupTask is a signal to clean up the task local state.
	CleanupTask(desc TaskDescriptor) error

	// StartTask is a signal to start executing the task in the background.
	StartTask(task *Task) (TaskHandle, error)
}

// ConflictDetector is an optional interface providers implement so the
// [Manager.AddTask] RAFT-apply path can reject a new task whose payload
// conflicts with an already-running task in the same namespace.
//
// Motivation: the REST handler holds a per-(collection, property)
// in-memory lock and runs [checkReindexConflict] before submitting,
// which closes the same-node race. But two parallel PUT
// /indexes/{prop} requests served by *different* nodes both pass the
// per-node lock + check (neither has called AddDistributedTask yet at
// the moment they each query the cluster task list) and both submit a
// RAFT task. At that point two reindex migrations race on shared
// on-disk state for the property and one of them ends up FAILED —
// the multi-node face of weaviate/weaviate#10675 (issue tracked as
// "parallel-migration bug #54").
//
// Putting the conflict check inside [Manager.AddTask] under m.mu makes
// it RAFT-deterministic: every node consults the same FSM-stored task
// list at apply time and rejects the duplicate identically, returning
// the conflict error to the originating client.
//
// FSM-determinism contract: CheckConflict MUST be a pure function of
// (newPayload, existingTasks). It must not read mutable process state
// (clocks, network, schema, RNG) — different nodes applying the same
// log entry must reach the same accept/reject decision.
type ConflictDetector interface {
	Provider

	// CheckConflict is called under [Manager.mu] before a new task is
	// appended to the FSM-stored task list. existingTasks is the full
	// namespace-scoped task list at apply time. Return a non-nil error
	// to reject the new task; the error propagates back to the
	// AddDistributedTask caller.
	CheckConflict(newPayload []byte, existingTasks []*Task) error
}

// SchemaMutationDetector is an optional interface providers implement so
// the schema FSM's UpdateProperty apply path can reject external schema
// mutations that would race with one of the provider's in-flight tasks.
//
// Motivation: 0-weaviate-issues#218. A `change-tokenization` reindex
// spawns separate per-shard sub-tasks for the searchable and filterable
// indexes. A DELETE `/index/searchable` arriving mid-flight applies
// `cleanStaleMigrationDirs("<prop>", "searchable")`, which wipes the
// `searchable_retokenize_<prop>_<gen>/` working dir under the still-
// running sub-task. That sub-unit FAILs; the sibling filterable sub-unit
// keeps going and commits its local bucket swap; the per-shard ack
// barrier sees mixed acks → task → FAILED → `flipSemanticMigrationSchema`
// skipped → schema stays at OLD tokenization while the filterable bucket
// on disk now holds NEW-tokenized data. Bucket↔schema inversion (Sev 1),
// same failure family as #214 Gap A but triggered by an external schema
// mutation instead of a crash.
//
// Putting the check inside the schema FSM's UpdateProperty apply makes
// it RAFT-deterministic: every node sees the same distributed-task FSM
// snapshot at the same applyIndex and reaches the same accept/reject
// decision. Symmetric to [ConflictDetector] (which protects in-flight
// tasks from new conflicting tasks); this protects them from out-of-band
// schema mutations during their flight.
//
// FSM-determinism contract: CheckPropertyUpdate MUST be a pure function
// of (className, propertyName, existingTasks). It must not read mutable
// process state (clocks, network, schema, RNG) — different nodes
// applying the same log entry must reach the same accept/reject
// decision.
type SchemaMutationDetector interface {
	Provider

	// CheckPropertyUpdate is called under [Manager.mu] from the
	// schema FSM's UpdateProperty apply path. existingTasks is the
	// full namespace-scoped task list at apply time. Return a
	// non-nil error to reject the property update; the error
	// propagates back to the UpdateProperty caller.
	CheckPropertyUpdate(className, propertyName string, existingTasks []*Task) error
}

// RecoveryAwareProvider is an optional interface providers implement to
// participate in post-restart callback retry. The Scheduler's bootstrap
// pre-mark (which normally suppresses replay of callbacks that fired
// pre-restart) calls into this hook for every terminal task; if the
// provider reports the local-side callback as NOT yet durably complete,
// the scheduler skips the pre-mark for that task so the next tick
// re-fires OnGroupCompleted and the provider's recovery path can
// finish the half-applied work.
//
// Motivating scenario (RollingRestartMidMigration): a node's
// OnGroupCompleted started running, completed swap for 2 of 3 local
// shards, then context-cancelled mid-shutdown of the 3rd shard's
// reindex bucket because the rolling restart began. The task is
// FINISHED in RAFT (the unit-completion was recorded before
// OnGroupCompleted fired), so without this hook the bootstrap pre-mark
// silently suppresses the retry and the 3rd shard stays at the old
// tokenization forever — per-replica divergence (#10675 family).
type RecoveryAwareProvider interface {
	Provider

	// LocalCallbacksDone returns true iff this provider has verified,
	// from durable local state, that OnGroupCompleted (and any
	// follow-up local recovery) has completed successfully for every
	// unit assigned to localNode. Returning false means "the
	// bootstrap pre-mark should NOT suppress callback replay for
	// this task — let OnGroupCompleted re-fire on next tick so the
	// provider can finish recovery."
	//
	// Called from [Scheduler.preMarkTerminalCallbacksLocked] under
	// s.mu, ONCE per terminal task at bootstrap. Implementations
	// should treat this as a cheap on-disk check.
	LocalCallbacksDone(task *Task, localNode string) bool
}

// UnitAwareProvider fires per-group callbacks as groups complete (mid-flight),
// then a global OnTaskCompleted when the task reaches terminal state.
//
// Every unit task has groups. If no explicit GroupID is set, all units
// belong to a single implicit default group (""). This means:
//   - Tasks without groups: OnGroupCompleted fires once with all local units
//     when all units reach terminal state (same effect as having a single group).
//   - Tasks with groups: OnGroupCompleted fires per-group as each completes,
//     even while the task is still STARTED
//
// Callback phases:
//  1. OnGroupCompleted — per group, fires as each group's units all reach terminal
//  2. OnTaskCompleted — fires once on every node after ALL units terminal
type UnitAwareProvider interface {
	Provider
	// OnGroupCompleted fires when all units in a group reach terminal state.
	// localGroupUnitIDs contains ONLY units assigned to THIS node, not all units
	// in the group. If a node has no units in the group, this callback does not
	// fire on that node.
	//
	// Returns a non-nil error iff at least one local unit's
	// post-completion work (e.g. the reindex provider's RunSwapOnShard)
	// failed. The [Scheduler] aggregates errors across this task's
	// groups for THIS NODE and publishes them via
	// [PostCompletionAckRecorder.RecordDistributedTaskPostCompletionAck];
	// any reported failure transitions the task to FAILED in the FSM,
	// which makes OnTaskCompleted skip the cluster-wide schema flip.
	// See 0-weaviate-issues#214 Gap A.
	OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) error
	OnTaskCompleted(task *Task)
}

type UnitStatus string

const (
	UnitStatusPending    UnitStatus = "PENDING"
	UnitStatusInProgress UnitStatus = "IN_PROGRESS"
	UnitStatusCompleted  UnitStatus = "COMPLETED"
	UnitStatusFailed     UnitStatus = "FAILED"
)

// Unit represents a trackable work unit within a distributed task (e.g. a single shard
// in a reindex operation). Units follow the lifecycle PENDING → IN_PROGRESS → COMPLETED/FAILED.
//
// NodeID starts empty (unassigned) and is set on the first progress update. The [Scheduler]
// treats unassigned units as belonging to any node, which is how initial assignment happens:
// the first node to report progress claims the unit.
//
// Unit values are owned by the [Manager] and mutated under its lock. Callers outside the
// Manager should only access units via cloned [Task] snapshots from ListDistributedTasks.
type Unit struct {
	ID         string     `json:"id"`
	GroupID    string     `json:"groupId,omitempty"`
	NodeID     string     `json:"nodeId"`
	Status     UnitStatus `json:"status"`
	Progress   float32    `json:"progress"`
	Error      string     `json:"error,omitempty"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`
}

// UnitSpec defines a unit with an optional group assignment. Used at task creation
// time when units need group membership (e.g. one group per tenant for MT reindex).
type UnitSpec struct {
	ID      string
	GroupID string
}

type TaskStatus string

const (
	// TaskStatusStarted means that the task is still running on some of the nodes.
	TaskStatusStarted TaskStatus = "STARTED"
	// TaskStatusFinalizing means every unit has reached a terminal state and
	// the scheduler is running the task's post-completion callbacks
	// (per-node OnGroupCompleted swap + cluster-wide OnTaskCompleted
	// schema flip for semantic migrations). The task is NOT yet safe to
	// act upon from the API surface: callers polling for "fully done"
	// must wait for [TaskStatusFinished]. Format-only journeys (no
	// post-completion callback work) pass through this state in
	// essentially zero time. See GH 0-weaviate-issues#212 Issues F+G for
	// the underlying race this state fixes.
	TaskStatusFinalizing TaskStatus = "FINALIZING"
	// TaskStatusFinished means that the task was successfully executed by
	// all nodes AND every post-completion callback (per-node swap +
	// cluster-wide schema flip) has run. Callers polling for "fully done"
	// should wait for this status — never for [TaskStatusFinalizing].
	TaskStatusFinished TaskStatus = "FINISHED"
	// TaskStatusCancelled means that the task was cancelled by user.
	TaskStatusCancelled TaskStatus = "CANCELLED"
	// TaskStatusFailed means that one of the nodes got a non-retryable error and all other nodes
	// terminated the execution.
	TaskStatusFailed TaskStatus = "FAILED"
)

func (t TaskStatus) String() string {
	return string(t)
}

// TaskDescriptor is a struct identifying a task execution under a certain task namespace.
type TaskDescriptor struct {
	// ID is the identifier of the task in the namespace.
	ID string `json:"ID"`

	// Version is the version of the task with task ID.
	// It is used to differentiate between multiple runs of the same task.
	Version uint64 `json:"version"`
}

// Task represents a distributed task tracked across the cluster via Raft consensus.
//
// Completion is tracked per-unit. The task finishes when all units reach a terminal
// state. A single unit failure immediately fails the entire task — remaining in-flight
// units are NOT waited for.
//
// Units are always required when creating a task.
type Task struct {
	// Namespace is the namespace of distributed tasks which are managed by different Provider implementations
	Namespace string `json:"namespace"`

	TaskDescriptor `json:",inline"`

	// Payload is arbitrary data that is needed to execute a task of Namespace.
	Payload []byte `json:"payload"`

	// Status is the current status of the task.
	Status TaskStatus `json:"status"`

	// StartedAt is the time that a task was submitted to the cluster.
	StartedAt time.Time `json:"startedAt"`

	// FinishedAt is the time that task reached a terminal status.
	// Additionally, it is used to schedule task clean up.
	FinishedAt time.Time `json:"finishedAt"`

	// Error is an optional field to store the error which moved the task to FAILED status.
	Error string `json:"error,omitempty"`

	// Units tracks per-unit progress. Always non-nil for valid tasks.
	Units map[string]*Unit `json:"units,omitempty"`

	// PostCompletionAcks records per-node confirmations that the node's
	// OnGroupCompleted callbacks completed (success or failure) for
	// every local unit it owned. Keys are node IDs. The map is populated
	// only after the task transitions to FINALIZING and only by the
	// [Scheduler] tick on each node firing
	// [PostCompletionAckRecorder.RecordDistributedTaskPostCompletionAck]
	// once OnGroupCompleted has returned. The scheduler gates
	// [TaskFinalizer.MarkDistributedTaskFinalized] on having an ack
	// from every node with local units (see
	// [Task.MissingPostCompletionAckNodes]); if any ack is
	// [PostCompletionAck.Success]==false the FSM transitions the task
	// to FAILED instead.
	//
	// Nil for tasks created before this branch (backward-compat with
	// older RAFT snapshots) and for tasks whose provider does not
	// implement [UnitAwareProvider] (no OnGroupCompleted to ack).
	//
	// See 0-weaviate-issues#214 Gap A.
	PostCompletionAcks map[string]PostCompletionAck `json:"postCompletionAcks,omitempty"`
}

// PostCompletionAck records one node's OnGroupCompleted result for a
// task. Persisted on the [Task] under the per-node-ID key of
// [Task.PostCompletionAcks] and survives RAFT snapshot/restore so the
// cluster-wide ack barrier is durable across restarts.
type PostCompletionAck struct {
	// Success is true iff the node's OnGroupCompleted (i.e. the per-shard
	// runtime swap / finalize) returned no error for every local unit.
	Success bool `json:"success"`
	// Error captures the aggregated error message when Success==false.
	// Empty when Success==true.
	Error string `json:"error,omitempty"`
	// AckedAt is the wall-clock time the ack was applied on the FSM
	// (set on the apply path, not from the scheduler). Useful for
	// forensics — the gap between AllUnitsTerminal's FinishedAt and the
	// last AckedAt is the FINALIZING window's wall-clock duration on
	// this cluster.
	AckedAt time.Time `json:"ackedAt"`
}

func (t *Task) Clone() *Task {
	clone := *t
	if t.Units != nil {
		clone.Units = make(map[string]*Unit, len(t.Units))
		for k, v := range t.Units {
			uCopy := *v
			clone.Units[k] = &uCopy
		}
	}
	if t.PostCompletionAcks != nil {
		clone.PostCompletionAcks = make(map[string]PostCompletionAck, len(t.PostCompletionAcks))
		for k, v := range t.PostCompletionAcks {
			clone.PostCompletionAcks[k] = v
		}
	}
	return &clone
}

// AllUnitsTerminal returns true if all units are in a terminal state (COMPLETED or FAILED).
func (t *Task) AllUnitsTerminal() bool {
	for _, u := range t.Units {
		if u.Status != UnitStatusCompleted && u.Status != UnitStatusFailed {
			return false
		}
	}
	return true
}

// AnyUnitFailed returns true if any unit has FAILED status.
func (t *Task) AnyUnitFailed() bool {
	for _, u := range t.Units {
		if u.Status == UnitStatusFailed {
			return true
		}
	}
	return false
}

// LocalUnitIDs returns the IDs of units assigned to the given node.
func (t *Task) LocalUnitIDs(nodeID string) []string {
	var ids []string
	for id, u := range t.Units {
		if u.NodeID == nodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// Groups returns the distinct GroupIDs across all units (includes "" for ungrouped).
func (t *Task) Groups() []string {
	seen := map[string]bool{}
	for _, u := range t.Units {
		seen[u.GroupID] = true
	}
	groups := make([]string, 0, len(seen))
	for g := range seen {
		groups = append(groups, g)
	}
	return groups
}

// AllGroupUnitsTerminal returns true if all units in the given group are terminal.
func (t *Task) AllGroupUnitsTerminal(groupID string) bool {
	for _, u := range t.Units {
		if u.GroupID != groupID {
			continue
		}
		if u.Status != UnitStatusCompleted && u.Status != UnitStatusFailed {
			return false
		}
	}
	return true
}

// LocalGroupUnitIDs returns the IDs of units in the given group assigned to the given node.
func (t *Task) LocalGroupUnitIDs(groupID, nodeID string) []string {
	var ids []string
	for id, u := range t.Units {
		if u.GroupID == groupID && u.NodeID == nodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// NodeHasNonTerminalUnits returns true if the given node has units that are not yet terminal.
// Unassigned units (empty NodeID) are considered as belonging to any node.
func (t *Task) NodeHasNonTerminalUnits(nodeID string) bool {
	for _, u := range t.Units {
		if u.Status == UnitStatusCompleted || u.Status == UnitStatusFailed {
			continue
		}
		if u.NodeID == "" || u.NodeID == nodeID {
			return true
		}
	}
	return false
}

// NodesWithLocalUnits returns the set of node IDs that own at least one
// unit assigned to them in this task. Used by the [Scheduler] tick to
// compute the ack-barrier predicate: every such node must record a
// post-completion ack before the task can transition FINALIZING →
// FINISHED. Units with empty NodeID (still PENDING / never claimed) are
// skipped — they cannot have a node-side OnGroupCompleted result yet.
//
// Returned slice is unsorted; callers that need determinism must sort
// it themselves.
func (t *Task) NodesWithLocalUnits() []string {
	seen := map[string]struct{}{}
	for _, u := range t.Units {
		if u.NodeID == "" {
			continue
		}
		seen[u.NodeID] = struct{}{}
	}
	nodes := make([]string, 0, len(seen))
	for n := range seen {
		nodes = append(nodes, n)
	}
	return nodes
}

// MissingPostCompletionAckNodes returns the node IDs that have local
// units in this task but have NOT yet recorded a post-completion ack.
// The scheduler uses this as the gating predicate for
// [TaskFinalizer.MarkDistributedTaskFinalized] — the FINALIZING →
// FINISHED transition must wait until this returns empty, so a node
// whose RunSwapOnShard silently failed cannot let the cluster-wide
// schema flip commit before its ack is recorded as a failure (which
// transitions the task to FAILED and skips the flip).
//
// Returns nil when all expected nodes have acked. Returned slice is
// unsorted.
func (t *Task) MissingPostCompletionAckNodes() []string {
	var missing []string
	for _, node := range t.NodesWithLocalUnits() {
		if _, ok := t.PostCompletionAcks[node]; !ok {
			missing = append(missing, node)
		}
	}
	return missing
}

// AnyPostCompletionAckFailed returns true iff any node has recorded a
// post-completion ack with [PostCompletionAck.Success]==false. The
// FSM uses this on the apply path to flip the task to FAILED — once
// any node reports failure, the schema flip must be skipped and the
// task must not progress to FINISHED.
func (t *Task) AnyPostCompletionAckFailed() bool {
	for _, ack := range t.PostCompletionAcks {
		if !ack.Success {
			return true
		}
	}
	return false
}

type ListDistributedTasksResponse struct {
	Tasks map[string][]*Task `json:"tasks"`
}
