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
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/models"
)

// TaskLister is an interface for listing distributed tasks in the cluster.
type TaskLister interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*Task, error)
}

// TaskHeaderLister lists tasks without their units, for callers that read only headers, statuses and groups.
type TaskHeaderLister interface {
	ListDistributedTaskHeaders(ctx context.Context) (map[string][]*Task, error)
}

type TaskCopySource interface {
	UpdateTaskCopy(c *TaskCopy) error
}

// TaskCopy holds the [Scheduler]'s own copy of every task, updated by [Manager.UpdateTaskCopy]. The scheduler also mirrors this node's post-completion ack into it until that ack's apply brings the FSM's value.
type TaskCopy struct {
	tasks   map[string]map[string]*Task
	changed map[TaskDescriptor]*changedGroups
}

func (c *TaskCopy) changedOf(desc TaskDescriptor) *changedGroups {
	if c.changed == nil {
		c.changed = map[TaskDescriptor]*changedGroups{}
	}
	if c.changed[desc] == nil {
		c.changed[desc] = &changedGroups{}
	}
	return c.changed[desc]
}

func (c *TaskCopy) takeChanged() map[TaskDescriptor]*changedGroups {
	changed := c.changed
	c.changed = nil
	return changed
}

type changedGroups struct {
	all    bool
	groups map[string]struct{}
}

func (g *changedGroups) add(group string) {
	if g.groups == nil {
		g.groups = map[string]struct{}{}
	}
	g.groups[group] = struct{}{}
}

func (g *changedGroups) merge(other *changedGroups) {
	g.all = g.all || other.all
	for group := range other.groups {
		g.add(group)
	}
}

// take returns the groups of pending it names and empties g, in time proportional to the groups it names.
func (g *changedGroups) take(pending map[string]struct{}) []string {
	named := g.groups
	if g.all {
		named = pending
	}
	var groups []string
	for group := range named {
		if _, ok := pending[group]; ok {
			groups = append(groups, group)
		}
	}
	*g = changedGroups{}
	return groups
}

// Tasks returns the copy's tasks, which the next update may change.
func (c *TaskCopy) Tasks() map[string]map[TaskDescriptor]*Task {
	result := make(map[string]map[TaskDescriptor]*Task, len(c.tasks))
	for namespace, tasks := range c.tasks {
		if len(tasks) == 0 {
			continue
		}
		result[namespace] = make(map[TaskDescriptor]*Task, len(tasks))
		for _, task := range tasks {
			result[namespace][task.TaskDescriptor] = task
		}
	}
	return result
}

func (c *TaskCopy) set(namespace, taskID string, task *Task) {
	if c.tasks[namespace] == nil {
		c.tasks[namespace] = map[string]*Task{}
	}
	c.tasks[namespace][taskID] = task
}

// SchedulerNotifier is implemented by the [Scheduler] and consumed by the
// [Manager] to request an early scheduling cycle after a task-state
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

type TaskHeader interface {
	Collection() string
	// VisiblePayload returns the payload fields a caller without tenant read may see.
	VisiblePayload() map[string]any
}

// HeaderDecoder is implemented by the detector whose checks read the header, so the two cannot be registered apart. DecodeHeader must be a pure function of the payload.
// The Manager derives the header when a task copy enters memory (add, restore, receive). Its checks and the reindex readers read the header; drop-vector readers still decode the payload.
type HeaderDecoder interface {
	DecodeHeader(payload []byte) (TaskHeader, error)
	// DecodeCollection reads only the header's collection, for a propose that needs nothing else.
	DecodeCollection(payload []byte) (string, error)
}

// TargetVectorExtractor reads a task payload's (collection, target vectors) binding
// for [Manager.PurgeTasksForCollectionTargetVectors]; ok=false skips the record.
type TargetVectorExtractor func(payload []byte) (collection string, targets []string, ok bool)

// CompletedTaskRetainer is an optional interface a [Provider] implements to
// veto the TTL cleanup of a terminal task whose record is still load-bearing.
// Consulted on the proposal side only (the scheduler's cleanup phase), so it
// need not be deterministic; the FSM's CleanUpTask stays unchanged. Vetoed
// records are re-evaluated every tick — return false once the record stops
// mattering, or it lives forever. namespaceTasks is the provider namespace's
// full task map, so relative judgments (e.g. "is this the newest record of
// its kind") stay local instead of costing a lookup per record per tick.
type CompletedTaskRetainer interface {
	ShouldRetainCompletedTask(task *Task, namespaceTasks map[TaskDescriptor]*Task) bool
}

// TaskCleaner is an interface for issuing a request to clean up a distributed task.
type TaskCleaner interface {
	CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// TaskFinalizer is how the [Scheduler] transitions a task out of
// [TaskStatusSwapping] once local [UnitAwareProvider.OnTaskCompleted] has
// returned. Both methods are idempotent at the FSM layer: only the first
// commit flips the status.
type TaskFinalizer interface {
	// MarkDistributedTaskFinalized transitions SWAPPING → FINISHED after
	// OnTaskCompleted returns nil, so FINISHED means "every post-completion
	// callback committed cluster-wide," not just "every unit terminal." See
	// [TaskStatusSwapping] for the race this fixes.
	MarkDistributedTaskFinalized(ctx context.Context, namespace, taskID string, taskVersion uint64) error

	MarkDistributedTaskFailed(ctx context.Context, namespace, taskID string, taskVersion uint64, errMsg string) error
}

// PostCompletionAckRecorder is the RAFT-apply hook the [Scheduler] uses
// to publish one node's OnGroupCompleted result: a success once its
// callbacks have returned for every local group in a task, a failure as
// soon as one local group fails.
// The scheduler gates [TaskFinalizer.MarkDistributedTaskFinalized] on
// having an ack from every node that has local units in the task, and
// transitions the task to FAILED if any ack reports failure — which
// makes [UnitAwareProvider.OnTaskCompleted] skip the cluster-wide
// schema flip on that path.
//
// The recorded state is stored on the [Task] (see [Task.PostCompletionAcks])
// and survives RAFT snapshot/restore so a node restart during the
// SWAPPING window does not lose the cluster's collected acks.
//
// Crash-safety contract: without this hook, a node whose RunSwapOnShard
// silently failed could still let the cluster-wide schema flip commit,
// leaving that replica serving wrong-tokenization data with no operator
// signal. The recorder closes that gap by gating MarkTaskFinalized on
// every per-node ack landing.
type PostCompletionAckRecorder interface {
	RecordDistributedTaskPostCompletionAck(
		ctx context.Context,
		namespace, taskID string,
		taskVersion uint64,
		groupID, nodeID string,
		success bool,
		errMsg string,
	) error

	RecordDistributedTaskPreparationCompleteAck(
		ctx context.Context,
		namespace, taskID string,
		taskVersion uint64,
		groupID, nodeID string,
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

// ConflictDetector lets the [Manager.AddTask] RAFT-apply path reject a new
// task whose payload conflicts with an already-running task in the same
// namespace.
//
// Motivation: the REST handler holds a per-(collection, property)
// in-memory lock and runs [checkReindexConflict] before submitting,
// which closes the same-node race. But two parallel PUT
// /properties/{prop}/index/{indexType} requests served by *different*
// nodes both pass the per-node lock + check (neither has called
// AddDistributedTask yet at the moment they each query the cluster task
// list) and both submit a
// RAFT task. At that point two reindex migrations race on shared
// on-disk state for the property and one of them ends up FAILED —
// the multi-node face of https://github.com/weaviate/weaviate/issues/10675 (issue tracked as
// "parallel-migration bug #54").
//
// Putting the conflict check inside [Manager.AddTask] under m.mu makes
// it RAFT-deterministic: every node consults the same FSM-stored task
// list at apply time and rejects the duplicate identically, returning
// the conflict error to the originating client.
//
// FSM-determinism contract: CheckConflict MUST be a pure function of
// (newHeader, existingTasks). It must not read mutable process state
// (clocks, network, schema, RNG) — different nodes applying the same
// log entry must reach the same accept/reject decision.
type ConflictDetector interface {
	// CheckConflict is called under [Manager.mu] before a new task is
	// appended to the FSM-stored task list. existingTasks is the full
	// namespace-scoped task list at apply time. Return a non-nil error
	// to reject the new task; the error propagates back to the
	// AddDistributedTask caller.
	CheckConflict(newHeader TaskHeader, existingTasks []*Task) error
	HeaderDecoder
}

// SchemaMutationDetector lets the schema FSM's UpdateProperty, DeleteClass,
// UpdateTenants and DeleteTenants apply paths reject a schema mutation that
// would race with one of a namespace's in-flight tasks.
//
// Without it, a mutation landing mid-migration can let the FSM apply a
// schema flip while the migration's own sub-task is still writing,
// producing a bucket↔schema inversion — same family as the ack-barrier
// failure mode, triggered externally instead of by a crash.
//
// The check runs inside the FSM apply so every node reaches the same
// accept/reject decision from the same applyIndex. Symmetric to
// [ConflictDetector] (protects in-flight tasks from new conflicting tasks);
// this protects them from out-of-band schema mutations during their flight.
//
// CheckPropertyUpdate MUST be a pure function of (className, propertyName,
// existingTasks) — no clocks, network, schema, or RNG — so every node
// applying the same log entry reaches the same decision.
type SchemaMutationDetector interface {
	// Every method's existingTasks is pre-filtered to className: tasks on a known other collection are dropped.
	// indexOff names the index type an update only turns off, and is empty for any other update.
	CheckPropertyUpdate(className, propertyName, indexOff string, existingTasks []*Task) error

	// CheckClassMutation is called under [Manager.mu] from the schema FSM's
	// destructive class-wide apply paths (DeleteClass etc.). Stricter than
	// CheckPropertyUpdate: any in-flight reindex on the class is a conflict,
	// because dropping the class wipes every in-flight migration's working dirs.
	CheckClassMutation(className string, existingTasks []*Task) error

	// Today's reindex tasks are class-scoped (their payload names a
	// collection but not a specific tenant — they apply to whatever
	// shards exist), so the conservative implementation is "block
	// every tenant mutation on a class with any in-flight reindex".
	// A future per-tenant reindex payload could narrow this.
	CheckTenantMutation(className string, tenants []string, existingTasks []*Task) error
}

// CutoverOutcome judges a run's cutover write: did it land, may it still land. Called under [Manager.mu].
type CutoverOutcome interface {
	CutoverLanded(task *Task, readClass func(collection string) *models.Class) bool
	CheckMigrationPropertyUpdate(className, propertyName string, run *Task) error
}

// VectorConfigRemovalGate is an optional interface a SchemaMutationDetector also
// implements to gate removal of a dropped ("none") VectorConfig entry: only the
// completing cleanup task's own in-flight finalize may remove it — a SWAPPING
// task covering the entry whose units plus inherited cleaned-shard set span
// every current shard. A shard in neither has not been stripped, so removing
// the marker would strand its data; and FINISHED records never vouch — they
// outlive finalize by the task TTL, and after a re-create + re-drop of the
// name a stale record would remove the new drop's marker over unstripped
// vectors. Dispatched by type assertion from the SchemaMutationDetector
// registry. Same FSM-determinism contract as [SchemaMutationDetector]: a pure
// function of its arguments.
type VectorConfigRemovalGate interface {
	CheckVectorConfigRemoval(className string, removedVectors, shards []string, existingTasks []*Task) error
}

// VectorConfigRemovalProposeGate is an optional interface a
// [VectorConfigRemovalGate] also implements to refuse a removal on the leader
// before the entry is appended. It may be stricter than the apply-time gate:
// a refusal here reaches no node, so tightening it never makes nodes of
// different versions disagree on a committed entry.
type VectorConfigRemovalProposeGate interface {
	// CheckVectorConfigRemovalAtPropose is called under [Manager.mu] on the
	// leader; non-nil refuses the propose. Arguments as for
	// CheckVectorConfigRemoval, read from the leader's current state.
	CheckVectorConfigRemovalAtPropose(className string, removedVectors, shards []string, existingTasks []*Task) error
}

// RecoveryAwareProvider is an optional interface a provider implements to
// keep the Scheduler's bootstrap pre-mark off its terminal tasks. The
// pre-mark normally suppresses replay of callbacks that already fired
// before a restart; when the provider reports local callback work as not
// durably complete, the scheduler skips it for that task and the next tick
// re-dispatches the task's callbacks.
//
// Re-dispatch recovers nothing: every callback is a no-op at terminal
// status. The one durable effect is a re-issued post-completion ack, once
// per process start — see [RecoveryAwareProvider.LocalCallbacksDone].
type RecoveryAwareProvider interface {
	Provider

	// LocalCallbacksDone returns false when durable local state shows
	// OnGroupCompleted (and any follow-up recovery) did not complete for
	// every unit assigned to localNode. State it cannot read answers false
	// too: not knowing is not the same as being done.
	//
	// All false does today is suppress the bootstrap pre-mark. The terminal
	// task's callbacks are then re-dispatched once on the next tick — each a
	// no-op at terminal status — and one
	// [PostCompletionAckRecorder.RecordDistributedTaskPostCompletionAck] RAFT
	// write is re-issued, once per process start, until the completed-task
	// TTL drops the task from the FSM.
	//
	// Otherwise it returns true, which is weaker than "verified done": a
	// task the provider has nothing to recover for (unparseable, wrong
	// migration kind, no local shards) also returns true, since replaying
	// callbacks for it would achieve nothing.
	//
	// Called from [Scheduler.preMarkTerminalCallbacksLocked] under s.mu, ONCE
	// per terminal task at bootstrap. Implementations should treat this as a
	// cheap on-disk check.
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
//  1. OnGroupCompleted — fires per group as each group's units reach terminal.
//     PREP-only for barrier tasks; PREP+OVERLAY+SWAP inline otherwise.
//  2. OnSwapRequested — OVERLAY+SWAP per decided group this node owes, even after the task ended.
//  3. OnTaskCompleted — fires once on every node after ALL units terminal.
type UnitAwareProvider interface {
	Provider
	// OnGroupCompleted fires when all units in a group reach terminal state.
	// localGroupUnitIDs contains ONLY units on this node; no-op for nodes
	// without local units in the group. Non-nil errors feed
	// RecordPreparationCompleteAck (barrier tasks) or RecordPostCompletionAck.
	OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) error

	OnSwapRequested(task *Task, groupID string, localGroupUnitIDs []string) error

	// OnTaskCompleted is invoked by the [Scheduler] once per task that has
	// reached a terminal status, after every local unit terminated and
	// (for unit-aware providers) every per-node post-completion ack
	// landed. The scheduler MAY re-invoke this method for the same task
	// if a downstream finalize-record write fails — concretely, when
	// [TaskFinalizer.MarkDistributedTaskFinalized] returns an error the
	// rollback path clears the per-task fired-marker so the next tick
	// re-fires OnTaskCompleted before retrying the finalize. Implementations
	// MUST therefore be idempotent against repeat calls with the same
	// (TaskDescriptor, Status): re-running must not double-apply a
	// destructive side effect (a schema flip reverted, a marker emitted
	// twice, etc.). Today's concrete provider (db/reindex_provider.go's
	// OnTaskCompleted → autoCleanupAfterTerminal) already is; new
	// implementations MUST preserve this contract.
	//
	// Return value (weaviate/0-weaviate-issues#297): a non-nil error on the
	// SWAPPING path withholds finalize and retries next tick. Wrap in
	// [ErrTaskCompletionPermanent] for a deterministically-unrecoverable
	// failure to fail the task immediately; a plain error is transient and
	// retried up to a bounded count before failing. Errors on terminal-status
	// invocations are best-effort and must not reopen the task (return nil).
	//
	// A per-property index flag flipped from here must land before the task
	// reaches FINISHED: GET /v1/schema/{class}/indexes reads both from one
	// node's FSM, and a FINISHED task beside an off flag drops that index from
	// the response. The class-wide blockmax flip is the exception, because
	// change-algorithm can defer it past FINISHED. That response never reads
	// it — it resolves models.IndexStatus.Algorithm from the per-property
	// models.Property.SearchableBlockmax stamp, which lands before FINISHED.
	OnTaskCompleted(task *Task) error
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
// Unit values are owned by the [Manager] and mutated under its lock. Callers outside the
// Manager should only access units via cloned [Task] snapshots from ListDistributedTasks.
type Unit struct {
	ID         string     `json:"id"`
	GroupID    string     `json:"groupId,omitempty"`
	Shard      string     `json:"shard,omitempty"`
	NodeID     string     `json:"nodeId"`
	Status     UnitStatus `json:"status"`
	Progress   float32    `json:"progress"`
	Error      string     `json:"error,omitempty"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`
}

// markTerminal stamps FinishedAt and UpdatedAt together with the status, so a
// unit that goes terminal before its first progress report still reports when
// it was last touched. at must come from the RAFT request, never a local
// clock, so every replica's apply produces the same timestamp.
func (u *Unit) markTerminal(status UnitStatus, at time.Time) {
	u.Status = status
	u.FinishedAt = at
	u.UpdatedAt = at
}

// UnitSpec defines a unit with an optional group assignment. Used at task creation
// time when units need group membership (e.g. one group per tenant for MT reindex).
type UnitSpec struct {
	ID      string
	GroupID string
	Shard   string
	Node    string
}

type TaskStatus string

const (
	// TaskStatusStarted means that the task is still running on some of
	// the nodes — at least one unit has not yet reached a terminal status
	// ([UnitStatusCompleted] or [UnitStatusFailed]).
	TaskStatusStarted TaskStatus = "STARTED"

	// TaskStatusPreparing means every unit is terminal on a PREP-barrier task but
	// some swaps are undecided. It becomes SWAPPING once every node's
	// PreparationCompleteAck lands; any failure makes it FAILED. Non-barrier tasks skip it.
	TaskStatusPreparing TaskStatus = "PREPARING"

	// TaskStatusSwapping means every node is firing its per-shard swap; a tenant's
	// replicas can serve different results meanwhile. FINISHED follows once every
	// node's PostCompletionAck lands.
	TaskStatusSwapping TaskStatus = "SWAPPING"

	// TaskStatusFinished means the task succeeded on every node AND every
	// per-node post-completion callback has run. Wait for this — not
	// SWAPPING — when polling for "fully done".
	TaskStatusFinished TaskStatus = "FINISHED"

	// TaskStatusCancelled means that the task was cancelled by user.
	TaskStatusCancelled TaskStatus = "CANCELLED"

	// TaskStatusFailed means that one of the nodes got a non-retryable
	// error and all other nodes terminated the execution.
	TaskStatusFailed TaskStatus = "FAILED"
)

func (t TaskStatus) String() string {
	return string(t)
}

// IsTerminal reports whether this status is a terminal state. A task in
// a terminal state is not transitioning further: it has reached
// [TaskStatusFinished], [TaskStatusFailed], or [TaskStatusCancelled]
// and the scheduler will not invoke any more per-unit or per-task
// callbacks for it via the normal in-flight path. Recovery replays on
// startup can still observe terminal tasks — providers that own
// destructive side-effects (per-shard swaps, file moves) should
// short-circuit on terminal status to avoid running them again.
func (t TaskStatus) IsTerminal() bool {
	switch t {
	case TaskStatusFinished, TaskStatusFailed, TaskStatusCancelled:
		return true
	case TaskStatusStarted, TaskStatusPreparing, TaskStatusSwapping:
		return false
	}
	// A status this build never declared. Fail-closed: treat it as in
	// flight.
	return false
}

// IsActive is true for every non-terminal status — used by conflict
// detection and the schema MutationGuard.
//
// The exact negation of [TaskStatus.IsTerminal], so a status this build
// does not recognize counts as in flight: reading it as done would admit
// a second migration onto a property a newer node is still migrating, and
// let the TTL sweep delete live state. See docs/runtime-reindex.md §4.2.
func (t TaskStatus) IsActive() bool {
	return !t.IsTerminal()
}

// IsCompleted is true once every unit of the task succeeded: SWAPPING
// (completion callbacks in flight) or FINISHED. FAILED/CANCELLED tasks are
// terminal but NOT completed — their units' work cannot be assumed done.
func (t TaskStatus) IsCompleted() bool {
	return t == TaskStatusSwapping || t == TaskStatusFinished
}

// IsRecognized is false for a status this build never declared — what a
// node sees when a newer release introduces one mid rolling-upgrade.
func (t TaskStatus) IsRecognized() bool {
	switch t {
	case TaskStatusStarted, TaskStatusPreparing, TaskStatusSwapping,
		TaskStatusFinished, TaskStatusFailed, TaskStatusCancelled:
		return true
	}
	return false
}

// A literal comparison, not a classification: the FSM apply that reads it
// must reach the same verdict on every binary that replays the entry,
// including one that cannot name the status.
func (t TaskStatus) IsCancellable() bool {
	return t == TaskStatusStarted || t == TaskStatusPreparing
}

// TaskDescriptor is a struct identifying a task execution under a certain task namespace.
type TaskDescriptor struct {
	// ID is the identifier of the task in the namespace.
	ID string `json:"ID"`

	// Version is the version of the task with task ID.
	// It is used to differentiate between multiple runs of the same task.
	Version uint64 `json:"version"`
}

type TaskStatusEntry struct {
	TaskDescriptor
	Status       TaskStatus
	GroupDecided bool
	PrepAckedBy  []string
}

// StatusEntryFor answers for one shard (copying all decided groups would be quadratic in tenants).
func (t *Task) StatusEntryFor(shard string) TaskStatusEntry {
	group := shard
	if t.hasGroup("") {
		group = ""
	}
	var acked []string
	for node := range t.PreparedGroups[group] {
		acked = append(acked, node)
	}
	return TaskStatusEntry{
		TaskDescriptor: t.TaskDescriptor, Status: t.Status, GroupDecided: t.GroupSwapDecided(group),
		PrepAckedBy: acked,
	}
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

	// NeedsPreparationBarrier opts the task into the two-phase RAFT swap barrier:
	// AllUnitsTerminal transitions to PREPARING (not SWAPPING), and the
	// FSM gates PREPARING → SWAPPING on every node's PreparationCompleteAck.
	// Set at AddTask time; immutable thereafter.
	NeedsPreparationBarrier bool `json:"needsPreparationBarrier"`

	// Status is the current status of the task.
	Status TaskStatus `json:"status"`

	// StartedAt is set once by [Manager.AddTask] and is never zero, unlike
	// its pointer-typed API siblings.
	StartedAt time.Time `json:"startedAt"`

	// FinishedAt is the terminal timestamp (zero until then); [Task.markTerminal]
	// is the sole writer, aside from Restore clearing it on non-terminal tasks.
	// Must not be stamped during PREPARING/SWAPPING — those can run minutes on
	// large collections — since cleanup TTL counts from this value.
	FinishedAt time.Time `json:"finishedAt"`

	// Error is an optional field to store the error which moved the task to FAILED status.
	Error string `json:"error,omitempty"`

	// Units tracks per-unit progress. Always non-nil for valid tasks.
	// Whoever replaces it calls [Task.IndexGroups], or group lookups go stale.
	Units map[string]*Unit `json:"units,omitempty"`

	groups map[string][]string

	groupCount int

	// unitCounts lets the per-tick checks answer per node without walking every unit.
	unitCounts map[string]unitCount

	header TaskHeader

	ended            bool
	decidedWithUnits int

	PreparedGroups map[string]map[string]PostCompletionAck `json:"preparedGroups,omitempty"`

	// SwapDecidedGroups outlives FAILED/CANCELLED: a decided group must swap on every replica or they diverge.
	SwapDecidedGroups map[string]time.Time `json:"swapDecidedGroups,omitempty"`

	SwapsOwed map[string][]string `json:"swapsOwed,omitempty"`

	NodePreparationAcks map[string]PostCompletionAck `json:"preparationCompletionAcks,omitempty"`

	// PostCompletionAcks records a non-barrier task's per-node completion acks.
	//
	// Nil for tasks whose provider does not implement [UnitAwareProvider]
	// (no OnGroupCompleted to ack).
	PostCompletionAcks map[string]PostCompletionAck `json:"postCompletionAcks,omitempty"`

	Summary *UnitSummary `json:"summary,omitempty"`
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
	// AckedAt is the acking node's clock at request-build time, not the FSM
	// apply-time clock — required so every replica's apply produces the
	// same value. The spread between the earliest and latest AckedAt shows
	// how long the cluster waited on its slowest node.
	AckedAt time.Time `json:"ackedAt"`
}

// unitCount counts one node's units, and unbound units count under node "".
type unitCount struct {
	total, notTerminal, failed int
}

func (t *Task) IndexGroups() {
	groups := map[string][]string{}
	for id, u := range t.Units {
		groups[u.GroupID] = append(groups[u.GroupID], id)
	}
	t.groups, t.groupCount = groups, len(groups)
	t.countUnits()
	t.countDecided()
}

// adoptGroups shares held's index when it has as many units (units are only removed), never its counts, which follow t's own units.
func (t *Task) adoptGroups(held heldCopy) {
	if !held.sameUnits {
		t.IndexGroups()
		return
	}
	t.groups, t.groupCount = held.groups, held.groupCount
	t.countUnits()
	t.countDecided()
}

func (t *Task) countUnits() {
	t.unitCounts = make(map[string]unitCount, 1)
	for _, u := range t.Units {
		t.countUnit(u, 1)
	}
}

func (t *Task) countUnit(u *Unit, delta int) {
	c := t.unitCounts[u.NodeID]
	c.total += delta
	if u.Status == UnitStatusFailed {
		c.failed += delta
	}
	if u.Status != UnitStatusCompleted && u.Status != UnitStatusFailed {
		c.notTerminal += delta
	}
	if c.total == 0 {
		delete(t.unitCounts, u.NodeID)
		return
	}
	if t.unitCounts == nil {
		t.unitCounts = map[string]unitCount{}
	}
	t.unitCounts[u.NodeID] = c
}

func (t *Task) updateUnit(u *Unit, write func()) {
	t.countUnit(u, -1)
	write()
	t.countUnit(u, 1)
}

func (t *Task) forgetGroups(groups []string) {
	for _, group := range groups {
		_, decided := t.SwapDecidedGroups[group]
		// Compaction, born tenants and takeovers leave switched groups without units.
		delete(t.SwapDecidedGroups, group)
		if !t.hasGroup(group) {
			continue
		}
		if decided {
			t.decidedWithUnits--
		}
		for _, id := range t.groups[group] {
			if u := t.Units[id]; u != nil {
				t.countUnit(u, -1)
			}
			delete(t.Units, id)
		}
		t.groupCount--
		delete(t.SwapsOwed, group)
		delete(t.PreparedGroups, group)
	}
}

func (t *Task) hasGroup(group string) bool {
	if t.Units == nil {
		_, ok := t.groups[group]
		return ok
	}
	return slices.ContainsFunc(t.groups[group], func(id string) bool { return t.Units[id] != nil })
}

func (t *Task) Clone() *Task {
	clone := t.cloneWithoutUnits()
	if t.Units != nil {
		copies := make([]Unit, 0, len(t.Units))
		clone.Units = make(map[string]*Unit, len(t.Units))
		for k, v := range t.Units {
			copies = append(copies, *v)
			clone.Units[k] = &copies[len(copies)-1]
		}
	}
	return clone
}

func (t *Task) cloneWithoutUnits() *Task {
	clone := *t
	clone.Units = nil
	if t.PostCompletionAcks != nil {
		clone.PostCompletionAcks = make(map[string]PostCompletionAck, len(t.PostCompletionAcks))
		for k, v := range t.PostCompletionAcks {
			clone.PostCompletionAcks[k] = v
		}
	}
	clone.SwapDecidedGroups = maps.Clone(t.SwapDecidedGroups)
	// Shallow: paySwap and RecordPreparationCompleteAck replace values, never edit them.
	clone.SwapsOwed = maps.Clone(t.SwapsOwed)
	clone.PreparedGroups = maps.Clone(t.PreparedGroups)
	clone.NodePreparationAcks = maps.Clone(t.NodePreparationAcks)
	clone.unitCounts = maps.Clone(t.unitCounts)
	return &clone
}

func (t *Task) copyChangesFrom(src *Task, change *taskChange) {
	t.Status, t.StartedAt, t.FinishedAt, t.Error = src.Status, src.StartedAt, src.FinishedAt, src.Error
	t.NodePreparationAcks = maps.Clone(src.NodePreparationAcks)
	t.PostCompletionAcks = maps.Clone(src.PostCompletionAcks)
	t.groups, t.groupCount, t.decidedWithUnits = src.groups, src.groupCount, src.decidedWithUnits
	t.unitCounts = maps.Clone(src.unitCounts)
	for id := range change.units {
		u, ok := src.Units[id]
		if !ok {
			delete(t.Units, id)
			continue
		}
		cp := *u
		t.Units[id] = &cp
	}
	for group := range change.groups {
		copyEntry(&t.PreparedGroups, src.PreparedGroups, group)
		copyEntry(&t.SwapDecidedGroups, src.SwapDecidedGroups, group)
		copyEntry(&t.SwapsOwed, src.SwapsOwed, group)
	}
}

func copyEntry[V any](dst *map[string]V, src map[string]V, key string) {
	v, ok := src[key]
	if !ok {
		delete(*dst, key)
		return
	}
	if *dst == nil {
		*dst = map[string]V{}
	}
	(*dst)[key] = v
}

func (t *Task) decideSwap(group string, at time.Time) {
	if t.SwapDecidedGroups == nil {
		t.SwapDecidedGroups = map[string]time.Time{}
	}
	if _, again := t.SwapDecidedGroups[group]; !again && t.hasGroup(group) {
		t.decidedWithUnits++
	}
	t.SwapDecidedGroups[group] = at
	delete(t.PreparedGroups, group)
	nodes := t.groupNodes(group)
	if len(nodes) == 0 || switchedByView(t.header) {
		return
	}
	if t.SwapsOwed == nil {
		t.SwapsOwed = map[string][]string{}
	}
	t.SwapsOwed[group] = nodes
}

func (t *Task) paySwap(group, node string) {
	owed := t.SwapsOwed[group]
	i := slices.Index(owed, node)
	if i < 0 {
		return
	}
	if len(owed) == 1 {
		delete(t.SwapsOwed, group)
		return
	}
	t.SwapsOwed[group] = slices.Delete(slices.Clone(owed), i, i+1)
}

func (t *Task) SwapOwedBy(group, node string) bool {
	return slices.Contains(t.SwapsOwed[group], node)
}

func (t *Task) GroupSwapDecided(groupID string) bool {
	_, ok := t.SwapDecidedGroups[groupID]
	return ok
}

// AllGroupsSwapDecided ignores switched groups without units, such as born tenants.
func (t *Task) AllGroupsSwapDecided() bool {
	return t.decidedWithUnits == t.groupCount
}

func (t *Task) countDecided() {
	t.decidedWithUnits = 0
	for group := range t.SwapDecidedGroups {
		if t.hasGroup(group) {
			t.decidedWithUnits++
		}
	}
}

func (t *Task) appendError(msg string) {
	if t.Error != "" {
		msg = t.Error + "; " + msg
	}
	t.Error = msg
}

// markTerminal stamps status and FinishedAt together so a terminal
// transition never sets one without the other. at must come from the RAFT
// request, never a local clock, so every replica's apply produces the same
// timestamp.
func (t *Task) markTerminal(status TaskStatus, at time.Time) {
	t.Status = status
	t.FinishedAt = at
}

// AllUnitsTerminal returns true if all units are in a terminal state (COMPLETED or FAILED).
func (t *Task) AllUnitsTerminal() bool {
	for _, c := range t.unitCounts {
		if c.notTerminal > 0 {
			return false
		}
	}
	return true
}

// AnyUnitFailed returns true if any unit has FAILED status.
func (t *Task) AnyUnitFailed() bool {
	for _, c := range t.unitCounts {
		if c.failed > 0 {
			return true
		}
	}
	return false
}

func (t *Task) singleGroup(groupID string) bool {
	_, ok := t.groups[groupID]
	return ok && len(t.groups) == 1
}

func (t *Task) UnitShard(unitID string) string {
	if u := t.Units[unitID]; u != nil {
		return u.Shard
	}
	return ""
}

// Header is nil when the payload did not decode or its namespace has no decoder.
func (t *Task) Header() TaskHeader {
	return t.header
}

func (t *Task) collection() string {
	if t.header == nil {
		return ""
	}
	return t.header.Collection()
}

func (t *Task) OnKnownOtherCollection(collection string) bool {
	own := t.collection()
	return own != "" && !strings.EqualFold(own, collection)
}

// Groups returns the distinct GroupIDs across all units (includes "" for ungrouped).
func (t *Task) Groups() []string {
	all := t.groupCount == len(t.groups)
	groups := make([]string, 0, t.groupCount)
	for group := range t.groups {
		if all || t.hasGroup(group) {
			groups = append(groups, group)
		}
	}
	return groups
}

// AllGroupUnitsTerminal returns true if all units in the given group are terminal.
func (t *Task) AllGroupUnitsTerminal(groupID string) bool {
	if t.singleGroup(groupID) {
		return t.AllUnitsTerminal()
	}
	for _, id := range t.groups[groupID] {
		if u := t.Units[id]; u != nil && u.Status != UnitStatusCompleted && u.Status != UnitStatusFailed {
			return false
		}
	}
	return true
}

func (t *Task) groupNodes(groupID string) []string {
	nodes := make([]string, 0, len(t.groups[groupID]))
	for _, id := range t.groups[groupID] {
		if u := t.Units[id]; u != nil && u.NodeID != "" && !slices.Contains(nodes, u.NodeID) {
			nodes = append(nodes, u.NodeID)
		}
	}
	slices.Sort(nodes)
	return nodes
}

func (t *Task) groupSet() map[string]struct{} {
	set := make(map[string]struct{}, t.groupCount)
	for _, group := range t.Groups() {
		set[group] = struct{}{}
	}
	return set
}

func (t *Task) groupHasUnitOn(groupID, nodeID string) bool {
	if t.singleGroup(groupID) {
		return t.unitCounts[nodeID].total > 0
	}
	return slices.ContainsFunc(t.groups[groupID], func(id string) bool { u := t.Units[id]; return u != nil && u.NodeID == nodeID })
}

// LocalGroupUnitIDs returns the IDs of units in the given group assigned to the given node.
func (t *Task) LocalGroupUnitIDs(groupID, nodeID string) []string {
	var ids []string
	for _, id := range t.groups[groupID] {
		if u := t.Units[id]; u != nil && u.NodeID == nodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// NodeHasNonTerminalUnits returns true if the given node has units that are not yet terminal.
// Unassigned units (empty NodeID) are considered as belonging to any node.
func (t *Task) NodeHasNonTerminalUnits(nodeID string) bool {
	return t.unitCounts[nodeID].notTerminal > 0 || t.unitCounts[""].notTerminal > 0
}

// NodesWithLocalUnits returns the set of node IDs that own at least one
// unit assigned to them in this task. Used by the [Scheduler] tick to
// compute the ack-barrier predicate: every such node must record a
// post-completion ack before the task can transition SWAPPING →
// FINISHED. Units with empty NodeID (still PENDING / never claimed) are
// skipped — they cannot have a node-side OnGroupCompleted result yet.
//
// Returned slice is unsorted; callers that need determinism must sort
// it themselves.
func (t *Task) NodesWithLocalUnits() []string {
	nodes := make([]string, 0, len(t.unitCounts))
	for n := range t.unitCounts {
		if n != "" {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// MissingPostCompletionAckNodes returns the node IDs that have local
// units in this task but have NOT yet recorded a post-completion ack.
// The scheduler uses this as the gating predicate for
// [TaskFinalizer.MarkDistributedTaskFinalized] — the SWAPPING →
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

type ListDistributedTasksResponse struct {
	Tasks map[string][]*Task `json:"tasks"`
}
