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
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

func errTaskNotRunning(namespace, taskID string, version uint64) error {
	// Wrap with the permanent-rejection sentinels so callers can detect
	// the stable FSM state via errors.Is. The wire-level "is no longer
	// running" phrase is preserved verbatim inside the human portion so
	// substring-based classifiers on older nodes (during a rolling
	// upgrade window) keep working.
	return wrapPermanent(ErrTaskNotRunning,
		fmt.Sprintf("task %s/%s/%d is no longer running", namespace, taskID, version))
}

// Manager is responsible for managing distributed tasks across the cluster.
type Manager struct {
	mu    sync.RWMutex
	tasks map[string]map[string]*Task // namespace -> taskID -> Task

	// conflictDetectors is the per-namespace registry consulted by
	// [Manager.AddTask] before appending a new task. nil-safe (and any
	// missing namespace is also nil-safe): no detector → no extra
	// rejection, behavior matches the pre-hook code.
	//
	// Set once at startup via [Manager.SetConflictDetectors]. Reading
	// it during AddTask under m.mu is safe — the setter takes m.mu, so
	// no concurrent read/write race.
	conflictDetectors map[string]ConflictDetector

	// schemaMutationDetectors is the per-namespace registry consulted
	// by the schema FSM's UpdateProperty apply path (see
	// [SchemaMutationDetector] godoc for the motivating bug).
	// nil-safe per the same convention as conflictDetectors.
	//
	// Set once at startup via
	// [Manager.SetSchemaMutationDetectors]. The schema FSM consults
	// these via [Manager.CheckPropertyUpdate]; reading under m.mu is
	// safe — the setter takes m.mu.
	schemaMutationDetectors map[string]SchemaMutationDetector

	completedTaskTTL time.Duration

	clock clockwork.Clock

	// notifier is signalled after every state-changing apply
	// (AddTask, RecordUnitCompletion, UpdateUnitProgress, CancelTask) so
	// the Scheduler runs an immediate scheduling cycle instead of waiting
	// for its next periodic tick. nil-safe so the Manager can be used in
	// tests and during bootstrap before the Scheduler is wired up. The
	// notifier's Wake() must be non-blocking — it is called under the
	// Manager's write lock to ensure every successful apply produces a
	// wake-up that observers cannot miss.
	notifier SchedulerNotifier
}

// SetConflictDetectors installs the per-namespace conflict-detection
// hook called by [Manager.AddTask]. Safe to call once at startup after
// both the Manager and the providers exist (see configure_api.go
// wiring). Subsequent calls overwrite the previous registration.
//
// Pass nil to disable conflict checking (e.g. unit tests that exercise
// AddTask in isolation).
func (m *Manager) SetConflictDetectors(detectors map[string]ConflictDetector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conflictDetectors = detectors
}

// SetSchemaMutationDetectors installs the per-namespace registry
// consulted by [Manager.CheckPropertyUpdate] from the schema FSM's
// UpdateProperty apply path. Safe to call once at startup after both
// the Manager and the providers exist (configure_api.go wiring).
// Subsequent calls overwrite the previous registration.
//
// Pass nil to disable the schema-mutation guard (e.g. unit tests that
// exercise schema applies in isolation).
func (m *Manager) SetSchemaMutationDetectors(detectors map[string]SchemaMutationDetector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemaMutationDetectors = detectors
}

// CheckPropertyUpdate consults every registered
// [SchemaMutationDetector] against the current FSM-stored task list
// and returns the first conflict reported. Called by the schema FSM's
// UpdateProperty apply path BEFORE the merge is applied; returning a
// non-nil error causes the apply to reject with that error.
//
// RAFT-deterministic by construction: under m.mu (write lock to match
// the apply paths that mutate m.tasks), every node sees the same task
// list at the same applyIndex, and each detector is contractually a
// pure function of its arguments. So every node reaches the same
// accept/reject decision.
//
// Returns nil when no detectors are registered or no task in any
// namespace flags the update. Empty fast-path keeps the schema apply
// path free of allocations in the common case.
func (m *Manager) CheckPropertyUpdate(className, propertyName string) error {
	return m.dispatchSchemaMutation(func(d SchemaMutationDetector, existing []*Task) error {
		return d.CheckPropertyUpdate(className, propertyName, existing)
	})
}

// CheckClassMutation consults every registered
// [SchemaMutationDetector] for class-wide destructive mutations
// (e.g. DeleteClass). Stricter than CheckPropertyUpdate — any
// in-flight reindex on the class blocks the mutation.
//
// Same RAFT-determinism contract as CheckPropertyUpdate.
func (m *Manager) CheckClassMutation(className string) error {
	return m.dispatchSchemaMutation(func(d SchemaMutationDetector, existing []*Task) error {
		return d.CheckClassMutation(className, existing)
	})
}

// CheckTenantMutation consults every registered
// [SchemaMutationDetector] for tenant-level mutations that would
// make the named tenants' shards locally unavailable (DeleteTenants,
// UpdateTenants toward OFFLOADED / FROZEN / transitional).
//
// Same RAFT-determinism contract as CheckPropertyUpdate.
func (m *Manager) CheckTenantMutation(className string, tenants []string) error {
	return m.dispatchSchemaMutation(func(d SchemaMutationDetector, existing []*Task) error {
		return d.CheckTenantMutation(className, tenants, existing)
	})
}

// dispatchSchemaMutation is the shared body of CheckPropertyUpdate /
// CheckClassMutation / CheckTenantMutation. Walks every registered
// [SchemaMutationDetector], hands it the namespace-scoped task list,
// returns the first conflict the call closure reports.
func (m *Manager) dispatchSchemaMutation(callDetector func(SchemaMutationDetector, []*Task) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.schemaMutationDetectors) == 0 {
		return nil
	}
	for namespace, detector := range m.schemaMutationDetectors {
		if detector == nil {
			continue
		}
		var existing []*Task
		for _, t := range m.tasks[namespace] {
			existing = append(existing, t)
		}
		if err := callDetector(detector, existing); err != nil {
			return err
		}
	}
	return nil
}

// SetSchedulerNotifier installs the scheduler wake-up notifier. Safe to
// call once at startup after both the Manager and the Scheduler exist
// (see configure_api.go wiring). Subsequent calls overwrite the previous
// notifier.
//
// notifier may be nil to disable reactive firing (e.g. in unit tests
// that exercise the periodic tick path in isolation).
func (m *Manager) SetSchedulerNotifier(notifier SchedulerNotifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifier = notifier
}

// notifySchedulerWithLock signals the installed [SchedulerNotifier].
// Caller must hold m.mu (write lock — every caller of this function is
// a state-changing apply method that already holds the lock). The
// notifier contract requires Wake() to be non-blocking, so this is
// cheap to call from any apply path.
func (m *Manager) notifySchedulerWithLock() {
	if m.notifier == nil {
		return
	}
	m.notifier.Wake()
}

type ManagerParameters struct {
	Clock clockwork.Clock

	CompletedTaskTTL time.Duration
}

func NewManager(params ManagerParameters) *Manager {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}

	return &Manager{
		tasks: make(map[string]map[string]*Task),

		completedTaskTTL: params.CompletedTaskTTL,

		clock: params.Clock,
	}
}

// AddTask registers a new distributed task from a Raft apply. The seqNum becomes the task's
// Version, used to distinguish re-runs of the same task ID. Returns an error if a task with
// the same namespace/ID is already running, or if no units are provided.
func (m *Manager) AddTask(c *api.ApplyRequest, seqNum uint64) error {
	var r api.AddDistributedTaskRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal add task request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.findTaskWithLock(r.Namespace, r.Id)
	if task != nil {
		if task.Status == TaskStatusStarted {
			return fmt.Errorf("task %s/%s is already running with version %d", r.Namespace, r.Id, task.Version)
		}

		if seqNum <= task.Version {
			return fmt.Errorf("task %s/%s is already finished with version %d", r.Namespace, r.Id, task.Version)
		}
	}

	// Cluster-wide conflict check: if a provider registered a
	// [ConflictDetector] for this namespace, give it the chance to
	// reject the new task based on the FSM-stored task list. This
	// closes the multi-node parallel-submit race the REST handler's
	// per-node submit lock cannot cover (#10675 family,
	// parallel-migration bug). The detector must be a pure function
	// of (newPayload, existingTasks) — see the ConflictDetector
	// godoc on the FSM-determinism contract.
	if cd, ok := m.conflictDetectors[r.Namespace]; ok && cd != nil {
		var existing []*Task
		for _, t := range m.tasks[r.Namespace] {
			existing = append(existing, t)
		}
		if err := cd.CheckConflict(r.Payload, existing); err != nil {
			return fmt.Errorf("task %s/%s conflicts with existing task: %w", r.Namespace, r.Id, err)
		}
	}

	newTask := &Task{
		Namespace:        r.Namespace,
		TaskDescriptor:   TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:          r.Payload,
		NeedsPrepBarrier: r.NeedsPrepBarrier,
		Status:           TaskStatusStarted,
		StartedAt:        time.UnixMilli(r.SubmittedAtUnixMillis),
	}

	if len(r.UnitSpecs) > 0 {
		newTask.Units = make(map[string]*Unit, len(r.UnitSpecs))
		for _, spec := range r.UnitSpecs {
			newTask.Units[spec.Id] = &Unit{
				ID:      spec.Id,
				GroupID: spec.GroupId,
				Status:  UnitStatusPending,
			}
		}
	} else if len(r.UnitIds) > 0 {
		newTask.Units = make(map[string]*Unit, len(r.UnitIds))
		for _, id := range r.UnitIds {
			newTask.Units[id] = &Unit{
				ID:     id,
				Status: UnitStatusPending,
			}
		}
	} else {
		return fmt.Errorf("task %s/%s must have at least one unit", r.Namespace, r.Id)
	}

	m.setTaskWithLock(newTask)
	m.notifySchedulerWithLock()

	return nil
}

// findStartedUnitWithLock validates that the task exists, is running, has units, the unit
// exists, and is owned by (or unassigned to) the given node. Returns the task and unit on success.
func (m *Manager) findStartedUnitWithLock(namespace, taskID string, version uint64, unitID, nodeID string) (*Task, *Unit, error) {
	task, err := m.findVersionedTaskWithLock(namespace, taskID, version)
	if err != nil {
		return nil, nil, err
	}

	if task.Status != TaskStatusStarted {
		return nil, nil, errTaskNotRunning(namespace, taskID, task.Version)
	}

	u, ok := task.Units[unitID]
	if !ok {
		// "unit ... does not exist" → ErrTaskDoesNotExist is the closest
		// existing sentinel; both phrases use the "does not exist"
		// substring and are classified as the same permanent state.
		// Keep the wire-level phrasing so substring matching on older
		// peers (during a rolling upgrade window) keeps working.
		return nil, nil, wrapPermanent(ErrTaskDoesNotExist,
			fmt.Sprintf("unit %s does not exist in task %s/%s/%d", unitID, namespace, taskID, task.Version))
	}

	if u.NodeID != "" && u.NodeID != nodeID {
		return nil, nil, wrapPermanent(ErrUnitWrongNode,
			fmt.Sprintf("unit %s in task %s/%s/%d belongs to node %s, not %s",
				unitID, namespace, taskID, task.Version, u.NodeID, nodeID))
	}

	return task, u, nil
}

// RecordUnitCompletion handles both success and failure (distinguished by a non-empty error
// field in the request). On failure, the task transitions to FAILED immediately — remaining
// in-flight units are NOT waited for, and their subsequent completion reports will be
// rejected with "task is no longer running". This fail-fast behavior is intentional: it avoids
// wasting cluster resources on a task that is already doomed.
func (m *Manager) RecordUnitCompletion(c *api.ApplyRequest) error {
	var r api.RecordDistributedTaskUnitCompletionRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record unit completion request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, u, err := m.findStartedUnitWithLock(r.Namespace, r.Id, r.Version, r.UnitId, r.NodeId)
	if err != nil {
		return err
	}

	if u.Status == UnitStatusCompleted || u.Status == UnitStatusFailed {
		return wrapPermanent(ErrUnitAlreadyTerminal,
			fmt.Sprintf("unit %s in task %s/%s/%d is already terminal", r.UnitId, r.Namespace, r.Id, task.Version))
	}

	finishedAt := time.UnixMilli(r.FinishedAtUnixMillis)

	if r.Error != "" {
		u.Status = UnitStatusFailed
		u.Error = r.Error
		u.FinishedAt = finishedAt
		task.Status = TaskStatusFailed
		task.Error = fmt.Sprintf("unit %s failed: %s", r.UnitId, r.Error)
		task.FinishedAt = finishedAt
		m.notifySchedulerWithLock()
		return nil
	}

	u.Status = UnitStatusCompleted
	u.Progress = 1.0
	u.FinishedAt = finishedAt

	if task.AllUnitsTerminal() {
		if task.AnyUnitFailed() {
			// Failures are terminal immediately — there are no
			// post-task callbacks to wait for on the failure path
			// (the schema flip is intentionally skipped when any
			// unit failed; see OnTaskCompleted's early-return for
			// FAILED tasks).
			task.Status = TaskStatusFailed
		} else {
			// All units COMPLETED with no failures — hand off to the
			// scheduler for post-completion callbacks.
			//
			// Routing decision:
			//   - NeedsPrepBarrier=true (semantic migrations) → PREPARING.
			//     Each node's scheduler fires its PREP body and emits a
			//     RecordPrepCompleteAck. The FSM (in RecordPrepCompleteAck)
			//     atomically transitions PREPARING → SWAPPING only when
			//     every expected ack lands with Success=true.
			//   - NeedsPrepBarrier=false (format-only / debug / etc.) →
			//     SWAPPING directly, preserving the pre-barrier behavior.
			//     The scheduler fires the per-node swap immediately and
			//     emits a RecordPostCompletionAck; the FSM gates the
			//     SWAPPING → FINISHED transition (via MarkTaskFinalized)
			//     on those acks. Same wire shape as today.
			if task.NeedsPrepBarrier {
				task.Status = TaskStatusPreparing
			} else {
				task.Status = TaskStatusSwapping
			}
		}
		// FinishedAt records when units completed, regardless of which
		// status path we took. For the SWAPPING path this is intentional:
		// completed-task TTL counts from "all units done," not from when
		// the callbacks finished committing. The cleanup predicate in
		// [Scheduler.tick] excludes SWAPPING explicitly so a task whose
		// FinishedAt is already past TTL won't be cleaned mid-swap.
		task.FinishedAt = finishedAt
	}

	// Notify on every unit completion — even when not the last one — so
	// the Scheduler can react to per-group barriers opening as soon as
	// the cluster-wide AllGroupUnitsTerminal predicate becomes true. The
	// Scheduler decides which callbacks to actually fire; here we just
	// ensure it gets a chance to look.
	m.notifySchedulerWithLock()
	return nil
}

// RecordPostCompletionAck records one node's OnGroupCompleted result on
// the task. The [Scheduler] tick on each node fires this command after
// its local OnGroupCompleted has returned for every local group, so the
// cluster has durable evidence of which nodes' post-completion work
// succeeded before the task is allowed to transition SWAPPING →
// FINISHED — the load-bearing invariant that prevents a per-node swap
// failure from leaving the cluster-wide schema flipped past a node
// whose local bucket pointer never moved.
//
// FSM transitions on apply:
//
//   - Ack arrives for an idempotently-already-acked (task, node): no-op,
//     the first ack wins.
//   - Ack arrives for a task no longer in a state that can use it
//     (FAILED / FINISHED / CANCELLED): no-op. A late-arriving ack from
//     a slow follower after the leader has already failed the task is
//     expected and not an error.
//   - Ack with Success==false arrives while the task is SWAPPING:
//     records the ack AND transitions the task to FAILED. The cluster-
//     wide schema flip (in [UnitAwareProvider.OnTaskCompleted]) is
//     skipped on FAILED, so a node whose RunSwapOnShard silently failed
//     cannot let the schema commit while one replica is broken.
//   - Ack with Success==true arrives while the task is STARTED or
//     SWAPPING: records the ack and leaves the status alone (the
//     scheduler issues MarkTaskFinalized once every expected ack has
//     landed).
//
// Idempotent: every node's scheduler may re-fire this on tick / wake
// retries until the apply commits. The first ack per (task, node) sticks;
// later acks for the same node are silently discarded.
func (m *Manager) RecordPostCompletionAck(c *api.ApplyRequest) error {
	var r api.RecordDistributedTaskPostCompletionAckRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record post-completion ack request: %w", err)
	}
	if r.NodeId == "" {
		return fmt.Errorf("post-completion ack for task %s/%s missing node_id", r.Namespace, r.Id)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	// Once the task has moved past the post-completion barrier (FAILED /
	// FINISHED / CANCELLED), drop the ack: the cluster has already
	// decided the task's outcome. This MUST be silent (no error) — every
	// node's scheduler may emit this command in parallel and the
	// stragglers must not produce noisy apply failures.
	switch task.Status {
	case TaskStatusFailed, TaskStatusFinished, TaskStatusCancelled:
		return nil
	case TaskStatusStarted, TaskStatusSwapping:
		// Normal paths.
	default:
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot record post-completion ack from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	if task.PostCompletionAcks == nil {
		task.PostCompletionAcks = map[string]PostCompletionAck{}
	}
	if _, present := task.PostCompletionAcks[r.NodeId]; present {
		// Idempotent: the first ack per (task, node) wins. A retry from
		// the scheduler (because the apply RPC errored on the wire) lands
		// here once the original commit propagated.
		return nil
	}

	task.PostCompletionAcks[r.NodeId] = PostCompletionAck{
		Success: r.Success,
		Error:   r.Error,
		AckedAt: time.UnixMilli(r.AckedAtUnixMillis),
	}

	// Any failure → the task fails immediately. Subsequent acks for the
	// same task are still recorded (forensic value), but the status is
	// locked to FAILED until cleanup.
	if !r.Success && task.Status == TaskStatusSwapping {
		task.Status = TaskStatusFailed
		// Preserve the first per-unit error message if one was already
		// set (defensive — RecordUnitCompletion would already have moved
		// the task to FAILED in that case). Append the post-completion
		// failure for forensic clarity.
		ackErr := fmt.Sprintf("post-completion swap failed on node %s: %s", r.NodeId, r.Error)
		if task.Error != "" {
			task.Error = task.Error + "; " + ackErr
		} else {
			task.Error = ackErr
		}
		// FinishedAt was set when AllUnitsTerminal landed; keep that —
		// the user-meaningful "when did the work end" timestamp is when
		// the units finished, not when we noticed the swap failure.
	}

	m.notifySchedulerWithLock()
	return nil
}

// RecordPrepCompleteAck records one node's PREP-phase result on the
// task. The [Scheduler] tick on each node fires this command after its
// local OnGroupCompleted's PREP body has returned for every local
// group, so the cluster has durable evidence of which nodes' prep work
// succeeded before the cluster-wide PREPARING → SWAPPING transition is
// committed — the load-bearing barrier invariant.
//
// FSM transitions on apply:
//
//   - Ack arrives for an idempotently-already-acked (task, node): no-op,
//     the first ack wins.
//   - Ack arrives for a task no longer in a state that can use it
//     (FAILED / FINISHED / CANCELLED, or SWAPPING/FINISHED after the
//     barrier has already lifted): no-op. Late-arriving acks from slow
//     followers must not produce noisy apply failures.
//   - Ack with Success==false arrives while the task is PREPARING:
//     records the ack AND transitions the task to FAILED. No node
//     proceeds to the atomic swap (the barrier holds — this is the
//     primary safety property of the PREP barrier).
//   - Ack with Success==true arrives while the task is STARTED or
//     PREPARING: records the ack. If every expected node (i.e. every
//     node that owns at least one local unit on this task) has now
//     ack'd with Success=true, transitions the task PREPARING →
//     SWAPPING. The scheduler tick on each node observes SWAPPING and
//     fires the per-node atomic swap (OnSwapRequested).
//
// Idempotent: every node's scheduler may re-fire this on tick / wake
// retries until the apply commits. The first ack per (task, node)
// sticks; later acks for the same node are silently discarded.
//
// FSM-determinism: the PREPARING → SWAPPING transition is computed
// purely from the task's Units → NodeID map (which is RAFT-replicated
// and identical on every node) plus the PrepCompletionAcks state — so
// every node's Manager arrives at the transition on the same apply.
func (m *Manager) RecordPrepCompleteAck(c *api.ApplyRequest) error {
	var r api.RecordDistributedTaskPrepCompleteAckRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record prep-complete ack request: %w", err)
	}
	if r.NodeId == "" {
		return fmt.Errorf("prep-complete ack for task %s/%s missing node_id", r.Namespace, r.Id)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	// Once the task has moved past the PREP barrier (SWAPPING / FINISHED
	// / FAILED / CANCELLED), drop the ack: the cluster has already
	// decided the barrier outcome. SWAPPING is reachable iff the
	// barrier lifted successfully, so a late PrepAck against SWAPPING
	// is a slow-follower retry that the FSM has nothing to do with.
	switch task.Status {
	case TaskStatusFailed, TaskStatusFinished, TaskStatusCancelled, TaskStatusSwapping:
		return nil
	case TaskStatusStarted, TaskStatusPreparing:
		// Normal paths. (STARTED accepted defensively: an early ack
		// race between AllUnitsTerminal's STARTED → PREPARING
		// transition and the scheduler's PrepAck emission should not
		// cause a noisy apply failure.)
	default:
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot record prep-complete ack from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	if task.PrepCompletionAcks == nil {
		task.PrepCompletionAcks = map[string]PostCompletionAck{}
	}
	if _, present := task.PrepCompletionAcks[r.NodeId]; present {
		// Idempotent: the first ack per (task, node) wins.
		return nil
	}

	task.PrepCompletionAcks[r.NodeId] = PostCompletionAck{
		Success: r.Success,
		Error:   r.Error,
		AckedAt: time.UnixMilli(r.AckedAtUnixMillis),
	}

	// Failure path: the task fails immediately. No node proceeds to the
	// atomic swap.
	if !r.Success && task.Status == TaskStatusPreparing {
		task.Status = TaskStatusFailed
		ackErr := fmt.Sprintf("prep failed on node %s: %s", r.NodeId, r.Error)
		if task.Error != "" {
			task.Error = task.Error + "; " + ackErr
		} else {
			task.Error = ackErr
		}
		// FinishedAt was set when AllUnitsTerminal landed; keep it.
		m.notifySchedulerWithLock()
		return nil
	}

	// Success path: if every expected ack has landed successfully,
	// transition PREPARING → SWAPPING. This is the moment the barrier
	// lifts cluster-wide.
	if r.Success && task.Status == TaskStatusPreparing && allExpectedPrepAcksLanded(task) {
		task.Status = TaskStatusSwapping
	}

	m.notifySchedulerWithLock()
	return nil
}

// allExpectedPrepAcksLanded returns true iff every node owning at
// least one local unit on the task has recorded a PrepCompletionAck
// with Success=true. Called under [Manager.mu]; pure transform of
// task state.
//
// Note: a unit without a NodeID (PENDING + unclaimed) is NOT expected
// to produce a PrepAck (no node is running it). PrepCompleteAck only
// fires after the PREP work runs, which only happens on a node that
// claimed the unit via the per-unit progress flow. By the time the
// task transitions to PREPARING, every unit must be terminal (which
// implies NodeID is set on every unit), so the expected-node set is
// fully determined from task.Units.
func allExpectedPrepAcksLanded(task *Task) bool {
	expected := map[string]struct{}{}
	for _, u := range task.Units {
		if u.NodeID != "" {
			expected[u.NodeID] = struct{}{}
		}
	}
	for node := range expected {
		ack, ok := task.PrepCompletionAcks[node]
		if !ok || !ack.Success {
			return false
		}
	}
	return true
}

// MarkTaskFinalized transitions a task from SWAPPING to FINISHED. It
// is issued by the scheduler once OnGroupCompleted (per-node swap) and
// OnTaskCompleted (cluster-wide schema flip for semantic migrations)
// have both succeeded.
//
// Idempotent at the FSM layer: every node's scheduler fires this command
// after its local callbacks succeed. The first commit flips the status;
// subsequent commits hit the "already FINISHED" short-circuit and return
// without error.
func (m *Manager) MarkTaskFinalized(c *api.ApplyRequest) error {
	var r api.MarkTaskFinalizedRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal mark task finalized request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	switch task.Status {
	case TaskStatusFinished:
		// Idempotent: another node's MarkTaskFinalized already
		// committed. Nothing more to do.
		return nil
	case TaskStatusSwapping:
		// Normal transition.
	default:
		// FAILED / CANCELLED / STARTED — refusing here protects against
		// a stale RAFT command arriving after a cancel/fail moved the
		// task to a terminal state we shouldn't overwrite.
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot be finalized from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	// FinishedAt is intentionally NOT overwritten here. It was already set
	// in [Manager.RecordUnitCompletion] when all units reached terminal
	// state — that is the user-meaningful "when did the work finish"
	// timestamp, and the completed-task TTL counts from there. The
	// FinalizedAtUnixMillis on the request is left in place for forensic
	// purposes (visible in RAFT logs) but not stored on the Task.
	task.Status = TaskStatusFinished
	m.notifySchedulerWithLock()
	return nil
}

// UpdateUnitProgress also handles initial node assignment: the first progress update for an
// unassigned unit sets its NodeID, claiming it for that node. After assignment, updates from
// other nodes are rejected. Progress updates to terminal units are silently ignored (no error)
// because in-flight Raft commands may arrive after a unit has already completed.
func (m *Manager) UpdateUnitProgress(c *api.ApplyRequest) error {
	var r api.UpdateDistributedTaskUnitProgressRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal update unit progress request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, u, err := m.findStartedUnitWithLock(r.Namespace, r.Id, r.Version, r.UnitId, r.NodeId)
	if err != nil {
		return err
	}

	if u.Status == UnitStatusCompleted || u.Status == UnitStatusFailed {
		return nil // silently ignore progress updates for terminal units
	}

	if r.Progress < 0 || r.Progress > 1 {
		return fmt.Errorf("progress for unit %s in task %s/%s/%d must be between 0.0 and 1.0, got %v",
			r.UnitId, r.Namespace, r.Id, r.Version, r.Progress)
	}

	u.NodeID = r.NodeId
	u.Progress = r.Progress
	u.UpdatedAt = time.UnixMilli(r.UpdatedAtUnixMillis)

	wasPending := u.Status == UnitStatusPending
	if wasPending {
		u.Status = UnitStatusInProgress
	}

	// Wake the scheduler on first-progress (Pending → InProgress) so it
	// can launch a freshly-claimed task without waiting for the next
	// tick. Subsequent progress updates inside a unit do not change the
	// Scheduler's view (the per-unit progress is consumed by REST
	// /v1/tasks pollers, not by the scheduler loop), so skip the wake-up
	// to avoid swamping the channel with no-op signals.
	if wasPending {
		m.notifySchedulerWithLock()
	}
	return nil
}

// CancelTask transitions a running task to CANCELLED. In-flight units are not waited
// for — the [Scheduler] will terminate their local handles on the next tick.
func (m *Manager) CancelTask(a *api.ApplyRequest) error {
	var r api.CancelDistributedTaskRequest
	if err := json.Unmarshal(a.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal cancel task request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return errTaskNotRunning(r.Namespace, r.Id, task.Version)
	}

	task.Status = TaskStatusCancelled
	task.FinishedAt = time.UnixMilli(r.CancelledAtUnixMillis)
	m.notifySchedulerWithLock()
	return nil
}

// CleanUpTask removes a terminal task from the Manager's state. It refuses to clean up tasks
// that are still running or whose completedTaskTTL has not yet elapsed, preventing premature
// removal of status information that other nodes may still need to observe.
func (m *Manager) CleanUpTask(a *api.ApplyRequest) error {
	var r api.CleanUpDistributedTaskRequest
	if err := json.Unmarshal(a.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal clean up task request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status == TaskStatusStarted {
		return fmt.Errorf("task %s/%s/%d is still running", r.Namespace, r.Id, task.Version)
	}

	if m.clock.Since(task.FinishedAt) <= m.completedTaskTTL {
		return fmt.Errorf("task %s/%s/%d is too fresh to clean up", r.Namespace, r.Id, task.Version)
	}

	delete(m.tasks[task.Namespace], task.ID)
	return nil
}

// ListDistributedTasks returns a snapshot of all tasks grouped by namespace. Each [Task] is
// cloned, so callers may read the returned values without holding the Manager's lock.
//
// Tasks within each namespace are sorted deterministically so adjacent
// polls return the same slice order regardless of Go's randomized map
// iteration. Sort key:
//
//  1. STARTED tasks first (the currently-running work matters most).
//  2. Within priority, by activity-time DESC (newest first). Activity-time
//     is FinishedAt for terminal tasks, StartedAt otherwise.
//  3. Tiebreak by ID ASC for full stability.
func (m *Manager) ListDistributedTasks(_ context.Context) (map[string][]*Task, error) {
	// Read-only: holding RLock lets concurrent /indexes polls proceed
	// without serialising against each other (they still wait on any
	// in-flight RAFT-apply mutator).
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]*Task, len(m.tasks))
	for namespace, tasks := range m.tasks {
		if len(tasks) == 0 {
			continue
		}

		result[namespace] = make([]*Task, 0, len(tasks))
		for _, task := range tasks {
			result[namespace] = append(result[namespace], task.Clone())
		}
		sortTasksForDisplay(result[namespace])
	}
	return result, nil
}

// sortTasksForDisplay sorts tasks in place so the slice is identical on
// every call given the same input set. See [Manager.ListDistributedTasks]
// for the sort-key rationale. SliceStable is intentional: equal-priority
// equal-time equal-ID inputs are byte-identical to clone anyway, but
// SliceStable documents the intent.
func sortTasksForDisplay(tasks []*Task) {
	sort.SliceStable(tasks, func(i, j int) bool {
		// "In flight" = STARTED, PREPARING, or SWAPPING (via
		// [TaskStatus.IsActive]): units still running, OR units done
		// but per-node PREP / cluster-wide barrier / per-node SWAP /
		// schema flip not yet committed. All display ahead of terminal
		// tasks so the freshest user-relevant task surfaces first.
		iStarted := tasks[i].Status.IsActive()
		jStarted := tasks[j].Status.IsActive()
		if iStarted != jStarted {
			return iStarted
		}

		iWhen := tasks[i].FinishedAt
		if iWhen.IsZero() {
			iWhen = tasks[i].StartedAt
		}
		jWhen := tasks[j].FinishedAt
		if jWhen.IsZero() {
			jWhen = tasks[j].StartedAt
		}
		if !iWhen.Equal(jWhen) {
			return iWhen.After(jWhen)
		}

		return tasks[i].ID < tasks[j].ID
	})
}

func (m *Manager) ListDistributedTasksPayload(ctx context.Context) ([]byte, error) {
	tasks, err := m.ListDistributedTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("list distributed tasks: %w", err)
	}

	return json.Marshal(&ListDistributedTasksResponse{
		Tasks: tasks,
	})
}

func (m *Manager) findVersionedTaskWithLock(namespace, taskID string, taskVersion uint64) (*Task, error) {
	task := m.findTaskWithLock(namespace, taskID)
	if task == nil || task.Version != taskVersion {
		return nil, wrapPermanent(ErrTaskDoesNotExist,
			fmt.Sprintf("task %s/%s/%d does not exist", namespace, taskID, taskVersion))
	}

	return task, nil
}

func (m *Manager) findTaskWithLock(namespace, taskID string) *Task {
	tasksNamespace, ok := m.tasks[namespace]
	if !ok {
		return nil
	}

	task, ok := tasksNamespace[taskID]
	if !ok {
		return nil
	}

	return task
}

func (m *Manager) setTaskWithLock(task *Task) {
	if _, ok := m.tasks[task.Namespace]; !ok {
		m.tasks[task.Namespace] = make(map[string]*Task)
	}

	m.tasks[task.Namespace][task.ID] = task
}

type snapshot struct {
	Tasks map[string][]*Task `json:"tasks,omitempty"`
}

// Snapshot serialises the full task state to JSON for Raft snapshotting. The inverse
// operation is [Manager.Restore].
func (m *Manager) Snapshot() ([]byte, error) {
	tasks, err := m.ListDistributedTasks(context.Background())
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	bytes, err := json.Marshal(&snapshot{
		Tasks: tasks,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return bytes, nil
}

// Restore replaces the Manager's in-memory state from a Raft snapshot produced by
// [Manager.Snapshot]. It is called during Raft leader election or when a follower installs
// a snapshot from the leader.
func (m *Manager) Restore(bytes []byte) error {
	var s snapshot
	if err := json.Unmarshal(bytes, &s); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for namespace, tasks := range s.Tasks {
		for _, task := range tasks {
			if _, ok := m.tasks[namespace]; !ok {
				m.tasks[namespace] = make(map[string]*Task)
			}

			m.tasks[namespace][task.ID] = task
		}
	}

	return nil
}
