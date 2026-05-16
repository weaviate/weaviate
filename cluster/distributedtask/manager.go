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
	// the stable FSM state via errors.Is. The legacy phrase
	// "is no longer running" is preserved verbatim inside the human
	// portion so substring-based classifiers on older nodes keep working.
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
		Namespace:      r.Namespace,
		TaskDescriptor: TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:        r.Payload,
		Status:         TaskStatusStarted,
		StartedAt:      time.UnixMilli(r.SubmittedAtUnixMillis),
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
		// existing sentinel; both phrases historically used the
		// "does not exist" substring and were classified as the same
		// permanent state. Keep the original phrasing so legacy
		// substring matching keeps working.
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
			// scheduler for post-completion callbacks (per-node swap
			// via OnGroupCompleted, cluster-wide schema flip via
			// OnTaskCompleted). The scheduler will RAFT
			// [MarkTaskFinalized] once every callback succeeds; that
			// is what flips Status to FINISHED. Until then the task
			// is FINALIZING and callers polling for "fully done" must
			// keep waiting.
			task.Status = TaskStatusFinalizing
		}
		// FinishedAt records when units completed, regardless of which
		// status path we took. For the FINALIZING path this is intentional:
		// completed-task TTL counts from "all units done," not from when
		// the callbacks finished committing. The cleanup predicate in
		// [Scheduler.tick] excludes FINALIZING explicitly so a task whose
		// FinishedAt is already past TTL won't be cleaned mid-finalize.
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

// MarkTaskFinalized transitions a task from FINALIZING to FINISHED. It
// is issued by the scheduler once OnGroupCompleted (per-node swap) and
// OnTaskCompleted (cluster-wide schema flip for semantic migrations)
// have both succeeded — see GH 0-weaviate-issues#212 Issues F+G.
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
	case TaskStatusFinalizing:
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
		// "In flight" = STARTED or FINALIZING: units still running, OR
		// units done but post-completion callbacks not yet committed.
		// Both display ahead of terminal tasks so the freshest
		// user-relevant task surfaces first.
		iStarted := tasks[i].Status == TaskStatusStarted || tasks[i].Status == TaskStatusFinalizing
		jStarted := tasks[j].Status == TaskStatusStarted || tasks[j].Status == TaskStatusFinalizing
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
