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
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
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

	// Per-namespace payload→collection extractors. Absent ⇒ namespace is
	// not collection-scoped and survives DeleteTasksForCollection.
	collectionExtractors map[string]CollectionExtractor

	// Per-namespace terminal-apply observers; see [TerminalObserver].
	terminalObservers map[string]TerminalObserver
	// terminalPending holds live terminal endings that applied before the
	// namespace registered its observer, since registration happens well after
	// the store starts applying. Bounded by [terminalPendingPerNamespace];
	// guarded by mu.
	terminalPending map[string][]*Task
	// terminalDispatch carries terminal tasks to the single drainer goroutine
	// that calls the observers; see [Manager.dispatchTerminalWithLock].
	terminalDispatch chan *Task
	// terminalDispatchDone is closed by [Manager.Close] to stop the drainer.
	terminalDispatchDone chan struct{}
	// terminalOverflowInFlight counts overflow goroutines spawned when the
	// queue is full; bounded by [terminalDispatchOverflowLimit]. Atomic: read
	// and incremented under mu, decremented from the goroutine itself.
	terminalOverflowInFlight atomic.Int64
	// terminalDrainerRunning keeps the drainer to exactly one goroutine across
	// repeated registrations. Guarded by mu.
	terminalDrainerRunning bool
	// terminalDispatchClosed makes Close idempotent. Guarded by mu.
	terminalDispatchClosed bool

	completedTaskTTL time.Duration

	clock clockwork.Clock

	logger logrus.FieldLogger

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
	// Sorted, because this runs on the RAFT apply path and returns the FIRST
	// error. Ranging the detector map directly would let two nodes applying
	// the same log entry name conflicts from different namespaces.
	namespaces := make([]string, 0, len(m.schemaMutationDetectors))
	for namespace := range m.schemaMutationDetectors {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)

	for _, namespace := range namespaces {
		detector := m.schemaMutationDetectors[namespace]
		if detector == nil {
			continue
		}
		if err := callDetector(detector, m.sortedTasksWithLock(namespace)); err != nil {
			return err
		}
	}
	return nil
}

// sortedTasksWithLock returns the namespace's tasks ordered by task ID.
// Caller must hold m.mu.
//
// Map order is nondeterministic; accept/reject is unaffected, but WHICH
// conflicting task gets named in the refusal is not — sorting keeps that
// message stable across nodes/retries and in sync with the REST pre-check.
func (m *Manager) sortedTasksWithLock(namespace string) []*Task {
	tasks := make([]*Task, 0, len(m.tasks[namespace]))
	for _, t := range m.tasks[namespace] {
		tasks = append(tasks, t)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks
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

	Logger logrus.FieldLogger
}

func NewManager(params ManagerParameters) *Manager {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}
	if params.Logger == nil {
		// Only tests leave this nil; production always passes a logger.
		discarding := logrus.New()
		discarding.Out = io.Discard
		params.Logger = discarding
	}

	return &Manager{
		tasks:                make(map[string]map[string]*Task),
		collectionExtractors: make(map[string]CollectionExtractor),
		terminalObservers:    make(map[string]TerminalObserver),
		terminalPending:      make(map[string][]*Task),
		terminalDispatch:     make(chan *Task, terminalDispatchQueueDepth),
		terminalDispatchDone: make(chan struct{}),

		completedTaskTTL: params.CompletedTaskTTL,

		clock:  params.Clock,
		logger: params.Logger,
	}
}

// RegisterCollectionExtractor opts a task namespace into DeleteTasksForCollection's
// cascade. Extractor runs under the Manager lock — must not block or recurse. Last
// write wins per namespace; nil / empty arguments are silently dropped.
func (m *Manager) RegisterCollectionExtractor(namespace string, extractor CollectionExtractor) {
	if namespace == "" || extractor == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collectionExtractors[namespace] = extractor
}

// RegisterTerminalObserver installs the namespace's [TerminalObserver], which
// fires on CANCELLED and on FAILED. Last write wins; nil/empty arguments are
// dropped. Also starts the drainer, so registering is what opens the queue.
//
// Endings that applied while the namespace had no observer are delivered here,
// oldest first, so the startup window between the store accepting applies and
// the observer being wired does not swallow them.
//
// Registrations after Close are dropped: dispatch is already shut, so a
// drainer started now would only exit immediately.
func (m *Manager) RegisterTerminalObserver(namespace string, observer TerminalObserver) {
	if namespace == "" || observer == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminalDispatchClosed {
		return
	}
	m.terminalObservers[namespace] = observer
	m.startTerminalDrainerWithLock()

	pending := m.terminalPending[namespace]
	delete(m.terminalPending, namespace)
	for _, task := range pending {
		m.enqueueTerminalWithLock(task)
	}
}

// Close stops the terminal-observer drainer. Idempotent; queued events are
// dropped since shutdown means there's no one left to read them. Does not
// wait for an observer call already in flight — the drainer looks its
// observer up under the same lock Close holds, so joining would deadlock.
// Observers must tolerate being called for a moment after Close returns.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminalDispatchClosed {
		return
	}
	m.terminalDispatchClosed = true
	clear(m.terminalPending)
	close(m.terminalDispatchDone)
}

// terminalDispatchQueueDepth is how deep the drainer's queue can get before
// the apply path falls back to a fan-out goroutine per event. Task endings
// are rare, so a queue this deep only fills if an observer has wedged.
const terminalDispatchQueueDepth = 256

// terminalPendingPerNamespace bounds the endings held for a namespace that has
// not registered its observer yet. Deep enough to cover the startup window
// between the store applying and the wiring completing, shallow enough that a
// namespace which never registers cannot retain tasks indefinitely.
const terminalPendingPerNamespace = 64

// terminalDispatchOverflowLimit bounds fan-out goroutines spawned once the
// queue is full. A wedged observer would otherwise let these pile up forever.
const terminalDispatchOverflowLimit = 32

// startTerminalDrainerWithLock brings up the single goroutine that runs terminal
// observers. Caller holds m.mu.
func (m *Manager) startTerminalDrainerWithLock() {
	if m.terminalDrainerRunning {
		return
	}
	m.terminalDrainerRunning = true

	queue, done := m.terminalDispatch, m.terminalDispatchDone
	enterrors.GoWrapper(func() {
		for {
			select {
			case <-done:
				return
			case task := <-queue:
				// select picks randomly among ready cases, so re-check done to
				// avoid delivering to a torn-down observer after Close.
				select {
				case <-done:
					return
				default:
				}
				m.runTerminalObserverSafely(task)
			}
		}
	}, m.logger)
}

// runTerminalObserverSafely keeps one namespace's panicking observer from
// killing the drainer for all of them: GoWrapper's recover sits outside the
// loop, so an unrecovered panic ends the drainer for good with no restart.
func (m *Manager) runTerminalObserverSafely(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.
				WithField("namespace", task.Namespace).
				WithField("task_id", task.ID).
				Errorf("distributedtask: terminal observer panicked; dropping this event and keeping the drainer alive: %v", r)
			// This recover fires before GoWrapper's, so report the way it
			// would have: Sentry capture plus the stack, or the operator
			// only ever sees the panic value.
			entsentry.Recover(r)
			enterrors.PrintStack(m.logger)
		}
	}()
	m.runTerminalObserver(task)
}

// runTerminalObserver looks the observer up under its own lock instead of
// carrying it from the apply path, so observer code never runs with the
// apply lock held.
func (m *Manager) runTerminalObserver(task *Task) {
	m.mu.RLock()
	observer := m.terminalObservers[task.Namespace]
	m.mu.RUnlock()
	if observer == nil {
		return
	}
	observer(task)
}

// dispatchTerminalWithLock hands a terminal task to the drainer. Caller holds m.mu.
//
// catchingUp skips reannouncing endings already in the local log at startup;
// see [TerminalObserver] for full delivery semantics.
//
// Runs off the apply path because observers take locks also held by
// HTTP/admission code, and running inline could stall the FSM behind a
// blocked observer. The task is cloned because m.tasks keeps mutating it.
func (m *Manager) dispatchTerminalWithLock(task *Task, catchingUp bool) {
	if m.terminalDispatchClosed {
		// The drainer is gone; without this, applies still arriving on the
		// way down would fill the queue with nothing left to empty it.
		return
	}
	if catchingUp {
		m.logger.WithFields(logrus.Fields{
			"namespace": task.Namespace,
			"task_id":   task.ID,
			"version":   task.Version,
		}).Debug("distributedtask: skipping the terminal observer for an ending replayed from the RAFT log")
		return
	}

	clone := task.Clone()
	if m.terminalObservers[task.Namespace] == nil {
		m.holdTerminalUntilRegisteredWithLock(clone)
		return
	}
	m.enqueueTerminalWithLock(clone)
}

// holdTerminalUntilRegisteredWithLock parks a live ending whose namespace has
// no observer yet. Caller holds m.mu. Oldest entries are dropped past the
// bound: a namespace that never registers must not grow this without limit.
func (m *Manager) holdTerminalUntilRegisteredWithLock(clone *Task) {
	pending := append(m.terminalPending[clone.Namespace], clone)
	if len(pending) > terminalPendingPerNamespace {
		m.logger.WithFields(logrus.Fields{
			"namespace": clone.Namespace,
			"task_id":   clone.ID,
		}).Error("distributedtask: no terminal observer is registered yet and the pre-registration buffer is full; dropping the oldest terminal event")
		pending = pending[len(pending)-terminalPendingPerNamespace:]
	}
	m.terminalPending[clone.Namespace] = pending
}

// enqueueTerminalWithLock hands one already-cloned ending to the drainer.
// Caller holds m.mu.
func (m *Manager) enqueueTerminalWithLock(clone *Task) {
	select {
	case m.terminalDispatch <- clone:
	default:
		logger := m.logger
		fields := logrus.Fields{"namespace": clone.Namespace, "task_id": clone.ID, "version": clone.Version}
		if m.terminalOverflowInFlight.Load() >= terminalDispatchOverflowLimit {
			// Past the bound the observer is wedged, not just behind.
			logger.WithFields(fields).Error("distributedtask: terminal-observer queue is full and the overflow bound is reached; dropping this terminal event")
			return
		}
		logger.WithFields(fields).Error("distributedtask: terminal-observer queue is full, the observer is not keeping up; dispatching this one separately")
		m.terminalOverflowInFlight.Add(1)
		done := m.terminalDispatchDone
		enterrors.GoWrapper(func() {
			defer m.terminalOverflowInFlight.Add(-1)
			// Not joined by Close (it holds the lock this goroutine would need
			// to look itself up), so this check is the only stop signal.
			select {
			case <-done:
				return
			default:
			}
			m.runTerminalObserverSafely(clone)
		}, logger)
	}
}

// DeleteTasksForCollection drops tasks whose payload binds to `collection`. Called
// from the schema FSM on DELETE_CLASS so a drop+recreate of the same class name
// starts with a clean task slate. Empty `collection` is rejected (an extractor
// emitting ("", true) on stray bytes would otherwise wipe the cluster).
// See weaviate/0-weaviate-issues#231.
func (m *Manager) DeleteTasksForCollection(collection string) []TaskDescriptor {
	if collection == "" {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var removed []TaskDescriptor
	for namespace, tasksByID := range m.tasks {
		extractor, ok := m.collectionExtractors[namespace]
		if !ok || extractor == nil {
			continue
		}
		for taskID, task := range tasksByID {
			c, ok := extractor(task.Payload)
			if !ok || c != collection {
				continue
			}
			delete(tasksByID, taskID)
			removed = append(removed, task.TaskDescriptor)
		}
	}
	return removed
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
		if err := cd.CheckConflict(r.Payload, m.sortedTasksWithLock(r.Namespace)); err != nil {
			return fmt.Errorf("task %s/%s conflicts with existing task: %w", r.Namespace, r.Id, err)
		}
	}

	newTask := &Task{
		Namespace:               r.Namespace,
		TaskDescriptor:          TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:                 r.Payload,
		NeedsPreparationBarrier: r.NeedsPreparationBarrier,
		Status:                  TaskStatusStarted,
		StartedAt:               time.UnixMilli(r.SubmittedAtUnixMillis),
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
func (m *Manager) RecordUnitCompletion(c *api.ApplyRequest, catchingUp bool) error {
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

	// Defense in depth for weaviate/0-weaviate-issues#240:
	// LocalGroupUnitIDs orphans units with empty NodeID, suppressing
	// every post-completion callback for that (shard, replica).
	if u.NodeID == "" {
		u.NodeID = r.NodeId
	}

	finishedAt := time.UnixMilli(r.FinishedAtUnixMillis)

	if r.Error != "" {
		u.Status = UnitStatusFailed
		u.Error = r.Error
		u.FinishedAt = finishedAt
		task.Status = TaskStatusFailed
		task.Error = fmt.Sprintf("unit %s failed: %s", r.UnitId, r.Error)
		task.FinishedAt = finishedAt
		m.dispatchTerminalWithLock(task, catchingUp)
		m.notifySchedulerWithLock()
		return nil
	}

	u.Status = UnitStatusCompleted
	u.Progress = 1.0
	u.FinishedAt = finishedAt

	if task.AllUnitsTerminal() {
		failedClosed := false
		if task.AnyUnitFailed() {
			// Fail-closed: AnyUnitFailed only trips via a restored snapshot
			// (see its godoc). Without this branch such a task would advance
			// to SWAPPING and run the schema flip on a half-failed migration.
			task.Status = TaskStatusFailed
			// Name a reason: FAILED with an empty Error leaves the operator
			// nothing to act on.
			//
			// Accepted cross-version cost: an older peer replaying this same
			// entry leaves Error empty, so during a rolling upgrade GET
			// /v1/tasks answers differently depending on which node serves
			// it. The status transition is identical on both binaries, no
			// production code outside serialization reads Task.Error, and the
			// field is snapshot-serialized, so a leader snapshot install
			// converges it. A version gate would buy nothing but delay.
			task.Error = "task restored with a failed unit; failing the task rather than running the schema flip"
			if unitID, unitErr, ok := task.firstFailedUnit(); ok {
				if unitErr == "" {
					unitErr = "no error recorded"
				}
				task.Error = fmt.Sprintf("%s: unit %s failed: %s", task.Error, unitID, unitErr)
			}
			failedClosed = true
		} else if task.NeedsPreparationBarrier {
			// Barrier tasks go through PREPARING; others jump to SWAPPING.
			task.Status = TaskStatusPreparing
		} else {
			task.Status = TaskStatusSwapping
		}
		// FinishedAt = when units completed — for PREPARING and SWAPPING alike.
		// The scheduler's TTL cleanup excludes every non-terminal status
		// (IsActive), so this stamp cannot clean a task mid-coordination.
		task.FinishedAt = finishedAt

		// Dispatch after FinishedAt is stamped so the observer's copy carries it.
		if failedClosed {
			m.dispatchTerminalWithLock(task, catchingUp)
		}
	}

	// Notify on every unit completion — even when not the last one — so
	// the Scheduler can react to per-group barriers opening as soon as
	// the cluster-wide AllGroupUnitsTerminal predicate becomes true. The
	// Scheduler decides which callbacks to actually fire; here we just
	// ensure it gets a chance to look.
	m.notifySchedulerWithLock()
	return nil
}

// RecordPostCompletionAck records one node's SWAP-phase ack on the task.
// Gates SWAPPING → FINISHED on every expected ack landing successfully;
// any Success=false flips to FAILED, which skips the cluster-wide schema
// flip in OnTaskCompleted. Idempotent: first ack per (task, node) wins;
// late acks against terminal states are silently dropped.
func (m *Manager) RecordPostCompletionAck(c *api.ApplyRequest, catchingUp bool) error {
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

	switch task.Status {
	case TaskStatusFailed, TaskStatusFinished, TaskStatusCancelled:
		// Past the barrier — silently drop straggler acks.
		return nil
	case TaskStatusStarted, TaskStatusSwapping:
	default:
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot record post-completion ack from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	if task.PostCompletionAcks == nil {
		task.PostCompletionAcks = map[string]PostCompletionAck{}
	}
	if _, present := task.PostCompletionAcks[r.NodeId]; present {
		// First ack per (task, node) wins; later retries are no-ops.
		return nil
	}

	task.PostCompletionAcks[r.NodeId] = PostCompletionAck{
		Success: r.Success,
		Error:   r.Error,
		AckedAt: time.UnixMilli(r.AckedAtUnixMillis),
	}

	// Any failure flips the task to FAILED immediately; later acks are
	// still recorded for forensic value. FinishedAt is not updated —
	// "when did the work end" should remain the AllUnitsTerminal moment.
	if !r.Success && task.Status == TaskStatusSwapping {
		task.Status = TaskStatusFailed
		ackErr := fmt.Sprintf("post-completion swap failed on node %s: %s", r.NodeId, r.Error)
		if task.Error != "" {
			task.Error = task.Error + "; " + ackErr
		} else {
			task.Error = ackErr
		}
		m.dispatchTerminalWithLock(task, catchingUp)
	}

	m.notifySchedulerWithLock()
	return nil
}

// RecordPreparationCompleteAck records one node's PREP-phase ack on the task.
// Gates PREPARING → SWAPPING on every expected ack landing successfully;
// any Success=false flips the task to FAILED, holding the barrier so no
// node proceeds to the atomic swap. Idempotent: first ack per (task,
// node) wins; late acks against terminal states are silently dropped.
//
// Specifically:
//
//   - Ack arrives for an idempotently-already-acked (task, node): no-op,
//     the first ack wins.
//   - Ack arrives for a task no longer in a state that can use it
//     (FAILED / FINISHED / CANCELLED, or SWAPPING/FINISHED after the
//     barrier has already lifted): no-op.
//   - Ack with Success==false arrives while the task is PREPARING:
//     records the ack AND transitions the task to FAILED.
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
// and identical on every node) plus the PreparationCompletionAcks state — so
// every node's Manager arrives at the transition on the same apply.
func (m *Manager) RecordPreparationCompleteAck(c *api.ApplyRequest, catchingUp bool) error {
	var r api.RecordDistributedTaskPreparationCompleteAckRequest
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

	switch task.Status {
	case TaskStatusFailed, TaskStatusFinished, TaskStatusCancelled, TaskStatusSwapping:
		// Past the barrier — silently drop the late ack.
		return nil
	case TaskStatusStarted, TaskStatusPreparing:
		// STARTED accepted defensively to absorb the AllUnitsTerminal-vs-
		// PrepAck emission race.
	default:
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot record prep-complete ack from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	if task.PreparationCompletionAcks == nil {
		task.PreparationCompletionAcks = map[string]PostCompletionAck{}
	}
	if _, present := task.PreparationCompletionAcks[r.NodeId]; present {
		// Idempotent: the first ack per (task, node) wins.
		return nil
	}

	task.PreparationCompletionAcks[r.NodeId] = PostCompletionAck{
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
		m.dispatchTerminalWithLock(task, catchingUp)
		m.notifySchedulerWithLock()
		return nil
	}

	// Success path: if every expected ack has landed successfully,
	// transition PREPARING → SWAPPING. This is the moment the barrier
	// lifts cluster-wide.
	if r.Success && task.Status == TaskStatusPreparing && allExpectedPreparationAcksLanded(task) {
		task.Status = TaskStatusSwapping
	}

	m.notifySchedulerWithLock()
	return nil
}

// allExpectedPreparationAcksLanded returns true iff every node owning at least
// one unit on the task has recorded a successful PrepCompletionAck. Pure
// transform; caller holds [Manager.mu]. By the time the task is in
// PREPARING, every unit is terminal so the expected set is fully known.
func allExpectedPreparationAcksLanded(task *Task) bool {
	expected := map[string]struct{}{}
	for _, u := range task.Units {
		if u.NodeID != "" {
			expected[u.NodeID] = struct{}{}
		}
	}
	for node := range expected {
		ack, ok := task.PreparationCompletionAcks[node]
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

// MarkTaskFailed transitions SWAPPING → FAILED when a node's
// [UnitAwareProvider.OnTaskCompleted] returns a terminal error, so a
// swallowed cutover failure can't leave the task FINISHED with an
// un-flipped schema (weaviate/0-weaviate-issues#297).
//
// Idempotent at the FSM layer: the first commit wins; a later call on an
// already-FAILED task is a no-op, and one racing a peer's FINISHED/CANCELLED
// is refused. FinishedAt stays at the AllUnitsTerminal moment.
func (m *Manager) MarkTaskFailed(c *api.ApplyRequest, catchingUp bool) error {
	var r api.MarkTaskFailedRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal mark task failed request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	switch task.Status {
	case TaskStatusFailed:
		// Idempotent: another node's MarkTaskFailed already committed.
		return nil
	case TaskStatusSwapping:
	default:
		// FINISHED / CANCELLED / STARTED — refuse so a stale command can't
		// overwrite a terminal status a peer (or the operator) committed.
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot be failed from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	task.Status = TaskStatusFailed
	if r.Error != "" {
		if task.Error != "" {
			task.Error = task.Error + "; " + r.Error
		} else {
			task.Error = r.Error
		}
	}
	m.dispatchTerminalWithLock(task, catchingUp)
	m.notifySchedulerWithLock()
	return nil
}

// UpdateUnitProgress also handles initial node assignment: the first progress update for an
// unassigned unit sets its NodeID, claiming it for that node. After assignment, updates from
// other nodes are rejected. Progress updates to terminal units are silently ignored (no error)
// because in-flight Raft commands may arrive after a unit has already completed.
//
// Stored Progress is monotonic per task version; only NodeID and UpdatedAt are applied when
// the requested Progress regresses. Receiver-side defence against sender-side miscomputation.
// See weaviate/0-weaviate-issues#232.
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
	if r.Progress > u.Progress {
		u.Progress = r.Progress
	} else if r.Progress < u.Progress {
		// Sender-side regression: surface so future emitter bugs don't
		// hide behind the receiver clamp. Debug-only — under steady-state
		// monotonic senders this branch is unreachable.
		m.logger.WithField("namespace", r.Namespace).
			WithField("task_id", r.Id).
			WithField("unit_id", r.UnitId).
			WithField("stored_progress", u.Progress).
			WithField("requested_progress", r.Progress).
			Debug("distributedtask: clamping unit-progress regression (sender bug)")
	}
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

// CancelTask transitions a STARTED task to CANCELLED and refuses every other status,
// including one this build cannot name (see [TaskStatus.IsCancellable]). In-flight units
// are not waited for — the [Scheduler] will terminate their local handles on the next tick.
func (m *Manager) CancelTask(a *api.ApplyRequest, catchingUp bool) error {
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

	// [TaskStatus.IsCancellable] is a literal, so every binary that
	// replays this entry writes CANCELLED under exactly the same
	// condition. Classifying instead would let a node that has never
	// heard of the status cancel a migration a newer node is still
	// coordinating, and follower apply errors are discarded, so the
	// divergence would be silent.
	//
	// The operator-facing message for the coordination phases lives in
	// the REST layer, which is free to classify because nothing
	// downstream replays its answer.
	if !task.Status.IsCancellable() {
		return errTaskNotRunning(r.Namespace, r.Id, task.Version)
	}

	task.Status = TaskStatusCancelled
	task.FinishedAt = time.UnixMilli(r.CancelledAtUnixMillis)
	m.dispatchTerminalWithLock(task, catchingUp)
	m.notifySchedulerWithLock()
	return nil
}

// CleanUpTask removes a task from the Manager's state. It refuses tasks in a status this
// build both declared and calls live, and tasks whose completedTaskTTL has not yet elapsed,
// preventing premature removal of status information that other nodes may still need to
// observe. A status this build cannot name is removable — see the guard below, that exit is
// the only one such a task has.
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

	// Refuse for every status this build both declared and calls live —
	// not just STARTED. A non-terminal task's FinishedAt is either zero
	// (STARTED) or the units-completion moment (PREPARING/SWAPPING); both
	// clear the age check below, so only this check stands between a task
	// mid-coordination and deletion.
	//
	// A status this build cannot name is deleted instead. The only
	// proposer is the Scheduler's TTL sweep, which reads the leader's
	// view (pinned by TestStructuralInvariant_TTLSweepIsTheOnlyCleanUpProposer),
	// so a CLEAN_UP for such a task exists only once the cluster already
	// considers it done. Nothing else can move the entry: no transition
	// advances a status this build cannot name (MarkTaskFinalized refuses
	// every status but FINISHED and SWAPPING) and no later sweep sees it,
	// while it keeps blocking schema mutations and backups on its
	// collection through the local map.
	//
	// So the exit fires only when some node in the cluster classifies the
	// status as terminal. For a new non-terminal status nothing ever
	// proposes a CLEAN_UP and this node stays pinned either way — which
	// is the cheap direction, and the one IsTerminal's godoc says a new
	// status has to be introduced in.
	if task.Status.IsActive() && task.Status.IsRecognized() {
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
		// "In flight" = every non-terminal status (via
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

// LocalUnrecognizedDistributedTasks returns this node's own copies of tasks in a
// status this build never declared, grouped by namespace.
//
// Only [Manager.Restore] can put one here — every other write to
// Task.Status is a literal from this build's vocabulary — so the source
// is always a snapshot from a node running a newer release. It matters
// because the leader-routed list stops carrying such a task once the
// peers clean their copies up, which is exactly when a leftover local
// copy starts silently refusing schema mutations. Returns clones; the
// map is empty in the ordinary case.
func (m *Manager) LocalUnrecognizedDistributedTasks() map[string][]*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result map[string][]*Task
	for namespace, tasks := range m.tasks {
		for _, task := range tasks {
			if task.Status.IsRecognized() {
				continue
			}
			if result == nil {
				result = map[string][]*Task{}
			}
			result[namespace] = append(result[namespace], task.Clone())
		}
	}
	return result
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

// Restore merges the tasks from a Raft snapshot produced by [Manager.Snapshot]
// into the Manager's in-memory state: entries are upserted per (namespace, task
// ID), none are removed. It is called during Raft leader election or when a
// follower installs a snapshot from the leader.
//
// Tasks already terminal in the snapshot do not fire their [TerminalObserver].
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
