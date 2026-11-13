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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
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
	// [Manager.AddTask] before appending a new task. A missing namespace
	// adds no rejection.
	conflictDetectors map[string]ConflictDetector

	// schemaMutationDetectors is the per-namespace registry consulted
	// by the schema FSM's mutation apply paths (see
	// [SchemaMutationDetector] godoc for the motivating bug).
	schemaMutationDetectors map[string]SchemaMutationDetector

	readOnlyClass func(collection string) *models.Class

	// Per-namespace payload→collection extractors. Absent ⇒ namespace is
	// not collection-scoped and survives DeleteTasksForCollection.
	collectionExtractors   map[string]CollectionExtractor
	targetVectorExtractors map[string]TargetVectorExtractor

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

func (m *Manager) CheckMigrationPropertyUpdate(className, propertyName string, run api.MigrationRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate, ok := m.schemaMutationDetectors[run.Namespace].(CutoverOutcome)
	if !ok {
		return nil
	}
	task := m.tasks[run.Namespace][run.ID]
	if task != nil && task.Version != run.Version {
		task = nil
	}
	return gate.CheckMigrationPropertyUpdate(className, propertyName, task)
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

// CheckVectorConfigRemoval consults every registered [SchemaMutationDetector]
// that also implements [VectorConfigRemovalGate]. Called from the schema FSM's
// UpdateClass apply; a non-nil error rejects the update. Same RAFT-determinism
// contract as [Manager.CheckClassMutation].
func (m *Manager) CheckVectorConfigRemoval(className string, removedVectors, shards []string) error {
	if len(removedVectors) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for namespace, detector := range m.schemaMutationDetectors {
		gate, ok := detector.(VectorConfigRemovalGate)
		if !ok {
			continue
		}
		var existing []*Task
		for _, t := range m.tasks[namespace] {
			existing = append(existing, t)
		}
		if err := gate.CheckVectorConfigRemoval(className, removedVectors, shards, existing); err != nil {
			return err
		}
	}
	return nil
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
	Logger logrus.FieldLogger

	// ConflictDetectors and SchemaMutationDetectors decide which applies the
	// Manager refuses. They are set only here, before RAFT replay starts, so a
	// replayed entry gets the verdict this build gives it live.
	ConflictDetectors       map[string]ConflictDetector
	SchemaMutationDetectors map[string]SchemaMutationDetector
	ReadOnlyClass           func(collection string) *models.Class
}

func NewManager(params ManagerParameters) *Manager {
	return &Manager{
		tasks:                  make(map[string]map[string]*Task),
		collectionExtractors:   make(map[string]CollectionExtractor),
		targetVectorExtractors: make(map[string]TargetVectorExtractor),

		conflictDetectors:       params.ConflictDetectors,
		schemaMutationDetectors: params.SchemaMutationDetectors,
		readOnlyClass:           params.ReadOnlyClass,

		logger: params.Logger,
	}
}

// RegisterCollectionExtractor opts a task namespace into DeleteTasksForCollection's
// cascade. Extractor may run concurrently, with or without the Manager lock — must not block or recurse. Last
// write wins per namespace; nil / empty arguments are silently dropped.
// It also keeps this namespace's tasks and replica movements from running on the same collection at once (HasActiveTaskForCollection, CollectionOfTask).
func (m *Manager) RegisterCollectionExtractor(namespace string, extractor CollectionExtractor) {
	if namespace == "" || extractor == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collectionExtractors[namespace] = extractor
}

// cacheCollectionWithLock runs once per add or restore, since HasActiveTaskForCollection runs on every replica copy.
func (m *Manager) cacheCollectionWithLock(task *Task) {
	if extractor := m.collectionExtractors[task.Namespace]; extractor != nil {
		if c, ok := extractor(task.Payload); ok {
			task.collection = c
		}
	}
}

// RegisterTargetVectorExtractor opts a task namespace into
// PurgeTasksForCollectionTargetVectors. Same contract as RegisterCollectionExtractor.
func (m *Manager) RegisterTargetVectorExtractor(namespace string, extractor TargetVectorExtractor) {
	if namespace == "" || extractor == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.targetVectorExtractors[namespace] = extractor
}

// ErrTaskStillActiveForTargetVectors means PurgeTasksForCollectionTargetVectors found an
// ACTIVE task bound to the given (collection, targets) and deleted nothing.
var ErrTaskStillActiveForTargetVectors = errors.New("a task bound to these targets is still active")

// PurgeTasksForCollectionTargetVectors drops NON-ACTIVE tasks whose payload binds to
// `collection` (case-insensitively, like every other consumer of these
// payloads) and overlaps `targets`, in namespaces with a registered target
// extractor. Called deterministically from the schema FSM when a new
// drop-vector marker is introduced, so the previous drop of a re-created name
// leaves no records for coverage inheritance or removal vouching to
// misattribute to the new drop. An ACTIVE match aborts with
// [ErrTaskStillActiveForTargetVectors] and deletes NOTHING: active tasks must keep
// the scheduler's running handles consistent (the previous drop's task can
// still be SWAPPING for a moment after its finalize removed the old marker),
// and such a survivor next to a fresh marker would seed stale coverage
// inheritance — the caller must refuse the marker introduction and let the
// client retry once the task settles. NOTE: SWAPPING normally lasts
// milliseconds, but a node that dies before delivering its post-completion
// ack holds SWAPPING until it acks; only DeleteClass clears such a wedge.
//
// Overlap semantics: a record is purged WHOLE when ANY of its target vectors
// overlaps `targets` — records are atomic, so a multi-target record's
// coverage for the non-purged siblings is sacrificed with it (they pay one
// idempotent re-clean; leaving the record would instead misattribute its
// stale coverage to the NEW drop, which is the corruption this purge
// prevents). Every live caller today passes a single target.
func (m *Manager) PurgeTasksForCollectionTargetVectors(collection string, targets []string) ([]TaskDescriptor, error) {
	if collection == "" || len(targets) == 0 {
		return nil, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	targetVectorSet := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		targetVectorSet[t] = struct{}{}
	}
	type match struct {
		namespace, taskID string
		desc              TaskDescriptor
	}
	var matches []match
	for namespace, tasksByID := range m.tasks {
		extractor, ok := m.targetVectorExtractors[namespace]
		if !ok || extractor == nil {
			continue
		}
		for taskID, task := range tasksByID {
			c, taskTargetVectors, ok := extractor(task.Payload)
			// EqualFold: collection names are case-insensitive identifiers, and a
			// byte-exact purge would be strictly weaker than the EqualFold
			// inheritance match whose soundness leans on it.
			if !ok || !strings.EqualFold(c, collection) {
				continue
			}
			overlaps := false
			for _, t := range taskTargetVectors {
				if _, ok := targetVectorSet[t]; ok {
					overlaps = true
					break
				}
			}
			if !overlaps {
				continue
			}
			if task.Status.IsActive() {
				return nil, fmt.Errorf("%w: task %s/%s (status %s)",
					ErrTaskStillActiveForTargetVectors, namespace, taskID, task.Status)
			}
			matches = append(matches, match{namespace: namespace, taskID: taskID, desc: task.TaskDescriptor})
		}
	}
	var removed []TaskDescriptor
	for _, hit := range matches {
		delete(m.tasks[hit.namespace], hit.taskID)
		removed = append(removed, hit.desc)
	}
	return removed, nil
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
			// EqualFold, matching PurgeTasksForCollectionTargetVectors and every
			// payload consumer: collection names are case-insensitive
			// identifiers, and a byte-exact cascade would leak a case-twin
			// record past DELETE_CLASS.
			if !ok || !strings.EqualFold(c, collection) {
				continue
			}
			delete(tasksByID, taskID)
			removed = append(removed, task.TaskDescriptor)
		}
	}
	return removed
}

func (m *Manager) ForgetGroupsForCollection(collection string, groups []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for namespace := range m.collectionExtractors {
		for _, task := range m.tasks[namespace] {
			if task.Status.IsActive() || !task.NeedsPreparationBarrier ||
				!strings.EqualFold(task.collection, collection) {
				continue
			}
			task.forgetGroups(groups)
		}
	}
}

func (m *Manager) CollectionOfTask(namespace string, payload []byte) string {
	extractor := m.collectionExtractor(namespace)
	if extractor == nil {
		return ""
	}
	collection, _ := extractor(payload)
	return collection
}

func (m *Manager) collectionExtractor(namespace string) CollectionExtractor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.collectionExtractors[namespace]
}

func (m *Manager) HasActiveTaskForCollection(collection string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for ns := range m.collectionExtractors {
		for _, task := range m.tasks[ns] {
			if task.Status.IsActive() && strings.EqualFold(task.collection, collection) {
				return true
			}
		}
	}
	return false
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

	// Cluster-wide conflict check: if [ManagerParameters.ConflictDetectors]
	// holds a [ConflictDetector] for this namespace, give it the chance to
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
			// Wrap the permanent-rejection sentinel (see [ErrTaskConflict]) so
			// the REST submit path classifies this as 409, not 500.
			return wrapPermanent(ErrTaskConflict,
				fmt.Sprintf("task %s/%s conflicts with existing task: %v", r.Namespace, r.Id, err))
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
	newTask.IndexGroups()

	m.cacheCollectionWithLock(newTask)
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

	u := task.Units[unitID]
	if u == nil {
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

	// Defense in depth for weaviate/0-weaviate-issues#240:
	// LocalGroupUnitIDs orphans units with empty NodeID, suppressing
	// every post-completion callback for that (shard, replica).
	if u.NodeID == "" {
		u.NodeID = r.NodeId
	}

	finishedAt := time.UnixMilli(r.FinishedAtUnixMillis)

	if r.Error != "" {
		u.markTerminal(UnitStatusFailed, finishedAt)
		u.Error = r.Error
		task.Error = fmt.Sprintf("unit %s failed: %s", r.UnitId, r.Error)
		task.markTerminal(TaskStatusFailed, finishedAt)
		m.notifySchedulerWithLock()
		return nil
	}

	u.markTerminal(UnitStatusCompleted, finishedAt)
	u.Progress = 1.0

	if task.AllUnitsTerminal() {
		if task.AnyUnitFailed() {
			// Failures are terminal immediately — there are no
			// post-task callbacks to wait for on the failure path
			// (the schema flip is intentionally skipped when any
			// unit failed; see OnTaskCompleted's early-return for
			// FAILED tasks).
			task.markTerminal(TaskStatusFailed, finishedAt)
		} else {
			// Barrier tasks go through PREPARING; others jump to SWAPPING.
			// Both are non-terminal, so neither stamps FinishedAt: the unit
			// work has stopped but the cutover has not run yet.
			if task.NeedsPreparationBarrier {
				task.Status = TaskStatusPreparing
			} else {
				task.Status = TaskStatusSwapping
			}
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
// A barrier task's successful ack pays a swap debt instead, and a failed one
// changes nothing: its node retries the swap until it pays.
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
	if task.NeedsPreparationBarrier {
		if r.Success {
			task.paySwap(r.GroupId, r.NodeId)
		}
		m.notifySchedulerWithLock()
		return nil
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

	ackedAt := time.UnixMilli(r.AckedAtUnixMillis)
	task.PostCompletionAcks[r.NodeId] = PostCompletionAck{
		Success: r.Success,
		Error:   r.Error,
		AckedAt: ackedAt,
	}

	// Any failure flips the task to FAILED immediately.
	if !r.Success && task.Status == TaskStatusSwapping {
		ackErr := fmt.Sprintf("post-completion swap failed on node %s: %s", r.NodeId, r.Error)
		task.appendError(ackErr)
		task.markTerminal(TaskStatusFailed, ackedAt)
	}

	m.notifySchedulerWithLock()
	return nil
}

func recordNodePreparationAck(task *Task, r *api.RecordDistributedTaskPreparationCompleteAckRequest) {
	if _, present := task.NodePreparationAcks[r.NodeId]; present {
		return
	}
	if task.NodePreparationAcks == nil {
		task.NodePreparationAcks = map[string]PostCompletionAck{}
	}
	ackedAt := time.UnixMilli(r.AckedAtUnixMillis)
	task.NodePreparationAcks[r.NodeId] = PostCompletionAck{Success: r.Success, Error: r.Error, AckedAt: ackedAt}
	if task.Status != TaskStatusPreparing {
		return
	}
	if !r.Success {
		ackErr := fmt.Sprintf("prep failed on node %s: %s", r.NodeId, r.Error)
		task.appendError(ackErr)
		task.markTerminal(TaskStatusFailed, ackedAt)
		return
	}
	for _, u := range task.Units {
		if ack, ok := task.NodePreparationAcks[u.NodeID]; u.NodeID != "" && (!ok || !ack.Success) {
			return
		}
	}
	task.Status = TaskStatusSwapping
}

// RecordPreparationCompleteAck decides a group's swap for good once every owning node acked success.
func (m *Manager) RecordPreparationCompleteAck(c *api.ApplyRequest) error {
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
		return nil
	case TaskStatusStarted, TaskStatusPreparing:
	default:
		return wrapPermanent(ErrTaskNotInFinalizingState,
			fmt.Sprintf("task %s/%s/%d cannot record prep-complete ack from status %s",
				r.Namespace, r.Id, task.Version, task.Status))
	}

	if r.GroupId == nil {
		recordNodePreparationAck(task, &r)
		m.notifySchedulerWithLock()
		return nil
	}

	group := *r.GroupId
	expected := task.groupNodes(group)
	if task.GroupSwapDecided(group) || !task.AllGroupUnitsTerminal(group) {
		return nil
	}
	if !slices.Contains(expected, r.NodeId) {
		return nil
	}
	acks := task.PreparedGroups[group]
	if _, present := acks[r.NodeId]; present {
		return nil
	}

	ackedAt := time.UnixMilli(r.AckedAtUnixMillis)
	if !r.Success {
		ackErr := fmt.Sprintf("prep failed on node %s for group %q: %s", r.NodeId, group, r.Error)
		task.appendError(ackErr)
		task.markTerminal(TaskStatusFailed, ackedAt)
		m.notifySchedulerWithLock()
		return nil
	}

	if task.PreparedGroups == nil {
		task.PreparedGroups = map[string]map[string]PostCompletionAck{}
	}
	// Clones share inner maps and encode them without the lock, so replace, never edit.
	acks = maps.Clone(acks)
	if acks == nil {
		acks = map[string]PostCompletionAck{}
	}
	acks[r.NodeId] = PostCompletionAck{Success: true, AckedAt: ackedAt}
	task.PreparedGroups[group] = acks
	if len(acks) == len(expected) {
		task.decideSwap(group, ackedAt)
		if task.Status == TaskStatusPreparing && task.AllGroupsSwapDecided() {
			task.Status = TaskStatusSwapping
		}
	}

	m.notifySchedulerWithLock()
	return nil
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

	task.markTerminal(TaskStatusFinished, time.UnixMilli(r.FinalizedAtUnixMillis))
	m.notifySchedulerWithLock()
	return nil
}

// MarkTaskFailed transitions SWAPPING → FAILED when a node's
// [UnitAwareProvider.OnTaskCompleted] returns a terminal error, so a
// swallowed cutover failure can't leave the task FINISHED with an
// un-flipped schema (weaviate/0-weaviate-issues#297).
// It records FINISHED instead when [CutoverOutcome.CutoverLanded] reports the write landed.
//
// Idempotent at the FSM layer: the first commit wins; a later call on an
// already-FAILED task is a no-op, and one racing a peer's FINISHED/CANCELLED
// is refused.
func (m *Manager) MarkTaskFailed(c *api.ApplyRequest) error {
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

	if d, ok := m.schemaMutationDetectors[r.Namespace].(CutoverOutcome); ok && d.CutoverLanded(task, m.readOnlyClass) {
		task.markTerminal(TaskStatusFinished, time.UnixMilli(r.FailedAtUnixMillis))
		m.notifySchedulerWithLock()
		return nil
	}
	if r.Error != "" {
		task.appendError(r.Error)
	}
	task.markTerminal(TaskStatusFailed, time.UnixMilli(r.FailedAtUnixMillis))
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

// CancelTask moves a STARTED or PREPARING task to CANCELLED and refuses every other status
// (see [TaskStatus.IsCancellable]). Decided groups keep their swap; SWAPPING answers [ErrNothingLeftToCancel].
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

	if task.Status == TaskStatusSwapping {
		return wrapPermanent(ErrNothingLeftToCancel,
			fmt.Sprintf("task %s/%s/%d is SWAPPING and ends once every replica that owes a swap has swapped", r.Namespace, r.Id, task.Version))
	}
	// Follower apply errors are discarded, so two binaries disagreeing on
	// what may be cancelled would diverge here silently.
	if !task.Status.IsCancellable() {
		return errTaskNotRunning(r.Namespace, r.Id, task.Version)
	}

	task.markTerminal(TaskStatusCancelled, time.UnixMilli(r.CancelledAtUnixMillis))
	m.notifySchedulerWithLock()
	return nil
}

// CleanUpTask removes a task from the Manager's state. It refuses a task that is both
// [TaskStatus.IsActive] and [TaskStatus.IsRecognized]. The TTL is judged by the proposing sweep.
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

	if task.Status.IsActive() && task.Status.IsRecognized() {
		return fmt.Errorf("task %s/%s/%d is still running", r.Namespace, r.Id, task.Version)
	}

	delete(m.tasks[task.Namespace], task.ID)
	return nil
}

// LocalDistributedTasks returns the cluster-wide task map as this node has
// applied it, grouped by namespace. Each [Task] is cloned, so callers may
// read the returned values without holding the Manager's lock.
//
// Tasks within each namespace are sorted deterministically so adjacent
// polls return the same slice order regardless of Go's randomized map
// iteration. Sort key:
//
//  1. STARTED tasks first (the currently-running work matters most).
//  2. Within priority, by activity-time DESC (newest first). Activity-time
//     is FinishedAt for terminal tasks, StartedAt otherwise.
//  3. Tiebreak by ID ASC for full stability.
func (m *Manager) LocalDistributedTasks() map[string][]*Task {
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
	return result
}

// LocalTaskStatuses copies no units: a decider can read once per record, under
// the lock RAFT applies wait on, where [Manager.LocalDistributedTasks] clones them all.
func (m *Manager) LocalTaskStatuses(namespace, shard string) []TaskStatusEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := m.tasks[namespace]
	out := make([]TaskStatusEntry, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, task.StatusEntryFor(shard))
	}
	return out
}

// This is not reached through the [TaskLister] interface in production —
// [cluster.Raft] is the concrete TaskLister the wiring hands out. It still
// has two direct production callers: [Manager.Snapshot], on the RAFT FSM's
// own snapshot path, and [Manager.ListDistributedTasksPayload], on the
// leader-routed distributed-task-list query dispatch. The TaskLister-shaped
// signature stays because six scheduler tests drive the Manager directly and
// need it that way.
func (m *Manager) ListDistributedTasks(_ context.Context) (map[string][]*Task, error) {
	return m.LocalDistributedTasks(), nil
}

// sortTasksForDisplay sorts tasks in place so the slice is identical on
// every call given the same input set. See [Manager.LocalDistributedTasks]
// for the sort-key rationale. SliceStable is intentional: equal-priority
// equal-time equal-ID inputs are byte-identical to clone anyway, but
// SliceStable documents the intent.
func sortTasksForDisplay(tasks []*Task) {
	sort.SliceStable(tasks, func(i, j int) bool {
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
// The leader-routed list stops carrying such a task once the peers clean
// their copies up, which is exactly when a leftover local copy starts
// silently refusing schema mutations.
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

func (m *Manager) DecodeTaskList(data []byte) (map[string][]*Task, error) {
	var response ListDistributedTasksResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for namespace, list := range response.Tasks {
		for _, task := range list {
			task.shareGroups(m.tasks[namespace][task.ID])
		}
	}
	return response.Tasks, nil
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

	// The FSM contract (see Store.Restore) requires discarding all previous
	// state: merging would resurrect tasks the snapshot's producer deleted
	// (e.g. records purged on a drop-vector marker introduction), and this
	// node alone would then refuse applies its peers accept.
	m.tasks = make(map[string]map[string]*Task, len(s.Tasks))
	for namespace, tasks := range s.Tasks {
		for _, task := range tasks {
			if _, ok := m.tasks[namespace]; !ok {
				m.tasks[namespace] = make(map[string]*Task)
			}

			// An older snapshot can hold a mid-coordination task with
			// FinishedAt already stamped, which would misreport work still
			// in flight.
			if !task.Status.IsTerminal() {
				task.FinishedAt = time.Time{}
			}

			task.IndexGroups()
			m.cacheCollectionWithLock(task)
			m.tasks[namespace][task.ID] = task
		}
	}

	return nil
}
