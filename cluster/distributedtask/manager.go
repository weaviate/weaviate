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
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// Manager is responsible for managing distributed tasks across the cluster.
type Manager struct {
	mu    sync.Mutex
	tasks map[string]map[string]*Task // namespace -> taskID -> Task

	completedTaskTTL           time.Duration
	subUnitProgressMinInterval time.Duration

	clock clockwork.Clock
}

type ManagerParameters struct {
	Clock clockwork.Clock

	CompletedTaskTTL time.Duration

	// SubUnitProgressMinInterval is the minimum time between progress updates for the same
	// sub-unit. Progress updates that arrive more frequently are silently dropped to bound
	// Raft write volume. Defaults to 30 seconds when zero.
	SubUnitProgressMinInterval time.Duration
}

// defaultSubUnitProgressMinInterval is the default throttle interval for sub-unit progress updates.
const defaultSubUnitProgressMinInterval = 30 * time.Second

func NewManager(params ManagerParameters) *Manager {
	if params.Clock == nil {
		params.Clock = clockwork.NewRealClock()
	}
	if params.SubUnitProgressMinInterval == 0 {
		params.SubUnitProgressMinInterval = defaultSubUnitProgressMinInterval
	}

	return &Manager{
		tasks: make(map[string]map[string]*Task),

		completedTaskTTL:           params.CompletedTaskTTL,
		subUnitProgressMinInterval: params.SubUnitProgressMinInterval,

		clock: params.Clock,
	}
}

func (m *Manager) AddTask(c *api.ApplyRequest, seqNum uint64) error {
	// Use AddDistributedTaskRequestWithSubUnits to handle both the legacy path
	// (SubUnitIds == nil) and the sub-unit path in a single unmarshal.
	var r api.AddDistributedTaskRequestWithSubUnits
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

	newTask := &Task{
		Namespace:      r.Namespace,
		TaskDescriptor: TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:        r.Payload,
		Status:         TaskStatusStarted,
		StartedAt:      time.UnixMilli(r.SubmittedAtUnixMillis),
		FinishedNodes:  map[string]bool{},
	}

	if len(r.SubUnitIds) > 0 {
		newTask.SubUnits = make(map[string]*SubUnit, len(r.SubUnitIds))
		for _, id := range r.SubUnitIds {
			newTask.SubUnits[id] = &SubUnit{
				ID:     id,
				Status: SubUnitStatusPending,
			}
		}
	}

	m.setTaskWithLock(newTask)

	return nil
}

func (m *Manager) RecordNodeCompletion(c *api.ApplyRequest, numberOfNodesInTheCluster int) error {
	var r api.RecordDistributedTaskNodeCompletionRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record task node completion request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return fmt.Errorf("task %s/%s/%d is no longer running", r.Namespace, r.Id, task.Version)
	}

	if r.Error != nil {
		task.Status = TaskStatusFailed
		task.Error = *r.Error
		task.FinishedAt = time.UnixMilli(r.FinishedAtUnixMillis)
		return nil
	}

	task.FinishedNodes[r.NodeId] = true
	if len(task.FinishedNodes) == numberOfNodesInTheCluster {
		task.Status = TaskStatusFinished
		task.FinishedAt = time.UnixMilli(r.FinishedAtUnixMillis)
		return nil
	}

	return nil
}

// RecordSubUnitCompletion applies a sub-unit completion or failure event.
// It is the FSM apply handler for TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_COMPLETED.
// When all sub-units reach COMPLETED the task transitions to FINISHED.
// If the sub-unit has an Error set, the task transitions to FAILED immediately.
func (m *Manager) RecordSubUnitCompletion(c *api.ApplyRequest) error {
	var r api.RecordDistributedTaskSubUnitCompletedRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record sub-unit completion request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return fmt.Errorf("task %s/%s/%d is no longer running", r.Namespace, r.Id, task.Version)
	}

	if task.SubUnits == nil {
		return fmt.Errorf("task %s/%s/%d does not use sub-unit tracking", r.Namespace, r.Id, task.Version)
	}

	su, ok := task.SubUnits[r.SubUnitId]
	if !ok {
		return fmt.Errorf("sub-unit %s not found in task %s/%s/%d", r.SubUnitId, r.Namespace, r.Id, task.Version)
	}

	finishedAt := time.UnixMilli(r.FinishedAtUnixMillis)

	if r.Error != nil {
		su.Status = SubUnitStatusFailed
		su.Error = *r.Error
		su.NodeID = r.NodeId
		su.UpdatedAt = finishedAt

		task.Status = TaskStatusFailed
		task.Error = fmt.Sprintf("sub-unit %s failed: %s", r.SubUnitId, *r.Error)
		task.FinishedAt = finishedAt
		return nil
	}

	su.Status = SubUnitStatusCompleted
	su.Progress = 1.0
	su.NodeID = r.NodeId
	su.UpdatedAt = finishedAt

	// Transition task to FINISHED when every sub-unit is completed.
	allDone := true
	for _, s := range task.SubUnits {
		if s.Status != SubUnitStatusCompleted {
			allDone = false
			break
		}
	}
	if allDone {
		task.Status = TaskStatusFinished
		task.FinishedAt = finishedAt
	}

	return nil
}

// RecordSubUnitProgress applies a sub-unit progress update.
// Updates that arrive more frequently than SubUnitProgressMinInterval are silently dropped.
// It is the FSM apply handler for TYPE_DISTRIBUTED_TASK_RECORD_SUB_UNIT_PROGRESS.
func (m *Manager) RecordSubUnitProgress(c *api.ApplyRequest) error {
	var r api.RecordDistributedTaskSubUnitProgressRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return fmt.Errorf("unmarshal record sub-unit progress request: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return fmt.Errorf("task %s/%s/%d is no longer running", r.Namespace, r.Id, task.Version)
	}

	if task.SubUnits == nil {
		return fmt.Errorf("task %s/%s/%d does not use sub-unit tracking", r.Namespace, r.Id, task.Version)
	}

	su, ok := task.SubUnits[r.SubUnitId]
	if !ok {
		return fmt.Errorf("sub-unit %s not found in task %s/%s/%d", r.SubUnitId, r.Namespace, r.Id, task.Version)
	}

	// Throttle: silently drop updates that arrive too frequently for the same sub-unit.
	if !su.UpdatedAt.IsZero() && m.clock.Since(su.UpdatedAt) < m.subUnitProgressMinInterval {
		return nil
	}

	su.Status = SubUnitStatusInProgress
	su.Progress = r.Progress
	su.NodeID = r.NodeId
	su.UpdatedAt = m.clock.Now()

	return nil
}

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
		return fmt.Errorf("task %s/%s/%d is no longer running", r.Namespace, r.Id, task.Version)
	}

	task.Status = TaskStatusCancelled
	task.FinishedAt = time.UnixMilli(r.CancelledAtUnixMillis)
	return nil
}

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

func (m *Manager) ListDistributedTasks(_ context.Context) (map[string][]*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make(map[string][]*Task, len(m.tasks))
	for namespace, tasks := range m.tasks {
		if len(tasks) == 0 {
			continue
		}

		result[namespace] = make([]*Task, 0, len(tasks))
		for _, task := range tasks {
			result[namespace] = append(result[namespace], task.Clone())
		}
	}
	return result, nil
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
		return nil, fmt.Errorf("task %s/%s/%d does not exist", namespace, taskID, taskVersion)
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
