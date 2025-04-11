package distributedtask

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

var (
	ErrTaskDoesNotExist      = errors.New("task does not exist")
	ErrTaskIsNoLongerRunning = errors.New("task is no longer running")
)

type TaskStatus string

const (
	// TaskStatusStarted means that the task is still running on some of the nodes.
	TaskStatusStarted TaskStatus = "STARTED"
	// TaskStatusFinished means that the task was successfully executed by all nodes.
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

type Task struct {
	// Type is the namespace of distributed tasks.
	Type string `json:"type"`

	// ID is the identifier of the task in the namespace of Type.
	ID string `json:"ID"`

	// Version is the version of the task with task ID.
	// It is used to differentiate between multiple runs of the same task.
	Version uint64 `json:"version"`

	// Payload is arbitrary data that is needed to execute a task of Type.
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

	// FinishedNodes is a map of nodeIDs that successfully finished the task.
	FinishedNodes map[string]struct{} `json:"finishedNodes"`

	handle TaskHandle
}

func (t *Task) Clone() *Task {
	clone := *t
	clone.FinishedNodes = maps.Clone(t.FinishedNodes)
	return &clone
}

type TaskHandle interface {
	Launch()
	Cancel()
}

type Provider interface {
	RegisterTaskNodeCompletionRecorder(recorder TaskNodeCompletionRecorder)

	// PrepareTask is meant for the provider to parse the payload and do any validation
	// seems necessary, however, the task should not do anything yet as it might not be started.
	PrepareTask(taskID string, taskVersion uint64, payload []byte) (TaskHandle, error)

	//RunningTasksIDs() []string // TODO: background rechecking
}

type Manager struct {
	mu        sync.Mutex
	tasks     map[string]map[string]*Task // taskID -> taskType -> Task
	providers map[string]Provider

	taskCompletionRecorder TaskNodeCompletionRecorder
}

type TaskNodeCompletionRecorder interface {
	RecordDistributedTaskNodeCompletion(ctx context.Context, taskType, taskID string, version uint64) error
}

func NewManager(taskCompletionRecorder TaskNodeCompletionRecorder) *Manager {
	return &Manager{
		tasks:     make(map[string]map[string]*Task),
		providers: make(map[string]Provider),

		taskCompletionRecorder: taskCompletionRecorder,
	}
}

func (m *Manager) RegisterProvider(name string, provider Provider) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.providers[name]; ok {
		return fmt.Errorf("provider %s already exists", name)
	}
	provider.RegisterTaskNodeCompletionRecorder(m.taskCompletionRecorder) // TODO: alternative idea is to change the TaskHandle to return completion notification channel

	m.providers[name] = provider
	return nil
}

// TODO: mention that this seqNum should be monotonically increasing
func (m *Manager) AddTask(c *api.ApplyRequest, seqNum uint64) error {
	var r api.AddDistributedTaskRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil { // TODO: refactor into a generic method
		return errors.Wrap(err, "unmarshal add task request")
	}

	provider, err := m.getProvider(r.Type)
	if err != nil {
		return err
	}

	taskHandle, err := provider.PrepareTask(r.Id, seqNum, r.Payload)
	if err != nil {
		return errors.Wrap(err, "prepare task")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.findTaskWithLock(r.Type, r.Id)
	if task != nil {
		if task.Status != TaskStatusStarted {
			return fmt.Errorf("task %s/%s is already running with version %d", r.Type, r.Id, task.Version) // TODO: unify error messages
		}

		// TODO: cancel the previous one?
	}

	// TODO: check if a task with given version already exists

	m.setTaskWithLock(&Task{
		Type:          r.Type,
		ID:            r.Id,
		Version:       seqNum,
		Payload:       r.Payload, // TODO: do we need this here?
		Status:        TaskStatusStarted,
		StartedAt:     time.UnixMilli(r.SubmittedAtUnixMillis),
		FinishedNodes: make(map[string]struct{}),

		handle: taskHandle,
	})

	taskHandle.Launch()

	return nil
}

func (m *Manager) RecordNodeCompletion(c *api.ApplyRequest, numberOfNodesInTheCluster int) error {
	var r api.RecordDistributedTaskNodeCompletionRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil {
		return errors.Wrap(err, "unmarshal record task node completion request")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Type, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return errors.Wrap(ErrTaskIsNoLongerRunning, "task is no longer running") // TODO: unify error messages
	}

	if r.Error != nil {
		task.Status = TaskStatusFailed
		task.Error = *r.Error
		task.FinishedNodes[r.Id] = struct{}{}

		// TODO: notify that task failed
		return nil
	}

	task.FinishedNodes[r.Id] = struct{}{}
	if len(task.FinishedNodes) == numberOfNodesInTheCluster {
		task.Status = TaskStatusFinished
		task.FinishedAt = time.UnixMilli(r.FinishedAtUnixMillis)

		// TODO: notify that task finished
		return nil
	}

	return nil
}

func (m *Manager) CancelTask(a *api.ApplyRequest) error {
	var r api.CancelDistributedTaskRequest
	if err := json.Unmarshal(a.SubCommand, &r); err != nil {
		return errors.Wrap(err, "unmarshal cancel task request")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Type, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return errors.Wrap(ErrTaskIsNoLongerRunning, "task is no longer running") // TODO: unify error messages
	}

	task.Status = TaskStatusCancelled
	task.FinishedAt = time.UnixMilli(r.CancelledAtUnixMillis)

	// TODO: notify that task was cancelled

	return nil
}

func (m *Manager) CleanUpTask(a *api.ApplyRequest) error {
	var r api.CleanUpDistributedTaskRequest
	if err := json.Unmarshal(a.SubCommand, &r); err != nil {
		return errors.Wrap(err, "unmarshal clean up task request")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task, err := m.findVersionedTaskWithLock(r.Type, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status == TaskStatusStarted {
		return errors.New("task is still running") // TODO: unify error messages
	}

	// TODO: constant
	// TODO: fake clock
	if time.Since(task.FinishedAt) < time.Hour*24 {
		return errors.New("task is too fresh to clean up")
	}

	delete(m.tasks[task.Type], task.ID)
	return nil
}

func (m *Manager) findVersionedTaskWithLock(taskType, taskID string, taskVersion uint64) (*Task, error) {
	task := m.findTaskWithLock(taskType, taskID)
	if task == nil {
		return nil, errors.Wrapf(ErrTaskDoesNotExist, "taskType=%s, taskID=%s", taskType, taskID)
	}

	if task.Version != taskVersion {
		return nil, errors.Wrapf(ErrTaskDoesNotExist, "taskType=%s, taskID=%s, existingVersion=%d, reqVersion=%d", taskType, taskID, task.Version, taskVersion)
	}

	return task, nil
}

func (m *Manager) findTaskWithLock(taskType, taskID string) *Task {
	tasksNamespace, ok := m.tasks[taskType]
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
	if _, ok := m.tasks[task.Type]; !ok {
		m.tasks[task.Type] = make(map[string]*Task)
	}

	m.tasks[task.Type][task.ID] = task
}

func (m *Manager) getProvider(name string) (Provider, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if provider, ok := m.providers[name]; ok {
		return provider, nil
	}
	return nil, fmt.Errorf("provider %s not found", name)
}
