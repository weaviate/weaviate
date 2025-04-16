//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package distributedtask

import (
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TODO: unify error lib
// TODO: check what happens in our raft implementation if we return an error
// TODO: integrate into backups

var (
	// TODO: think through which errors can be ignored and remove these random ones
	ErrTaskDoesNotExist      = errors.New("task does not exist")
	ErrTaskIsNoLongerRunning = errors.New("task is no longer running")
)

const (
	completedTaskTTL = time.Hour * 24 // TODO: config
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

// TaskDescriptor is a struct identifying a task execution under a certain task type.
type TaskDescriptor struct {
	// ID is the identifier of the task in the namespace of Type.
	ID string `json:"ID"`

	// Version is the version of the task with task ID.
	// It is used to differentiate between multiple runs of the same task.
	Version uint64 `json:"version"`
}

type Task struct {
	// Type is the namespace of distributed tasks.
	Type string `json:"type"`

	TaskDescriptor `json:",inline"`

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
	FinishedNodes map[string]bool `json:"finishedNodes"`
}

func (t *Task) Clone() *Task {
	clone := *t
	clone.FinishedNodes = maps.Clone(t.FinishedNodes)
	return &clone
}

type Manager struct {
	mu    sync.Mutex
	tasks map[string]map[string]*Task // taskID -> taskType -> Task

	logger logrus.FieldLogger
	clock  clockwork.Clock
}

func NewManager(clock clockwork.Clock, logger logrus.FieldLogger) *Manager {
	return &Manager{
		tasks: make(map[string]map[string]*Task),

		logger: logger,
		clock:  clock,
	}
}

// TODO: mention that this seqNum should be monotonically increasing
func (m *Manager) AddTask(c *api.ApplyRequest, seqNum uint64) error {
	var r api.AddDistributedTaskRequest
	if err := json.Unmarshal(c.SubCommand, &r); err != nil { // TODO: refactor into a generic method
		return errors.Wrap(err, "unmarshal add task request")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.findTaskWithLock(r.Type, r.Id)
	if task != nil {
		if task.Status == TaskStatusStarted {
			return fmt.Errorf("task %s/%s is already running with version %d", r.Type, r.Id, task.Version) // TODO: unify error messages
		}

		if seqNum <= task.Version {
			return fmt.Errorf("task %s/%s is already finished with version %d", r.Type, r.Id, task.Version)
		}
	}

	m.setTaskWithLock(&Task{
		Type:           r.Type,
		TaskDescriptor: TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:        r.Payload,
		Status:         TaskStatusStarted,
		StartedAt:      time.UnixMilli(r.SubmittedAtUnixMillis),
		FinishedNodes:  map[string]bool{},
	})

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Type,
		"taskID":        r.Id,
		"taskVersion":   seqNum,
	}).Info("added task to the cluster")

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
		task.FinishedAt = time.UnixMilli(r.FinishedAtUnixMillis)

		m.logger.WithFields(logrus.Fields{
			"taskNamespace": r.Type,
			"taskID":        r.Id,
			"taskVersion":   task.Version,
			"error":         *r.Error,
		}).Info("task failed")
		return nil
	}

	task.FinishedNodes[r.NodeId] = true
	if len(task.FinishedNodes) == numberOfNodesInTheCluster {
		task.Status = TaskStatusFinished
		task.FinishedAt = time.UnixMilli(r.FinishedAtUnixMillis)

		m.logger.WithFields(logrus.Fields{
			"taskNamespace": r.Type,
			"taskID":        r.Id,
			"taskVersion":   task.Version,
		}).Info("task completed")
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

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Type,
		"taskID":        r.Id,
		"taskVersion":   task.Version,
	}).Info("task cancelled")

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

	if m.clock.Since(task.FinishedAt) <= completedTaskTTL {
		return errors.New("task is too fresh to clean up")
	}

	delete(m.tasks[task.Type], task.ID)

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Type,
		"taskID":        r.Id,
		"taskVersion":   task.Version,
		"startedAt":     task.StartedAt.String(),
	}).Info("task cleaned up")

	return nil
}

func (m *Manager) ListTasks() map[string][]*Task {
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

	return result
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
