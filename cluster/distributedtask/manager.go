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
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TODO: integrate into backups
// TODO: unify error lib
// TODO: move stuff to types
// TODO: add tests for raft code?

var (
	// TODO: think through which errors can be ignored and remove these random ones
	ErrTaskDoesNotExist      = errors.New("task does not exist")
	ErrTaskIsNoLongerRunning = errors.New("task is no longer running")
)

const (
	completedTaskTTL = time.Hour * 24 // TODO: config
)

type Manager struct {
	mu    sync.Mutex
	tasks map[string]map[string]*Task // namespace -> taskID -> Task

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

	task := m.findTaskWithLock(r.Namespace, r.Id)
	if task != nil {
		if task.Status == TaskStatusStarted {
			return fmt.Errorf("task %s/%s is already running with version %d", r.Namespace, r.Id, task.Version) // TODO: unify error messages
		}

		if seqNum <= task.Version {
			return fmt.Errorf("task %s/%s is already finished with version %d", r.Namespace, r.Id, task.Version)
		}
	}

	m.setTaskWithLock(&Task{
		Namespace:      r.Namespace,
		TaskDescriptor: TaskDescriptor{ID: r.Id, Version: seqNum},
		Payload:        r.Payload,
		Status:         TaskStatusStarted,
		StartedAt:      time.UnixMilli(r.SubmittedAtUnixMillis),
		FinishedNodes:  map[string]bool{},
	})

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Namespace,
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

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
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
			"taskNamespace": r.Namespace,
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
			"taskNamespace": r.Namespace,
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

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status != TaskStatusStarted {
		return errors.Wrap(ErrTaskIsNoLongerRunning, "task is no longer running") // TODO: unify error messages
	}

	task.Status = TaskStatusCancelled
	task.FinishedAt = time.UnixMilli(r.CancelledAtUnixMillis)

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Namespace,
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

	task, err := m.findVersionedTaskWithLock(r.Namespace, r.Id, r.Version)
	if err != nil {
		return err
	}

	if task.Status == TaskStatusStarted {
		return errors.New("task is still running") // TODO: unify error messages
	}

	if m.clock.Since(task.FinishedAt) <= completedTaskTTL {
		return errors.New("task is too fresh to clean up")
	}

	delete(m.tasks[task.Namespace], task.ID)

	m.logger.WithFields(logrus.Fields{
		"taskNamespace": r.Namespace,
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

func (m *Manager) GetTaskListPayload() ([]byte, error) {
	resp := ListDistributedTasksResponse{
		Tasks: m.ListTasks(),
	}
	return json.Marshal(&resp)
}

func (m *Manager) findVersionedTaskWithLock(namespace, taskID string, taskVersion uint64) (*Task, error) {
	task := m.findTaskWithLock(namespace, taskID)
	if task == nil {
		return nil, errors.Wrapf(ErrTaskDoesNotExist, "namespace=%s, taskID=%s", namespace, taskID)
	}

	if task.Version != taskVersion {
		return nil, errors.Wrapf(ErrTaskDoesNotExist, "namespace=%s, taskID=%s, existingVersion=%d, reqVersion=%d", namespace, taskID, task.Version, taskVersion)
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
