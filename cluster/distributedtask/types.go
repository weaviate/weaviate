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
	"context"
	"maps"
	"time"
)

type TasksLister interface {
	ListTasks() map[string][]*Task
}

// TODO: probably split this as it currently feels awkward
type TaskStatusChanger interface {
	RecordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64) error
	RecordDistributedTaskNodeFailed(ctx context.Context, namespace, taskID string, version uint64, errMsg string) error
	CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

type TaskHandle interface {
	// Optional: if it is still running it should terminate, if not should be no-op
	// It can clean up the local state after receiving the signal. The state will be cleaned up
	// by the provider during startup in case task was not able to.
	// TODO: scheduler should not fail on clean up.
	Terminate()
}

type Provider interface {
	GetLocalTasks() []TaskDescriptor
	CleanupTask(desc TaskDescriptor) error
	StartTask(task *Task) (TaskHandle, error)
	SetCompletionRecorder(recorder TaskStatusChanger)
}

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

// TaskDescriptor is a struct identifying a task execution under a certain task namespace.
type TaskDescriptor struct {
	// ID is the identifier of the task in the namespace.
	ID string `json:"ID"`

	// Version is the version of the task with task ID.
	// It is used to differentiate between multiple runs of the same task.
	Version uint64 `json:"version"`
}

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

	// FinishedNodes is a map of nodeIDs that successfully finished the task.
	FinishedNodes map[string]bool `json:"finishedNodes"`
}

func (t *Task) Clone() *Task {
	clone := *t
	clone.FinishedNodes = maps.Clone(t.FinishedNodes)
	return &clone
}

type ListDistributedTasksResponse struct {
	Tasks map[string][]*Task `json:"tasks"`
}
