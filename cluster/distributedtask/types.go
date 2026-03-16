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
	"time"
)

// SubUnitStatus represents the lifecycle state of an individual sub-unit within a task.
type SubUnitStatus string

const (
	// SubUnitStatusPending means the sub-unit has not yet been picked up by any node.
	SubUnitStatusPending SubUnitStatus = "PENDING"
	// SubUnitStatusInProgress means a node is currently executing this sub-unit.
	SubUnitStatusInProgress SubUnitStatus = "IN_PROGRESS"
	// SubUnitStatusCompleted means the sub-unit finished successfully.
	SubUnitStatusCompleted SubUnitStatus = "COMPLETED"
	// SubUnitStatusFailed means the sub-unit encountered a non-retryable error.
	SubUnitStatusFailed SubUnitStatus = "FAILED"
)

// SubUnit tracks the execution state of a single independent work item within a task.
// Sub-unit tracking is enabled by supplying SubUnitIds at task creation time.
type SubUnit struct {
	// ID is the identifier of this sub-unit within the task.
	ID string `json:"id"`

	// Status is the current lifecycle state of the sub-unit.
	Status SubUnitStatus `json:"status"`

	// Progress is a fraction in [0.0, 1.0] representing completion progress.
	// Updated by the Provider; subject to throttling in the Manager.
	Progress float64 `json:"progress,omitempty"`

	// NodeID is the ID of the node that last reported on this sub-unit.
	NodeID string `json:"nodeID,omitempty"`

	// Error contains the failure reason when Status == SubUnitStatusFailed.
	Error string `json:"error,omitempty"`

	// UpdatedAt is the time of the last state or progress update.
	UpdatedAt time.Time `json:"updatedAt"`
}

// SubUnitCompletionRecorder is an interface for recording sub-unit state transitions.
// Providers operating in sub-unit mode receive this recorder via SubUnitAwareProvider.SetSubUnitRecorder.
type SubUnitCompletionRecorder interface {
	RecordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, subUnitID string) error
	RecordDistributedTaskSubUnitFailure(ctx context.Context, namespace, taskID string, version uint64, subUnitID string, errMsg string) error
	RecordDistributedTaskSubUnitProgress(ctx context.Context, namespace, taskID string, version uint64, subUnitID string, progress float64) error
}

// TaskCompletedHandler is an optional interface that a Provider may implement to receive a
// callback when a task transitions from STARTED to FINISHED (i.e. all sub-units completed).
// The callback is invoked by the Scheduler on every node; implementations must be idempotent.
type TaskCompletedHandler interface {
	OnTaskCompleted(task *Task) error
}

// SubUnitAwareProvider is an optional interface that a Provider may implement to receive a
// SubUnitCompletionRecorder for reporting per-sub-unit state transitions and progress.
type SubUnitAwareProvider interface {
	SetSubUnitRecorder(recorder SubUnitCompletionRecorder)
}

// TasksLister is an interface for listing distributed tasks in the cluster.
type TasksLister interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*Task, error)
}

// TaskCleaner is an interface for issuing a request to clean up a distributed task.
type TaskCleaner interface {
	CleanUpDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
}

// TaskCompletionRecorder is an interface for recording the completion of a distributed task.
type TaskCompletionRecorder interface {
	RecordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64) error
	RecordDistributedTaskNodeFailure(ctx context.Context, namespace, taskID string, version uint64, errMsg string) error
}

// TaskHandle is an interface to control a locally running task.
type TaskHandle interface {
	// Terminate is a signal to stop executing the task. If the task is no longer running because it already finished,
	// the method call should be a no-op.
	//
	// Terminated task can be started later again, therefore, no local state can be removed.
	Terminate()
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
	// Used in node-completion mode (SubUnits == nil).
	FinishedNodes map[string]bool `json:"finishedNodes"`

	// SubUnits tracks per-sub-unit execution state.
	// When non-nil, the task uses sub-unit tracking mode instead of FinishedNodes.
	// The task transitions to FINISHED only when all sub-units reach COMPLETED.
	// Old snapshots without this field unmarshal to nil, retaining node-completion behaviour.
	SubUnits map[string]*SubUnit `json:"subUnits,omitempty"`
}

func (t *Task) Clone() *Task {
	clone := *t
	clone.FinishedNodes = maps.Clone(t.FinishedNodes)
	if t.SubUnits != nil {
		clone.SubUnits = make(map[string]*SubUnit, len(t.SubUnits))
		for id, su := range t.SubUnits {
			suCopy := *su
			clone.SubUnits[id] = &suCopy
		}
	}
	return &clone
}

type ListDistributedTasksResponse struct {
	Tasks map[string][]*Task `json:"tasks"`
}
