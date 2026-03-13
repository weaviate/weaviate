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
	"time"
)

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
	RecordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string) error
	RecordDistributedTaskSubUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID, errMsg string) error
	UpdateDistributedTaskSubUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string, progress float32) error
}

// TaskHandle is an interface to control a locally running task.
type TaskHandle interface {
	// Terminate is a signal to stop executing the task. If the task is no longer running because it already finished,
	// the method call should be a no-op.
	//
	// Terminated task can be started later again, therefore, no local state can be removed.
	Terminate()

	// Done returns a channel that is closed when the task's goroutine exits, whether due to
	// completion, failure, or termination. The scheduler uses this to detect dead handles
	// and allow re-launch of tasks that still have pending work.
	Done() <-chan struct{}
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

// SubUnitAwareProvider fires per-group callbacks as groups complete (mid-flight),
// then a global OnTaskCompleted when the task reaches terminal state.
//
// Every sub-unit task has groups. If no explicit GroupID is set, all sub-units
// belong to a single implicit default group (""). This means:
//   - Tasks without groups: OnGroupCompleted fires once with all local sub-units
//     when all sub-units reach terminal state (same effect as having a single group).
//   - Tasks with groups: OnGroupCompleted fires per-group as each completes,
//     even while the task is still STARTED
//
// Callback phases:
//  1. OnGroupCompleted — per group, fires as each group's sub-units all reach terminal
//  2. OnTaskCompleted — fires once on every node after ALL sub-units terminal
type SubUnitAwareProvider interface {
	Provider
	OnGroupCompleted(task *Task, groupID string, localGroupSubUnitIDs []string)
	OnTaskCompleted(task *Task)
}

type SubUnitStatus string

const (
	SubUnitStatusPending    SubUnitStatus = "PENDING"
	SubUnitStatusInProgress SubUnitStatus = "IN_PROGRESS"
	SubUnitStatusCompleted  SubUnitStatus = "COMPLETED"
	SubUnitStatusFailed     SubUnitStatus = "FAILED"
)

// SubUnit represents a trackable work unit within a distributed task (e.g. a single shard
// in a reindex operation). Sub-units follow the lifecycle PENDING → IN_PROGRESS → COMPLETED/FAILED.
//
// NodeID starts empty (unassigned) and is set on the first progress update. The [Scheduler]
// treats unassigned sub-units as belonging to any node, which is how initial assignment happens:
// the first node to report progress claims the sub-unit.
//
// SubUnit values are owned by the [Manager] and mutated under its lock. Callers outside the
// Manager should only access sub-units via cloned [Task] snapshots from ListDistributedTasks.
type SubUnit struct {
	ID         string        `json:"id"`
	GroupID    string        `json:"groupId,omitempty"`
	NodeID     string        `json:"nodeId"`
	Status     SubUnitStatus `json:"status"`
	Progress   float32       `json:"progress"`
	Error      string        `json:"error,omitempty"`
	UpdatedAt  time.Time     `json:"updatedAt"`
	FinishedAt time.Time     `json:"finishedAt,omitempty"`
}

// SubUnitSpec defines a sub-unit with an optional group assignment. Used at task creation
// time when sub-units need group membership (e.g. one group per tenant for MT reindex).
type SubUnitSpec struct {
	ID      string
	GroupID string
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

// Task represents a distributed task tracked across the cluster via Raft consensus.
//
// Completion is tracked per-sub-unit. The task finishes when all sub-units reach a terminal
// state. A single sub-unit failure immediately fails the entire task — remaining in-flight
// sub-units are NOT waited for.
//
// Sub-units are always required when creating a task.
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

	// SubUnits tracks per-sub-unit progress. Always non-nil for valid tasks.
	SubUnits map[string]*SubUnit `json:"subUnits,omitempty"`
}

func (t *Task) Clone() *Task {
	clone := *t
	if t.SubUnits != nil {
		clone.SubUnits = make(map[string]*SubUnit, len(t.SubUnits))
		for k, v := range t.SubUnits {
			subCopy := *v
			clone.SubUnits[k] = &subCopy
		}
	}
	return &clone
}

// AllSubUnitsTerminal returns true if all sub-units are in a terminal state (COMPLETED or FAILED).
func (t *Task) AllSubUnitsTerminal() bool {
	for _, su := range t.SubUnits {
		if su.Status != SubUnitStatusCompleted && su.Status != SubUnitStatusFailed {
			return false
		}
	}
	return true
}

// AnySubUnitFailed returns true if any sub-unit has FAILED status.
func (t *Task) AnySubUnitFailed() bool {
	for _, su := range t.SubUnits {
		if su.Status == SubUnitStatusFailed {
			return true
		}
	}
	return false
}

// LocalSubUnitIDs returns the IDs of sub-units assigned to the given node.
func (t *Task) LocalSubUnitIDs(nodeID string) []string {
	var ids []string
	for id, su := range t.SubUnits {
		if su.NodeID == nodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// Groups returns the distinct GroupIDs across all sub-units (includes "" for ungrouped).
func (t *Task) Groups() []string {
	seen := map[string]bool{}
	for _, su := range t.SubUnits {
		seen[su.GroupID] = true
	}
	groups := make([]string, 0, len(seen))
	for g := range seen {
		groups = append(groups, g)
	}
	return groups
}

// AllGroupSubUnitsTerminal returns true if all sub-units in the given group are terminal.
func (t *Task) AllGroupSubUnitsTerminal(groupID string) bool {
	for _, su := range t.SubUnits {
		if su.GroupID != groupID {
			continue
		}
		if su.Status != SubUnitStatusCompleted && su.Status != SubUnitStatusFailed {
			return false
		}
	}
	return true
}

// LocalGroupSubUnitIDs returns the IDs of sub-units in the given group assigned to the given node.
func (t *Task) LocalGroupSubUnitIDs(groupID, nodeID string) []string {
	var ids []string
	for id, su := range t.SubUnits {
		if su.GroupID == groupID && su.NodeID == nodeID {
			ids = append(ids, id)
		}
	}
	return ids
}

// NodeHasNonTerminalSubUnits returns true if the given node has sub-units that are not yet terminal.
// Unassigned sub-units (empty NodeID) are considered as belonging to any node.
func (t *Task) NodeHasNonTerminalSubUnits(nodeID string) bool {
	for _, su := range t.SubUnits {
		if su.Status == SubUnitStatusCompleted || su.Status == SubUnitStatusFailed {
			continue
		}
		if su.NodeID == "" || su.NodeID == nodeID {
			return true
		}
	}
	return false
}

type ListDistributedTasksResponse struct {
	Tasks map[string][]*Task `json:"tasks"`
}
