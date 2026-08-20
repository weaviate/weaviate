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
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestForceTerminateTask(t *testing.T) {
	const (
		ns      = "test-ns"
		taskID  = "task-1"
		nodeID  = "node-1"
		unitID1 = "u1"
		unitID2 = "u2"
	)

	addTask := func(t *testing.T, m *Manager, seqNum uint64) {
		t.Helper()
		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             ns,
			Id:                    taskID,
			UnitIds:               []string{unitID1, unitID2},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), seqNum)
		require.NoError(t, err)
	}

	forceTerminate := func(t *testing.T, m *Manager, version uint64, status string) error {
		t.Helper()
		return m.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace:               ns,
			Id:                      taskID,
			Version:                 version,
			Reason:                  "test reason",
			RequestedTerminalStatus: status,
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
	}

	newManager := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		return NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})
	}

	t.Run("accepted from STARTED", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)
		err := forceTerminate(t, m, 1, string(TaskStatusCancelled))
		require.NoError(t, err)

		task := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusCancelled, task.Status)
		assert.Contains(t, task.Error, "test reason")
	})

	t.Run("accepted from PREPARING", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		m.mu.Lock()
		task := m.findTaskWithLock(ns, taskID)
		task.NeedsPreparationBarrier = true
		for _, u := range task.Units {
			u.Status = UnitStatusCompleted
		}
		task.Status = TaskStatusPreparing
		m.mu.Unlock()

		err := forceTerminate(t, m, 1, string(TaskStatusFailed))
		require.NoError(t, err)

		got := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusFailed, got.Status)
	})

	t.Run("accepted from SWAPPING with missing acks", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		m.mu.Lock()
		task := m.findTaskWithLock(ns, taskID)
		for _, u := range task.Units {
			u.Status = UnitStatusCompleted
			u.NodeID = nodeID
		}
		task.Status = TaskStatusSwapping
		m.mu.Unlock()

		err := forceTerminate(t, m, 1, string(TaskStatusCancelled))
		require.NoError(t, err)

		got := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusCancelled, got.Status)
	})

	t.Run("refused when SWAPPING with all acks landed", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		m.mu.Lock()
		task := m.findTaskWithLock(ns, taskID)
		for _, u := range task.Units {
			u.Status = UnitStatusCompleted
			u.NodeID = nodeID
		}
		task.Status = TaskStatusSwapping
		task.PostCompletionAcks = map[string]PostCompletionAck{
			nodeID: {Success: true},
		}
		m.mu.Unlock()

		err := forceTerminate(t, m, 1, string(TaskStatusCancelled))
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrForceTerminateRefused))
		assert.True(t, errors.Is(err, ErrPermanentRejection))

		got := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusSwapping, got.Status)
	})

	t.Run("refused for unrecognized status", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		m.mu.Lock()
		task := m.findTaskWithLock(ns, taskID)
		task.Status = TaskStatus("FUTURE_STATUS")
		m.mu.Unlock()

		err := forceTerminate(t, m, 1, string(TaskStatusFailed))
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrForceTerminateRefused))
	})

	t.Run("already-terminal proposal is dropped", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		err := forceTerminate(t, m, 1, string(TaskStatusCancelled))
		require.NoError(t, err)

		err = forceTerminate(t, m, 1, string(TaskStatusFailed))
		require.NoError(t, err)

		got := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusCancelled, got.Status)
	})

	t.Run("reason and requested status recorded in Task.Error", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		err := forceTerminate(t, m, 1, string(TaskStatusFailed))
		require.NoError(t, err)

		got := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusFailed, got.Status)
		assert.Contains(t, got.Error, "test reason")
	})

	t.Run("ErrForceTerminateRefused survives gRPC rehydration", func(t *testing.T) {
		m := newManager(t)
		addTask(t, m, 1)

		m.mu.Lock()
		task := m.findTaskWithLock(ns, taskID)
		task.Status = TaskStatus("FUTURE_STATUS")
		m.mu.Unlock()

		refusalErr := forceTerminate(t, m, 1, string(TaskStatusFailed))
		require.Error(t, refusalErr)

		rpcErr := ToRPCError(refusalErr)
		require.NotNil(t, rpcErr)
		rehydrated := RehydratePermanentRejection(rpcErr)
		assert.True(t, errors.Is(rehydrated, ErrForceTerminateRefused))
		assert.True(t, errors.Is(rehydrated, ErrPermanentRejection))
	})
}

func TestRecordUnitCompletionRetry(t *testing.T) {
	const (
		ns     = "test-ns"
		taskID = "task-1"
		nodeID = "node-1"
		unitID = "u1"
	)

	newManager := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		return NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})
	}

	addTaskWithRetries := func(t *testing.T, m *Manager, maxRetries int32) {
		t.Helper()
		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             ns,
			Id:                    taskID,
			UnitIds:               []string{unitID},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			MaxUnitRetries:        maxRetries,
		}), 1)
		require.NoError(t, err)

		err = m.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace:           ns,
			Id:                  taskID,
			Version:             1,
			NodeId:              nodeID,
			UnitId:              unitID,
			Progress:            0.0,
			UpdatedAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)
	}

	recordRetryableFailure := func(t *testing.T, m *Manager) error {
		t.Helper()
		return m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            ns,
			Id:                   taskID,
			Version:              1,
			NodeId:               nodeID,
			UnitId:               unitID,
			Error:                "transient error",
			FinishedAtUnixMillis: time.Now().UnixMilli(),
			Retryable:            true,
		}))
	}

	t.Run("retryable failure below budget re-opens the unit on the same node", func(t *testing.T) {
		m := newManager(t)
		addTaskWithRetries(t, m, 3)

		err := recordRetryableFailure(t, m)
		require.NoError(t, err)

		task := m.GetDistributedTask(context.Background(), ns, taskID)
		require.Equal(t, TaskStatusStarted, task.Status)
		u := task.Units[unitID]
		assert.Equal(t, UnitStatusPending, u.Status)
		assert.Equal(t, int32(1), u.Attempts)
		assert.Equal(t, nodeID, u.NodeID)
	})

	t.Run("exhausted budget fails the task", func(t *testing.T) {
		m := newManager(t)
		addTaskWithRetries(t, m, 1)

		err := recordRetryableFailure(t, m)
		require.NoError(t, err)
		task := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusStarted, task.Status)

		// Re-claim after re-open.
		err = m.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: ns, Id: taskID, Version: 1,
			NodeId: nodeID, UnitId: unitID,
			Progress: 0.0, UpdatedAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		err = recordRetryableFailure(t, m)
		require.NoError(t, err)
		task = m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusFailed, task.Status)
	})

	t.Run("zero-value fields keep fail-fast", func(t *testing.T) {
		m := newManager(t)
		addTaskWithRetries(t, m, 0)

		err := recordRetryableFailure(t, m)
		require.NoError(t, err)

		task := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusFailed, task.Status)
	})

	t.Run("re-open refused once the task is terminal", func(t *testing.T) {
		m := newManager(t)
		addTaskWithRetries(t, m, 3)

		err := m.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: ns, Id: taskID, Version: 1,
			Reason:                  "test",
			RequestedTerminalStatus: string(TaskStatusCancelled),
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		err = recordRetryableFailure(t, m)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTaskNotRunning))
	})
}

func TestTaskAlreadyRunningSentinel(t *testing.T) {
	const (
		ns     = "test-ns"
		taskID = "task-1"
	)

	newManager := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		return NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})
	}

	t.Run("typed sentinel for STARTED same-ID re-add", func(t *testing.T) {
		m := newManager(t)

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u2"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 2)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTaskAlreadyRunning))
		assert.True(t, errors.Is(err, ErrPermanentRejection))
		// Legacy phrase preserved for old substring classifiers.
		assert.Contains(t, err.Error(), "is already running")
	})

	t.Run("typed sentinel survives gRPC rehydration", func(t *testing.T) {
		m := newManager(t)

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		addErr := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u2"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 2)
		require.Error(t, addErr)

		// Simulate the gRPC round-trip: ToRPCError, then RehydratePermanentRejection.
		rpcErr := ToRPCError(addErr)
		require.NotNil(t, rpcErr)
		rehydrated := RehydratePermanentRejection(rpcErr)
		assert.True(t, errors.Is(rehydrated, ErrTaskAlreadyRunning))
		assert.True(t, errors.Is(rehydrated, ErrPermanentRejection))
	})

	t.Run("non-STARTED same-ID re-add still replaces", func(t *testing.T) {
		m := newManager(t)

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = m.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: ns, Id: taskID, Version: 1,
			Reason:                  "done",
			RequestedTerminalStatus: string(TaskStatusFailed),
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// Re-add with higher seqNum replaces the terminal record.
		err = m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns, Id: taskID,
			UnitIds:               []string{"u2"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 2)
		require.NoError(t, err)

		task := m.GetDistributedTask(context.Background(), ns, taskID)
		assert.Equal(t, TaskStatusStarted, task.Status)
		assert.Equal(t, uint64(2), task.Version)
	})
}

func TestCrossNamespaceConflict(t *testing.T) {
	const (
		ns1    = "ns1"
		ns2    = "ns2"
		taskID = "task-1"
	)

	newManager := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		return NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})
	}

	t.Run("full map visibility for CrossNamespaceConflictDetector", func(t *testing.T) {
		m := newManager(t)

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns1, Id: "existing",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		detector := &fakeCrossNamespaceDetector{
			checkCross: func(_ []byte, allTasks map[string]map[string]*Task) error {
				for _, t := range allTasks[ns1] {
					if t.Status.IsActive() {
						return errors.New("ns1 has active task")
					}
				}
				return nil
			},
		}
		m.SetConflictDetectors(map[string]ConflictDetector{ns2: detector})

		err = m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns2, Id: taskID,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 2)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrTaskConflict))
	})

	t.Run("same-namespace-only for non-cross detector", func(t *testing.T) {
		m := newManager(t)

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns1, Id: "existing",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		detector := &fakeConflictDetector{rejectWith: nil}
		m.SetConflictDetectors(map[string]ConflictDetector{ns2: detector})

		err = m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: ns2, Id: taskID,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 2)
		require.NoError(t, err)
		assert.Equal(t, 1, detector.called)
	})
}

// fakeCrossNamespaceDetector implements both ConflictDetector and
// CrossNamespaceConflictDetector.
type fakeCrossNamespaceDetector struct {
	fakeConflictDetector
	checkCross func([]byte, map[string]map[string]*Task) error
}

func (f *fakeCrossNamespaceDetector) CheckCrossNamespaceConflict(payload []byte, allTasks map[string]map[string]*Task) error {
	if f.checkCross != nil {
		return f.checkCross(payload, allTasks)
	}
	return nil
}

func TestTaskWireCompat(t *testing.T) {
	t.Run("zero-value new fields reproduce legacy behavior", func(t *testing.T) {
		// An old binary dropping the new fields degrades to legacy
		// semantics, never to new ones.
		task := &Task{
			Namespace:      "test",
			TaskDescriptor: TaskDescriptor{ID: "t1", Version: 1},
			Status:         TaskStatusStarted,
			Units: map[string]*Unit{
				"u1": {ID: "u1", Status: UnitStatusPending},
			},
		}
		assert.Equal(t, int32(0), task.FormatVersion)
		assert.Equal(t, int64(0), task.StaleTimeoutMs)
		assert.Equal(t, int32(0), task.MaxUnitRetries)
		assert.Equal(t, int32(0), task.Units["u1"].Attempts)

		data, err := json.Marshal(task)
		require.NoError(t, err)

		var roundTripped Task
		require.NoError(t, json.Unmarshal(data, &roundTripped))
		assert.Equal(t, int32(0), roundTripped.FormatVersion)
		assert.Equal(t, int64(0), roundTripped.StaleTimeoutMs)
		assert.Equal(t, int32(0), roundTripped.MaxUnitRetries)
	})

	t.Run("FormatVersion stamped by AddTask", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		m := NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		err := m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "t1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		task := m.GetDistributedTask(context.Background(), "test", "t1")
		assert.Equal(t, int32(1), task.FormatVersion)
	})

	t.Run("old binary dropping new fields degrades to legacy", func(t *testing.T) {
		task := &Task{
			Namespace:      "test",
			TaskDescriptor: TaskDescriptor{ID: "t1", Version: 1},
			Status:         TaskStatusStarted,
			FormatVersion:  1,
			StaleTimeoutMs: 420000,
			MaxUnitRetries: 3,
			Units: map[string]*Unit{
				"u1": {ID: "u1", Status: UnitStatusPending, Attempts: 2},
			},
		}
		data, err := json.Marshal(task)
		require.NoError(t, err)

		// Simulate old binary: unmarshal into a struct without the new fields.
		type oldUnit struct {
			ID     string     `json:"id"`
			Status UnitStatus `json:"status"`
		}
		type oldTask struct {
			Namespace               string `json:"namespace"`
			TaskDescriptor          `json:",inline"`
			Payload                 []byte              `json:"payload"`
			NeedsPreparationBarrier bool                `json:"needsPreparationBarrier"`
			Status                  TaskStatus          `json:"status"`
			StartedAt               time.Time           `json:"startedAt"`
			FinishedAt              time.Time           `json:"finishedAt"`
			Error                   string              `json:"error,omitempty"`
			Units                   map[string]*oldUnit `json:"units,omitempty"`
		}

		var old oldTask
		require.NoError(t, json.Unmarshal(data, &old))
		assert.Equal(t, TaskStatusStarted, old.Status)
	})
}

func TestGetDistributedTaskAccessor(t *testing.T) {
	newMgr := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		return NewManager(ManagerParameters{
			Clock:            clockwork.NewFakeClock(),
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})
	}

	getReq := func(t *testing.T, ns, id string) []byte {
		t.Helper()
		b, err := json.Marshal(struct {
			Namespace string `json:"namespace"`
			ID        string `json:"id"`
		}{Namespace: ns, ID: id})
		require.NoError(t, err)
		return b
	}

	t.Run("not-found through real Manager path returns nil nil", func(t *testing.T) {
		mgr := newMgr(t)

		_, fsmErr := mgr.GetDistributedTaskPayload(context.Background(), getReq(t, "ns", "nonexistent"))
		require.Error(t, fsmErr)

		// In-process path: the %w chain preserves the sentinel.
		assert.True(t, errors.Is(fsmErr, ErrTaskDoesNotExist))

		// Rehydration path (same call Raft.GetDistributedTask makes).
		rehydrated := RehydratePermanentRejection(fsmErr)
		assert.True(t, errors.Is(rehydrated, ErrTaskDoesNotExist))
	})

	t.Run("not-found from gRPC-collapsed error rehydrates correctly", func(t *testing.T) {
		mgr := newMgr(t)

		_, fsmErr := mgr.GetDistributedTaskPayload(context.Background(), getReq(t, "ns", "nonexistent"))
		require.Error(t, fsmErr)

		// Simulate gRPC round-trip: ToRPCError flattens the chain,
		// RehydratePermanentRejection restores it from the marker.
		rpcErr := ToRPCError(fsmErr)
		require.NotNil(t, rpcErr)
		rehydrated := RehydratePermanentRejection(rpcErr)
		assert.True(t, errors.Is(rehydrated, ErrTaskDoesNotExist))
		assert.True(t, errors.Is(rehydrated, ErrPermanentRejection))
	})

	t.Run("present task returns acks and byte-exact payload", func(t *testing.T) {
		mgr := newMgr(t)

		storedPayload := []byte(`{"backend":"s3","bucket":"my-bucket"}`)
		err := mgr.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			Payload:               storedPayload,
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = mgr.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			NodeId: "node-1", UnitId: "u1",
			Progress: 0.0, UpdatedAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		err = mgr.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			NodeId: "node-1", UnitId: "u1",
			FinishedAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		err = mgr.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			NodeId: "node-1", Success: true,
			AckedAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		respBytes, err := mgr.GetDistributedTaskPayload(context.Background(), getReq(t, "ns", "task-1"))
		require.NoError(t, err)

		var resp GetDistributedTaskResponse
		require.NoError(t, json.Unmarshal(respBytes, &resp))
		require.NotNil(t, resp.Task)

		// Payload is byte-identical to what was stored. PR 2's
		// duplicate-submit comparison depends on byte fidelity.
		assert.Equal(t, storedPayload, resp.Task.Payload,
			"payload bytes must round-trip exactly")

		require.Contains(t, resp.Task.PostCompletionAcks, "node-1")
		assert.True(t, resp.Task.PostCompletionAcks["node-1"].Success)
	})

	t.Run("an error is never a miss", func(t *testing.T) {
		// A non-ErrTaskDoesNotExist error must surface as (nil, err),
		// never as (nil, nil). PR 2's status dispatch falls back to
		// legacy on error and must never confuse an outage with
		// task-absence.
		mgr := newMgr(t)

		// Feed malformed JSON to trigger an unmarshal error (not
		// ErrTaskDoesNotExist).
		_, fsmErr := mgr.GetDistributedTaskPayload(context.Background(), []byte(`{invalid`))
		require.Error(t, fsmErr)
		assert.False(t, errors.Is(fsmErr, ErrTaskDoesNotExist),
			"the error must NOT be classifiable as task-not-found")

		// The accessor's rehydration path must leave this error intact
		// (not silently convert it to nil).
		rehydrated := RehydratePermanentRejection(fsmErr)
		require.Error(t, rehydrated)
		assert.False(t, errors.Is(rehydrated, ErrTaskDoesNotExist))
	})
}
