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

package backup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// awaitingTicks is the scheduler's SWAPPING callback budget
// (maxCompletedCallbackAttempts, 5) plus two, so a wait that consumed the
// budget would have failed the task.
//
// TODO: tracks distributedtask.maxCompletedCallbackAttempts by hand because it
// is unexported; raising that constant past 5 silently weakens these tests.
const awaitingTicks = 5 + 2

// completionScheduler runs a distributedtask scheduler over one backup task
// served unchanged on every listing, and counts the proposals it makes.
type completionScheduler struct {
	task      *distributedtask.Task
	listCalls atomic.Int32
	finalized atomic.Int32
	failed    atomic.Int32
}

func (c *completionScheduler) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	c.listCalls.Add(1)
	return map[string][]*distributedtask.Task{BackupTaskNamespace: {c.task.Clone()}}, nil
}

func (c *completionScheduler) MarkDistributedTaskFinalized(context.Context, string, string, uint64) error {
	c.finalized.Add(1)
	return nil
}

func (c *completionScheduler) MarkDistributedTaskFailed(context.Context, string, string, uint64, string) error {
	c.failed.Add(1)
	return nil
}

func (c *completionScheduler) CleanUpDistributedTask(context.Context, string, string, uint64) error {
	return nil
}

// startCompletionScheduler starts a scheduler on node over a SWAPPING task
// whose units are all COMPLETED.
func startCompletionScheduler(t *testing.T, node string, provider *BackupTaskProvider) *completionScheduler {
	t.Helper()
	task := makeTask("b1", distributedtask.TaskStatusSwapping, makePayload("b1"))
	for _, unitID := range []string{"node-1/Article", "node-1/Book", "node-2/Article"} {
		task.Units[unitID] = &distributedtask.Unit{ID: unitID, Status: distributedtask.UnitStatusCompleted}
	}
	c := &completionScheduler{task: task}
	logger, _ := test.NewNullLogger()
	s := distributedtask.NewScheduler(distributedtask.SchedulerParams{
		CompletionRecorder: &threadSafeRecorder{},
		TaskLister:         c,
		TaskCleaner:        c,
		TaskFinalizer:      c,
		Providers:          map[string]distributedtask.Provider{BackupTaskNamespace: provider},
		Logger:             logger,
		LocalNode:          node,
		CompletedTaskTTL:   time.Hour,
		TickInterval:       5 * time.Millisecond,
	})
	require.NoError(t, s.Start(context.Background()))
	t.Cleanup(s.Close)
	return c
}

// waitForTicks waits until the scheduler has listed tasks at least n times.
func (c *completionScheduler) waitForTicks(t *testing.T, n int32) {
	t.Helper()
	require.Eventually(t, func() bool { return c.listCalls.Load() >= n }, 10*time.Second, time.Millisecond)
}

// startedGlobalBackend serves a Started global descriptor until the test or
// a write changes it.
func startedGlobalBackend() *fakeBackend {
	be := newFakeBackend()
	be.glMeta = backup.DistributedBackupDescriptor{ID: "b1", Status: backup.Started}
	be.serveWrittenGlobalBackupMeta = true
	be.On("PutObject", mock.Anything, "b1", GlobalBackupFile, mock.Anything).Return(nil)
	return be
}

func TestBackupGlobalDescriptorSingleWriter(t *testing.T) {
	t.Run("a non-participant finalizes only after the global descriptor is final", func(t *testing.T) {
		be := startedGlobalBackend()
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-3",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
			IsLeader: func() bool { return false },
		})

		sched := startCompletionScheduler(t, "node-3", provider)
		sched.waitForTicks(t, awaitingTicks)
		assert.Zero(t, sched.finalized.Load())
		assert.Zero(t, sched.failed.Load())
		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)

		be.Lock()
		be.glMeta.Status = backup.Success
		be.Unlock()
		require.Eventually(t, func() bool { return sched.finalized.Load() > 0 }, 10*time.Second, time.Millisecond)
		assert.Zero(t, sched.failed.Load())
	})

	t.Run("a participant that is not payload.Leader writes no Started descriptor", func(t *testing.T) {
		be := newFakeBackend()
		recorder := &threadSafeRecorder{}
		provider := newFlowProvider(t, be, describingSourcer("Article", "Book"), recorder)
		payload := makePayload("b1")
		payload.Leader = "node-2"

		handle, err := provider.StartTask(makeTask("b1", distributedtask.TaskStatusStarted, payload))
		require.NoError(t, err)
		waitDone(t, handle)

		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
		assert.ElementsMatch(t, []string{"node-1/Article", "node-1/Book"}, recorder.getCompletions())
		assert.Empty(t, recorder.getFailures())
	})

	t.Run("a non-leader writes no terminal descriptor", func(t *testing.T) {
		be := startedGlobalBackend()
		// A leader would go on to read the node descriptors and write.
		be.On("GetObject", mock.Anything, mock.Anything, BackupFile).Return(nil, backup.ErrNotFound{})
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
			IsLeader: func() bool { return false },
		})

		err := provider.OnTaskCompleted(makeTask("b1", distributedtask.TaskStatusSwapping, makePayload("b1")))

		assert.ErrorIs(t, err, distributedtask.ErrTaskCompletionAwaiting)
		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
	})

	t.Run("a node that becomes leader writes the terminal descriptor", func(t *testing.T) {
		be := startedGlobalBackend()
		nodeSuccess := marshalDescriptor(t, backup.DistributedBackupDescriptor{ID: "b1", Status: backup.Success})
		be.On("GetObject", mock.Anything, "b1/node-1", BackupFile).Return(nodeSuccess, nil)
		be.On("GetObject", mock.Anything, "b1/node-2", BackupFile).Return(nodeSuccess, nil)
		var leader atomic.Bool
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-2",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
			IsLeader: leader.Load,
		})

		sched := startCompletionScheduler(t, "node-2", provider)
		sched.waitForTicks(t, awaitingTicks)
		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
		assert.Zero(t, sched.finalized.Load())
		assert.Zero(t, sched.failed.Load())

		leader.Store(true)
		require.Eventually(t, func() bool { return sched.finalized.Load() > 0 }, 10*time.Second, time.Millisecond)
		assert.Equal(t, backup.Success, be.globalMetaStatus())
		assert.Zero(t, sched.failed.Load())
	})
}
