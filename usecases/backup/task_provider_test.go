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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// --- test helpers ---

type noopAuthorizer struct{}

func (n *noopAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, _ ...string) error {
	return nil
}

func (n *noopAuthorizer) AuthorizeSilent(_ context.Context, _ *models.Principal, _ string, _ ...string) error {
	return nil
}

func (n *noopAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	return resources, nil
}

type threadSafeRecorder struct {
	mu          sync.Mutex
	completions []string // unitIDs
	failures    []string
	progresses  []progressEntry
}

type progressEntry struct {
	unitID   string
	progress float32
}

func (r *threadSafeRecorder) RecordDistributedTaskUnitCompletion(_ context.Context, _, _ string, _ uint64, _, unitID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completions = append(r.completions, unitID)
	return nil
}

func (r *threadSafeRecorder) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, unitID)
	return nil
}

func (r *threadSafeRecorder) RecordDistributedTaskRetryableUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, unitID)
	return nil
}

func (r *threadSafeRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, _, _ string, _ uint64, _, unitID string, progress float32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progresses = append(r.progresses, progressEntry{unitID, progress})
	return nil
}

func (r *threadSafeRecorder) getProgresses() []progressEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]progressEntry(nil), r.progresses...)
}

func (r *threadSafeRecorder) getFailures() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.failures...)
}

// nodeHandlerWithLatch builds the node-side handler whose `lastOp` latch the
// legacy 2PC path and the DTM provider share.
func nodeHandlerWithLatch(t *testing.T, node string, sourcer Sourcer, backends BackupBackendProvider) *Handler {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return &Handler{
		node:      node,
		logger:    logger,
		backends:  backends,
		backupper: newBackupper(node, logger, config.Backup{}, sourcer, nil, nil, backends),
	}
}

func makeTask(id string, status distributedtask.TaskStatus, payload *taskPayload) *distributedtask.Task {
	data, _ := json.Marshal(payload)
	return &distributedtask.Task{
		Namespace:      BackupTaskNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 42},
		Payload:        data,
		Status:         status,
		StartedAt:      time.Now().UTC(),
		Units:          map[string]*distributedtask.Unit{},
	}
}

func makePayload(id string) *taskPayload {
	return &taskPayload{
		ID:      id,
		Backend: "s3",
		Nodes: map[string]*backup.NodeDescriptor{
			"node-1": {Classes: []string{"Article", "Book"}},
			"node-2": {Classes: []string{"Article"}},
		},
		Leader:          "node-1",
		Classes:         []string{"Article", "Book"},
		ServerVersion:   "1.30.0",
		CompressionType: backup.CompressionGZIP,
	}
}

func TestBackupTaskProvider(t *testing.T) {
	t.Run("StartTask defers while applied index lags", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:   "node-1",
			Logger: logger,
			Cfg:    config.Backup{},
			AppliedIndexProbe: func(ctx context.Context, version uint64) error {
				return fmt.Errorf("not caught up")
			},
		})
		provider.SetCompletionRecorder(&threadSafeRecorder{})

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		_, err := provider.StartTask(task)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not caught up")
	})

	t.Run("idle handle for no-local-group nodes", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:   "node-3",
			Logger: logger,
			Cfg:    config.Backup{},
		})
		provider.SetCompletionRecorder(&threadSafeRecorder{})

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		handle, err := provider.StartTask(task)
		require.NoError(t, err)
		require.NotNil(t, handle)
		select {
		case <-handle.Done():
			t.Fatal("idle handle should not be done yet")
		default:
		}
		handle.Terminate()
		select {
		case <-handle.Done():
		case <-time.After(time.Second):
			t.Fatal("idle handle should be done after Terminate")
		}
	})

	t.Run("same-ID StartTask re-attaches to the existing handle", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		recorder := &threadSafeRecorder{}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", mock.Anything, mock.Anything).Return(nil)
		sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(func() <-chan backup.ClassDescriptor {
				ch := make(chan backup.ClassDescriptor)
				return ch
			}())
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		be := newFakeBackend()
		be.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/test")
		be.On("SourceDataPath").Return("/data")
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
		be.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Sourcer:  sourcer,
			Backends: &fakeBackupBackendProvider{backend: be},
		})
		provider.SetCompletionRecorder(recorder)

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		h1, err := provider.StartTask(task)
		require.NoError(t, err)

		h2, err := provider.StartTask(task)
		require.NoError(t, err)
		assert.Equal(t, h1, h2, "same-ID StartTask must re-attach")

		h1.Terminate()
	})

	t.Run("a different id in the node latch fails the local units", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		recorder := &threadSafeRecorder{}
		be := newFakeBackend()
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/test")

		handler := nodeHandlerWithLatch(t, "node-1", &fakeSourcer{}, &fakeBackupBackendProvider{backend: be})
		require.Empty(t, handler.backupper.lastOp.renew("legacy-backup", "/test", "", ""))

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:        "node-1",
			Logger:      logger,
			Cfg:         config.Backup{},
			Backends:    &fakeBackupBackendProvider{backend: be},
			NodeHandler: handler,
		})
		provider.SetCompletionRecorder(recorder)

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		handle, err := provider.StartTask(task)
		require.Error(t, err)
		assert.Nil(t, handle)
		assert.Contains(t, err.Error(), "legacy-backup")
		assert.ElementsMatch(t, []string{"node-1/Article", "node-1/Book"}, recorder.getFailures(),
			"every local unit must fail cleanly")
		assert.Equal(t, "legacy-backup", handler.backupper.lastOp.get().ID,
			"the refused start must not steal the latch")
	})

	t.Run("the same id in the node latch re-attaches without a second flow", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		recorder := &threadSafeRecorder{}
		be := newFakeBackend()
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/test")

		handler := nodeHandlerWithLatch(t, "node-1", &fakeSourcer{}, &fakeBackupBackendProvider{backend: be})
		require.Empty(t, handler.backupper.lastOp.renew("b1", "/test", "", ""))

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:        "node-1",
			Logger:      logger,
			Cfg:         config.Backup{},
			Backends:    &fakeBackupBackendProvider{backend: be},
			NodeHandler: handler,
		})
		provider.SetCompletionRecorder(recorder)

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		handle, err := provider.StartTask(task)
		require.NoError(t, err)
		assert.IsType(t, &idleTaskHandle{}, handle)
		assert.Empty(t, recorder.getProgresses(), "re-attach must not claim units again")
		assert.Empty(t, recorder.getFailures())
	})

	t.Run("the node latch is released when the flow exits", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		blockCh := make(chan backup.ClassDescriptor)
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", mock.Anything, mock.Anything).Return(nil)
		sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return((<-chan backup.ClassDescriptor)(blockCh))
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		be := newFakeBackend()
		be.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/test")
		be.On("SourceDataPath").Return("/data")
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
		be.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		handler := nodeHandlerWithLatch(t, "node-1", sourcer, &fakeBackupBackendProvider{backend: be})
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:        "node-1",
			Logger:      logger,
			Cfg:         config.Backup{},
			Sourcer:     sourcer,
			Backends:    &fakeBackupBackendProvider{backend: be},
			NodeHandler: handler,
		})
		provider.SetCompletionRecorder(&threadSafeRecorder{})

		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		handle, err := provider.StartTask(task)
		require.NoError(t, err)
		assert.Equal(t, "b1", handler.backupper.lastOp.get().ID, "the flow must hold the latch")

		handle.Terminate()
		select {
		case <-handle.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("handle.Done must close after Terminate")
		}
		assert.Empty(t, handler.backupper.lastOp.get().ID, "the latch must be free once the flow exits")
	})

	t.Run("the Started descriptor is written at flow start", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
		be.On("PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything).Return(nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		payload := makePayload("b1")
		task := makeTask("b1", distributedtask.TaskStatusStarted, payload)
		require.NoError(t, provider.writeStartedDescriptor(context.Background(), task, payload))

		be.AssertCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
		assert.Equal(t, backup.Started, be.glMeta.Status)
		assert.Equal(t, "b1", be.glMeta.ID)
		assert.Equal(t, Version, be.glMeta.Version, "artifact structure version comes from the writer's build")
		assert.Equal(t, "node-1", be.glMeta.Leader, "leader comes from the payload, not the writing node")
		assert.Equal(t, payload.ServerVersion, be.glMeta.ServerVersion)
	})

	t.Run("an existing descriptor is never overwritten at flow start", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		existing, _ := json.Marshal(backup.DistributedBackupDescriptor{ID: "b1", Status: backup.Success})
		be.On("GetObject", mock.Anything, mock.Anything, GlobalBackupFile).Return(existing, nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		payload := makePayload("b1")
		task := makeTask("b1", distributedtask.TaskStatusStarted, payload)
		require.NoError(t, provider.writeStartedDescriptor(context.Background(), task, payload))
		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
	})

	t.Run("bootstrap CleanupTask on a still-active task releases local state only", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		sourcer := &fakeSourcer{}
		sourcer.On("ReleaseBackup", context.Background(), "b1", "Article").Return(nil)
		sourcer.On("ReleaseBackup", context.Background(), "b1", "Book").Return(nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:    "node-1",
			Logger:  logger,
			Cfg:     config.Backup{},
			Sourcer: sourcer,
		})

		payload := makePayload("b1")
		payloadBytes, _ := json.Marshal(payload)
		provider.payloadCache["b1"] = payloadBytes

		desc := distributedtask.TaskDescriptor{ID: "b1", Version: 42}
		err := provider.CleanupTask(desc)
		require.NoError(t, err)
		sourcer.AssertCalled(t, "ReleaseBackup", context.Background(), "b1", "Article")
		sourcer.AssertCalled(t, "ReleaseBackup", context.Background(), "b1", "Book")
		_, ok := provider.payloadCache["b1"]
		assert.False(t, ok)
	})

	t.Run("GetLocalTasks reports cached descriptors for bootstrap", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:   "node-1",
			Logger: logger,
			Cfg:    config.Backup{},
		})
		assert.Nil(t, provider.GetLocalTasks(), "empty cache returns nil")

		provider.payloadCache["bak-1"] = []byte(`{}`)
		provider.payloadCache["bak-2"] = []byte(`{}`)

		descs := provider.GetLocalTasks()
		require.Len(t, descs, 2)
		ids := map[string]bool{}
		for _, d := range descs {
			ids[d.ID] = true
		}
		assert.True(t, ids["bak-1"])
		assert.True(t, ids["bak-2"])
	})

	t.Run("keepalive writes progress to prevent stale detection", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		recorder := &threadSafeRecorder{}

		ctx, cancel := context.WithCancel(context.Background())
		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		classes := []string{"Article", "Book"}

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:   "node-1",
			Logger: logger,
			Cfg:    config.Backup{},
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			provider.runKeepalive(ctx, task, classes, recorder)
		}()

		time.Sleep(keepaliveInterval + time.Second)
		cancel()
		<-done

		progs := recorder.getProgresses()
		require.NotEmpty(t, progs, "keepalive must write at least one progress update")
		for _, p := range progs {
			assert.Equal(t, float32(0), p.progress, "keepalive re-reports progress=0")
		}
	})

	t.Run("retained until descriptor terminal", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()

		startedDesc := backup.DistributedBackupDescriptor{
			ID: "b1", Status: backup.Started,
		}
		startedBytes, _ := json.Marshal(startedDesc)
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(startedBytes, nil).Once()

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
		assert.True(t, provider.ShouldRetainCompletedTask(task, nil), "must retain while descriptor is non-terminal")

		termDesc := backup.DistributedBackupDescriptor{
			ID: "b1", Status: backup.Success,
		}
		termBytes, _ := json.Marshal(termDesc)
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(termBytes, nil)

		assert.False(t, provider.ShouldRetainCompletedTask(task, nil), "must release once descriptor is terminal")
	})
}

func TestBackupConflictDetector(t *testing.T) {
	logger, _ := test.NewNullLogger()
	provider := NewBackupTaskProvider(BackupTaskProviderParams{
		Node:   "node-1",
		Logger: logger,
		Cfg:    config.Backup{},
	})

	makeExisting := func(id string, status distributedtask.TaskStatus) *distributedtask.Task {
		return makeTask(id, status, makePayload(id))
	}

	newPayloadBytes := func(id string) []byte {
		p := makePayload(id)
		data, _ := json.Marshal(p)
		return data
	}

	t.Run("same-ID rejection per status", func(t *testing.T) {
		for _, status := range []distributedtask.TaskStatus{
			distributedtask.TaskStatusStarted,
			distributedtask.TaskStatusSwapping,
			distributedtask.TaskStatusFailed,
			distributedtask.TaskStatusCancelled,
		} {
			t.Run(string(status), func(t *testing.T) {
				err := provider.CheckConflict(newPayloadBytes("bak-1"), []*distributedtask.Task{
					makeExisting("bak-1", status),
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), "bak-1")
			})
		}
	})

	t.Run("one-backup-at-a-time", func(t *testing.T) {
		err := provider.CheckConflict(newPayloadBytes("bak-2"), []*distributedtask.Task{
			makeExisting("bak-other", distributedtask.TaskStatusStarted),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already in progress")
	})

	t.Run("terminal tasks do not block new backup", func(t *testing.T) {
		err := provider.CheckConflict(newPayloadBytes("bak-3"), []*distributedtask.Task{
			makeExisting("bak-old", distributedtask.TaskStatusFinished),
		})
		assert.NoError(t, err)
	})

	t.Run("reindex exclusion via cross-namespace", func(t *testing.T) {
		allTasks := map[string]map[string]*distributedtask.Task{
			"reindex": {
				"rx-1": {
					Namespace:      "reindex",
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "rx-1"},
					Status:         distributedtask.TaskStatusStarted,
				},
			},
		}
		err := provider.CheckCrossNamespaceConflict(newPayloadBytes("bak-4"), allTasks)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reindex")
	})

	t.Run("terminal cross-namespace tasks do not block", func(t *testing.T) {
		allTasks := map[string]map[string]*distributedtask.Task{
			"reindex": {
				"rx-1": {
					Namespace:      "reindex",
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "rx-1"},
					Status:         distributedtask.TaskStatusFinished,
				},
			},
		}
		err := provider.CheckCrossNamespaceConflict(newPayloadBytes("bak-5"), allTasks)
		assert.NoError(t, err)
	})
}

func TestBackupTerminalDescriptor(t *testing.T) {
	t.Run("verdict function", func(t *testing.T) {
		t.Run("CANCELLED task yields CANCELED descriptor", func(t *testing.T) {
			task := makeTask("b1", distributedtask.TaskStatusCancelled, makePayload("b1"))
			st, _ := backupVerdict(task)
			assert.Equal(t, backup.Cancelled, st)
		})

		t.Run("FAILED task yields FAILED descriptor", func(t *testing.T) {
			task := makeTask("b1", distributedtask.TaskStatusFailed, makePayload("b1"))
			task.Error = "something broke"
			st, errMsg := backupVerdict(task)
			assert.Equal(t, backup.Failed, st)
			assert.Contains(t, errMsg, "something broke")
		})

		t.Run("FINISHED task with all units succeeded yields SUCCESS", func(t *testing.T) {
			task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
			task.Units["node-1/Article"] = &distributedtask.Unit{Status: distributedtask.UnitStatusCompleted}
			task.Units["node-1/Book"] = &distributedtask.Unit{Status: distributedtask.UnitStatusCompleted}
			task.PostCompletionAcks = map[string]distributedtask.PostCompletionAck{
				"node-1": {Success: true},
			}
			st, errMsg := backupVerdict(task)
			assert.Equal(t, backup.Success, st)
			assert.Empty(t, errMsg)
		})

		t.Run("any failed unit yields FAILED", func(t *testing.T) {
			task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
			task.Units["node-1/Article"] = &distributedtask.Unit{Status: distributedtask.UnitStatusCompleted}
			task.Units["node-1/Book"] = &distributedtask.Unit{
				Status: distributedtask.UnitStatusFailed,
				Error:  "upload timeout",
			}
			st, errMsg := backupVerdict(task)
			assert.Equal(t, backup.Failed, st)
			assert.Contains(t, errMsg, "upload timeout")
		})

		t.Run("failed ack yields FAILED", func(t *testing.T) {
			task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
			task.Units["node-1/Article"] = &distributedtask.Unit{Status: distributedtask.UnitStatusCompleted}
			task.PostCompletionAcks = map[string]distributedtask.PostCompletionAck{
				"node-1": {Success: false, Error: "ack error"},
			}
			st, errMsg := backupVerdict(task)
			assert.Equal(t, backup.Failed, st)
			assert.Contains(t, errMsg, "ack error")
		})
	})

	t.Run("missing node descriptor on failure fills from task record", func(t *testing.T) {
		nd := &backup.NodeDescriptor{Classes: []string{"Article", "Book"}}
		task := makeTask("b1", distributedtask.TaskStatusFailed, makePayload("b1"))
		task.Units["node-1/Article"] = &distributedtask.Unit{
			Status: distributedtask.UnitStatusFailed,
			Error:  "upload timeout",
		}
		task.Units["node-1/Book"] = &distributedtask.Unit{
			Status: distributedtask.UnitStatusFailed,
			Error:  "connection reset",
		}

		fillNodeFromTaskRecord(nd, "node-1", task, backup.NewErrNotFound(fmt.Errorf("not found")))

		assert.Equal(t, backup.Failed, nd.Status, "all units failed => node status FAILED")
		assert.Contains(t, nd.Error, "per task record:")
		assert.Contains(t, nd.Error, "upload timeout")
		assert.Contains(t, nd.Error, "connection reset")
		assert.Zero(t, nd.PreCompressionSizeBytes, "sizes stay absent")
	})

	t.Run("non-not-found read error annotates without synthesizing status", func(t *testing.T) {
		nd := &backup.NodeDescriptor{Classes: []string{"Article"}}
		task := makeTask("b1", distributedtask.TaskStatusFailed, makePayload("b1"))
		task.Units["node-1/Article"] = &distributedtask.Unit{
			Status: distributedtask.UnitStatusFailed,
			Error:  "upload timeout",
		}

		transientErr := fmt.Errorf("connection refused")
		fillNodeFromTaskRecord(nd, "node-1", task, transientErr)

		assert.Empty(t, nd.Status, "status must not be synthesized for a transient read error")
		assert.Contains(t, nd.Error, "node descriptor unreadable")
		assert.Contains(t, nd.Error, "connection refused")
		assert.NotContains(t, nd.Error, "per task record",
			"the record's unit data must not appear for a non-not-found error")
	})
}

func TestBackupStatusMapping(t *testing.T) {
	t.Run("STARTED with no claimed unit maps to STARTED", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		task.Units["node-1/Article"] = &distributedtask.Unit{Status: distributedtask.UnitStatusPending}
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Started, st)
	})

	t.Run("STARTED with in-progress unit maps to TRANSFERRING", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))
		task.Units["node-1/Article"] = &distributedtask.Unit{Status: distributedtask.UnitStatusInProgress}
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Transferring, st)
	})

	t.Run("SWAPPING maps to TRANSFERRED", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusSwapping, makePayload("b1"))
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Transferred, st)
	})

	t.Run("FINISHED maps to SUCCESS", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Success, st)
	})

	t.Run("FAILED maps to FAILED", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusFailed, makePayload("b1"))
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Failed, st)
	})

	t.Run("CANCELLED maps to CANCELED", func(t *testing.T) {
		task := makeTask("b1", distributedtask.TaskStatusCancelled, makePayload("b1"))
		st, _ := dtmStatusToBackup(task)
		assert.Equal(t, backup.Cancelled, st)
	})

	t.Run("terminal task with retained record serves Size from descriptor", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		descr := backup.DistributedBackupDescriptor{
			ID:                      "b1",
			Status:                  backup.Success,
			PreCompressionSizeBytes: 42 * 1024 * 1024 * 1024,
			CompletedAt:             time.Now().UTC(),
			BaseBackupID:            "base-1",
		}
		descrBytes, _ := json.Marshal(descr)
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(descrBytes, nil)
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
		st := provider.dtmTaskToStatus(context.Background(), task)

		assert.Equal(t, backup.Success, st.Status)
		expectedSize := float64(42*1024*1024*1024) / (1024 * 1024 * 1024)
		assert.InDelta(t, expectedSize, st.Size, 0.001, "Size must come from the descriptor")
		assert.Equal(t, "base-1", st.BaseBackupID)
	})

	t.Run("descriptor unreadable falls back to task record", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, backup.NewErrNotFound(fmt.Errorf("not found")))
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		task := makeTask("b1", distributedtask.TaskStatusFinished, makePayload("b1"))
		st := provider.dtmTaskToStatus(context.Background(), task)

		assert.Equal(t, backup.Success, st.Status)
		assert.Zero(t, st.Size, "Size must be zero when descriptor is unreadable")
	})

	t.Run("nil-nil miss dispatches to the legacy path", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		descr := backup.DistributedBackupDescriptor{
			ID: "b1", Status: backup.Success,
		}
		descrBytes, _ := json.Marshal(descr)
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(descrBytes, nil)
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")

		dtm := &fakeDTMClient{getTask: nil, getError: nil}
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		s := &Scheduler{
			logger:       logger,
			authorizer:   &noopAuthorizer{},
			backends:     &fakeBackupBackendProvider{backend: be},
			dtm:          dtm,
			taskProvider: provider,
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}),
		}

		st, err := s.BackupStatus(context.Background(), nil, "fakeBackend", "b1", "", "")
		require.NoError(t, err)
		assert.Equal(t, backup.Success, st.Status)
	})

	t.Run("accessor error falls back to the legacy path, never treated as a miss", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		descr := backup.DistributedBackupDescriptor{
			ID: "b1", Status: backup.Success,
		}
		descrBytes, _ := json.Marshal(descr)
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(descrBytes, nil)
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")

		dtmErr := fmt.Errorf("simulated DTM accessor error")
		dtm := &fakeDTMClient{getTask: nil, getError: dtmErr}
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})

		s := &Scheduler{
			logger:       logger,
			authorizer:   &noopAuthorizer{},
			backends:     &fakeBackupBackendProvider{backend: be},
			dtm:          dtm,
			taskProvider: provider,
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}),
		}

		st, err := s.BackupStatus(context.Background(), nil, "fakeBackend", "b1", "", "")
		require.NoError(t, err, "BackupStatus must not error on DTM accessor failure")
		assert.Equal(t, backup.Success, st.Status, "must fall back to legacy descriptor")
	})
}

// dtmProposeFixture drives Scheduler.Backup over a fake DTM client with the
// gate on.
type dtmProposeFixture struct {
	scheduler *Scheduler
	dtm       *fakeDTMClient
	req       *BackupRequest
}

func newDTMProposeFixture(t *testing.T) *dtmProposeFixture {
	t.Helper()
	const (
		cls      = "Class-A"
		node     = "Node-A"
		backupID = "bak-1"
	)
	ctx := context.Background()

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.selector.On("ListClasses", ctx).Return([]string{cls})
	fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
	fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("dst/path")
	fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)

	dtm := &fakeDTMClient{}
	s := fs.scheduler()
	s.SetDTMClient(dtm, config.Backup{DistributedTasksEnabled: true}, nil)

	return &dtmProposeFixture{
		scheduler: s,
		dtm:       dtm,
		req:       &BackupRequest{ID: backupID, Backend: "gcs", Include: []string{cls}},
	}
}

func TestBackupProposeRetry(t *testing.T) {
	// One clean propose first, to capture the exact bytes the propose path
	// marshals for this request. Every subtest compares against these.
	fixture := newDTMProposeFixture(t)
	_, err := fixture.scheduler.Backup(context.Background(), nil, fixture.req)
	require.NoError(t, err)
	proposedPayload := fixture.dtm.proposedPayload
	require.NotEmpty(t, proposedPayload)

	rejections := map[string]error{
		"ErrTaskAlreadyRunning": distributedtask.ErrTaskAlreadyRunning,
		"ErrTaskConflict":       distributedtask.ErrTaskConflict,
	}

	t.Run("an equal payload reports the first attempt's success", func(t *testing.T) {
		for _, status := range []distributedtask.TaskStatus{
			distributedtask.TaskStatusStarted,
			distributedtask.TaskStatusSwapping,
			distributedtask.TaskStatusFailed,
			distributedtask.TaskStatusCancelled,
		} {
			for name, rejection := range rejections {
				t.Run(string(status)+"/"+name, func(t *testing.T) {
					f := newDTMProposeFixture(t)
					f.dtm.proposeErr = rejection
					f.dtm.getTask = &distributedtask.Task{
						Namespace:      BackupTaskNamespace,
						TaskDescriptor: distributedtask.TaskDescriptor{ID: f.req.ID, Version: 7},
						Payload:        proposedPayload,
						Status:         status,
					}

					resp, err := f.scheduler.Backup(context.Background(), nil, f.req)
					require.NoError(t, err)
					require.NotNil(t, resp)
					assert.Equal(t, f.req.ID, resp.ID)
				})
			}
		}
	})

	t.Run("a different payload is refused as an id conflict", func(t *testing.T) {
		f := newDTMProposeFixture(t)
		other := makePayload(f.req.ID)
		other.Backend = "s3"
		otherBytes, err := marshalTaskPayload(other)
		require.NoError(t, err)

		f.dtm.proposeErr = distributedtask.ErrTaskAlreadyRunning
		f.dtm.getTask = &distributedtask.Task{
			Namespace:      BackupTaskNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: f.req.ID},
			Payload:        otherBytes,
			Status:         distributedtask.TaskStatusStarted,
		}

		resp, err := f.scheduler.Backup(context.Background(), nil, f.req)
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "already in use")
	})

	t.Run("a nil-nil miss passes the conflict through", func(t *testing.T) {
		f := newDTMProposeFixture(t)
		f.dtm.proposeErr = distributedtask.ErrTaskConflict
		f.dtm.getTask = nil

		resp, err := f.scheduler.Backup(context.Background(), nil, f.req)
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		// ErrUnprocessable does not unwrap, so the conflict reason travels
		// in the message.
		assert.Contains(t, err.Error(), distributedtask.ErrTaskConflict.Error(),
			"the conflict reason must survive")
	})

	t.Run("an accessor error is surfaced, never treated as a miss", func(t *testing.T) {
		f := newDTMProposeFixture(t)
		accessorErr := errors.New("leader unreachable")
		f.dtm.proposeErr = distributedtask.ErrTaskAlreadyRunning
		f.dtm.getError = accessorErr

		resp, err := f.scheduler.Backup(context.Background(), nil, f.req)
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.ErrorIs(t, err, accessorErr)
		var unproc backup.ErrUnprocessable
		assert.False(t, errors.As(err, &unproc), "an accessor error is a failure, not a 4xx conflict")
	})
}

func TestBackupRetryClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection reset by the object store", fmt.Errorf("put chunk: %w", syscall.ECONNRESET), true},
		{"connection refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"broken pipe mid-upload", fmt.Errorf("upload: %w", syscall.EPIPE), true},
		{"truncated response body", fmt.Errorf("read meta: %w", io.ErrUnexpectedEOF), true},
		{"backend client deadline", fmt.Errorf("put object: %w", context.DeadlineExceeded), true},
		{"operator cancel", fmt.Errorf("upload: %w", context.Canceled), false},
		{"access denied", errors.New("AccessDenied: not authorized"), false},
		{"class no longer exists", errors.New("class Article not found"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isRetryableUploadError(tc.err))
		})
	}
}

// TestBackupStatusAuthorization covers the window while a DTM backup is in
// flight, when the global descriptor the legacy authorize reads does not
// exist yet.
func TestBackupStatusAuthorization(t *testing.T) {
	newScheduler := func(t *testing.T, authorizer authorization.Authorizer) *Scheduler {
		t.Helper()
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		// No descriptor yet: the descriptor-scoped authorize is a no-op here.
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")
		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})
		return &Scheduler{
			logger:       logger,
			authorizer:   authorizer,
			backends:     &fakeBackupBackendProvider{backend: be},
			dtm:          &fakeDTMClient{getTask: makeTask("b1", distributedtask.TaskStatusStarted, makePayload("b1"))},
			taskProvider: provider,
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}),
		}
	}

	t.Run("an unpermitted principal cannot read an in-flight backup", func(t *testing.T) {
		denier := &recordingAuthorizer{err: errors.New("forbidden")}
		s := newScheduler(t, denier)

		st, err := s.BackupStatus(context.Background(), nil, "fakeBackend", "b1", "", "")
		require.Error(t, err)
		assert.Nil(t, st)
		assert.Contains(t, denier.resources, authorization.Backups("Article")[0],
			"the read must be scoped to the record's classes")
	})

	t.Run("a permitted principal is served from the task record", func(t *testing.T) {
		s := newScheduler(t, &recordingAuthorizer{})

		st, err := s.BackupStatus(context.Background(), nil, "fakeBackend", "b1", "", "")
		require.NoError(t, err)
		assert.Equal(t, backup.Started, st.Status)
	})
}

// recordingAuthorizer answers every check with err and records the resources it
// was asked about.
type recordingAuthorizer struct {
	err       error
	resources []string
}

func (a *recordingAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
	a.resources = append(a.resources, resources...)
	return a.err
}

func (a *recordingAuthorizer) AuthorizeSilent(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
	return a.err
}

func (a *recordingAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	if a.err != nil {
		return nil, a.err
	}
	return resources, nil
}

func TestBackupGateDispatch(t *testing.T) {
	t.Run("force=true refused while the backup flag is off", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		s := &Scheduler{
			logger:    logger,
			backupCfg: config.Backup{DistributedTasksEnabled: false},
		}
		task := makeTask("b1", distributedtask.TaskStatusSwapping, makePayload("b1"))
		err := s.cancelDTMBackup(context.Background(), task, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "flag is off")
	})

	t.Run("force refusal from the FSM maps ErrForceTerminateRefused to a 4xx", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		dtm := &fakeDTMClient{
			forceTermErr: distributedtask.ErrForceTerminateRefused,
		}
		s := &Scheduler{
			logger:    logger,
			dtm:       dtm,
			backupCfg: config.Backup{DistributedTasksEnabled: true},
		}
		task := makeTask("b1", distributedtask.TaskStatusSwapping, makePayload("b1"))
		err := s.cancelDTMBackup(context.Background(), task, true)
		require.Error(t, err)
		var unproc backup.ErrUnprocessable
		assert.True(t, errors.As(err, &unproc), "expected ErrUnprocessable, got: %T", err)
	})

	t.Run("cancel surfaces a DTM accessor error instead of falling back", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		be.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
		be.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("/backups/b1")

		dtmErr := fmt.Errorf("simulated DTM accessor error")
		dtm := &fakeDTMClient{getTask: nil, getError: dtmErr}
		s := &Scheduler{
			logger:     logger,
			authorizer: &noopAuthorizer{},
			backends:   &fakeBackupBackendProvider{backend: be},
			dtm:        dtm,
			backupper:  newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}),
		}

		cancelErr := s.CancelWithForce(context.Background(), nil, "fakeBackend", "b1", "", "", false)
		require.Error(t, cancelErr, "Cancel must surface the DTM accessor error")
		assert.Contains(t, cancelErr.Error(), "DTM query failed")
	})
}

func TestCleanupUnfinishedBackups(t *testing.T) {
	t.Run("readiness timeout aborts the sweep", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		dtm := &fakeDTMClient{
			ready:          false,
			waitUntilDBErr: fmt.Errorf("timeout"),
		}
		s := &Scheduler{
			logger:   logger,
			dtm:      dtm,
			backends: &fakeBackupBackendProvider{backend: newFakeBackend()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		s.CleanupUnfinishedBackups(ctx)
	})

	t.Run("a DTM query error skips the id", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		dtm := &fakeDTMClient{
			ready:    true,
			getError: fmt.Errorf("dtm down"),
		}
		be := newFakeBackend()
		be.On("AllBackups", context.Background()).Return([]*backup.DistributedBackupDescriptor{
			{ID: "b1", Status: backup.Started},
		}, nil)
		s := &Scheduler{
			logger:   logger,
			dtm:      dtm,
			backends: &fakeBackupBackendProvider{backend: be},
		}
		ctx := context.Background()
		s.CleanupUnfinishedBackups(ctx)
		be.AssertNotCalled(t, "PutObject")
	})

	t.Run("the readiness wait gets its full budget from the caller", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		var observed time.Duration
		dtm := &fakeDTMClient{
			ready: true,
			onWait: func(ctx context.Context) {
				if deadline, ok := ctx.Deadline(); ok {
					observed = time.Until(deadline)
				}
			},
		}
		be := newFakeBackend()
		be.On("AllBackups", mock.Anything).Return([]*backup.DistributedBackupDescriptor{}, nil)
		s := &Scheduler{
			logger:   logger,
			dtm:      dtm,
			backends: &fakeBackupBackendProvider{backend: be},
		}

		ctx, cancel := context.WithTimeout(context.Background(), CleanupSweepTimeout)
		defer cancel()
		s.CleanupUnfinishedBackups(ctx)

		assert.InDelta(t, sweepReadinessTimeout.Seconds(), observed.Seconds(), 5,
			"a caller budget shorter than the readiness wait aborts the sweep on every startup")
	})
}

type fakeDTMClient struct {
	ready          bool
	waitUntilDBErr error
	getTask        *distributedtask.Task
	getError       error
	cancelErr      error
	forceTermErr   error
	proposeErr     error

	// proposedPayload is the last payload the propose path marshaled.
	proposedPayload []byte
	// onWait observes the context the sweep gives the readiness wait.
	onWait func(ctx context.Context)
}

func (f *fakeDTMClient) GetDistributedTask(_ context.Context, _, _ string) (*distributedtask.Task, error) {
	return f.getTask, f.getError
}

func (f *fakeDTMClient) CancelDistributedTask(_ context.Context, _, _ string, _ uint64) error {
	return f.cancelErr
}

func (f *fakeDTMClient) ForceTerminateDistributedTask(_ context.Context, _, _ string, _ uint64, _, _ string) error {
	return f.forceTermErr
}

func (f *fakeDTMClient) ProposeBackupTask(_ context.Context, _ string, payload []byte, _ []distributedtask.UnitSpec, _ int64, _ int32) error {
	f.proposedPayload = payload
	return f.proposeErr
}

func (f *fakeDTMClient) Ready() bool {
	return f.ready
}

func (f *fakeDTMClient) WaitUntilDBRestored(ctx context.Context, _ time.Duration, _ chan struct{}) error {
	if f.onWait != nil {
		f.onWait(ctx)
	}
	return f.waitUntilDBErr
}
