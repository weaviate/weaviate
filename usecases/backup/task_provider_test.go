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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

// --- test helpers ---

type noopAuthorizer struct{}

func (n *noopAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, _ ...string) error {
	return nil
}

func (n *noopAuthorizer) AuthorizeAndRequireActiveNamespace(_ context.Context, _ *models.Principal, _ string, _ string, _ ...string) error {
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
	// failures holds every failed unit; retryableFailures holds the subset
	// reported as retryable.
	failures          []string
	retryableFailures []string
	failureMessages   []string
	progresses        []progressEntry
	// claimTask, when set, has each reported unit pinned to the reporting node,
	// as the FSM does on a claim.
	claimTask *distributedtask.Task
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

func (r *threadSafeRecorder) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, unitID)
	r.failureMessages = append(r.failureMessages, errMsg)
	return nil
}

func (r *threadSafeRecorder) RecordDistributedTaskRetryableUnitFailure(_ context.Context, _, _ string, _ uint64, _, unitID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, unitID)
	r.retryableFailures = append(r.retryableFailures, unitID)
	r.failureMessages = append(r.failureMessages, errMsg)
	return nil
}

func (r *threadSafeRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, _, _ string, _ uint64, nodeID, unitID string, progress float32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progresses = append(r.progresses, progressEntry{unitID, progress})
	if r.claimTask != nil {
		if u := r.claimTask.Units[unitID]; u != nil {
			u.NodeID = nodeID
		}
	}
	return nil
}

func (r *threadSafeRecorder) getCompletions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.completions...)
}

func (r *threadSafeRecorder) getFailureMessages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.failureMessages...)
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

func (r *threadSafeRecorder) getRetryableFailures() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.retryableFailures...)
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

func TestClassCompletionTracker(t *testing.T) {
	t.Run("the completion-order final class waits for flush", func(t *testing.T) {
		tracker := newClassCompletionTracker(2)
		var recorded []string
		record := func(className string) bool {
			recorded = append(recorded, className)
			return true
		}

		tracker.uploaded("Book", record)
		tracker.uploaded("Article", record)
		assert.Equal(t, []string{"Book"}, recorded)

		tracker.flush([]string{"Article", "Book"}, record)
		assert.Equal(t, []string{"Book", "Article"}, recorded)
	})

	t.Run("a failed early report is retried after flush", func(t *testing.T) {
		tracker := newClassCompletionTracker(2)
		attempts := map[string]int{}
		record := func(className string) bool {
			attempts[className]++
			return className != "Book" || attempts[className] > 1
		}

		tracker.uploaded("Book", record)
		tracker.uploaded("Article", record)
		tracker.flush([]string{"Article", "Book"}, record)

		assert.Equal(t, 2, attempts["Book"])
		assert.Equal(t, 1, attempts["Article"])
	})
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
		sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
		require.Empty(t, handler.backupper.lastOp.renew("legacy-backup", "", "/test", "", ""))

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
		require.Empty(t, handler.backupper.lastOp.renew("b1", "", "/test", "", ""))

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
		// The uploader drains this channel until it closes; DB.BackupDescriptors
		// closes it once its ctx ends.
		sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return((<-chan backup.ClassDescriptor)(blockCh)).
			Run(func(a mock.Arguments) {
				context.AfterFunc(a.Get(0).(context.Context), func() { close(blockCh) })
			})
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
		require.NoError(t, provider.writeStartedDescriptor(context.Background(), task, payload, nil))

		be.AssertCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
		assert.Equal(t, backup.Started, be.glMeta.Status)
		assert.Equal(t, "b1", be.glMeta.ID)
		assert.Equal(t, Version, be.glMeta.Version, "artifact structure version comes from the writer's build")
		assert.Equal(t, "node-1", be.glMeta.Leader, "leader comes from the payload, not the writing node")
		assert.Equal(t, payload.ServerVersion, be.glMeta.ServerVersion)
	})

	t.Run("the Started descriptor preserves selection and base-chain format", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		be.On("GetObject", mock.Anything, mock.Anything, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		// coordStore.Meta probes the legacy per-node descriptor when the global one is missing.
		be.On("GetObject", mock.Anything, mock.Anything, BackupFile).Return(nil, backup.ErrNotFound{})
		be.On("PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything).Return(nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})
		payload := makePayload("b1")
		payload.SkipUsers = true
		payload.SkipRoles = true
		payload.BaseChainDeduped = true

		require.NoError(t, provider.writeStartedDescriptor(
			context.Background(), makeTask("b1", distributedtask.TaskStatusStarted, payload), payload, nil,
		))
		assert.True(t, be.glMeta.SkipUsers)
		assert.True(t, be.glMeta.SkipRoles)
		assert.False(t, be.glMeta.DedupeReplicas)
		assert.Equal(t, VersionDedupeReplicas, be.glMeta.Version)
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
		require.NoError(t, provider.writeStartedDescriptor(context.Background(), task, payload, nil))
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

// dedupeFlow runs one node's DTM flow for a dedupe backup over fakes. Article
// has two shards replicated on node-1 and node-2; Book lives on node-1 only.
type dedupeFlow struct {
	node     string
	payload  *taskPayload
	provider *BackupTaskProvider
	backend  *fakeBackend
	sourcer  *fakeSourcer
	planner  *fakeDedupePlanner
	recorder *threadSafeRecorder
	hook     *test.Hook
	// claimedBy presets NodeID on the local units, as a restarted flow sees it.
	claimedBy string

	mu sync.Mutex
	// uploads records each BackupDescriptors call, the start of every upload.
	uploads []dedupeUpload
}

type dedupeUpload struct {
	designations map[string]map[string]string
	// global is the global descriptor as written when the upload started.
	global backup.DistributedBackupDescriptor
}

func newDedupeFlow(t *testing.T, node string) *dedupeFlow {
	t.Helper()
	fx := &dedupeFlow{
		node:     node,
		payload:  makePayload("b1"),
		backend:  newFakeBackend(),
		sourcer:  &fakeSourcer{},
		planner:  &fakeDedupePlanner{},
		recorder: &threadSafeRecorder{},
	}
	fx.payload.DedupeReplicas = true

	fx.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fx.backend.On("SourceDataPath").Return("/data")
	// coordStore.Meta probes the legacy per-node descriptor when the global one is missing.
	fx.backend.On("GetObject", mock.Anything, "b1", BackupFile).Return(nil, backup.ErrNotFound{})
	fx.backend.On("PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	classes := fx.payload.Nodes[node].Classes
	described := make(chan backup.ClassDescriptor, len(classes))
	for _, cls := range classes {
		described <- backup.ClassDescriptor{Name: cls}
	}
	close(described)
	fx.sourcer.On("BackupDescriptors", mock.Anything, "b1", classes, mock.Anything, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(described)).
		Run(func(a mock.Arguments) {
			fx.backend.RLock()
			global := fx.backend.glMeta
			fx.backend.RUnlock()
			fx.mu.Lock()
			defer fx.mu.Unlock()
			fx.uploads = append(fx.uploads, dedupeUpload{
				designations: a.Get(4).(map[string]map[string]string),
				global:       global,
			})
		})
	fx.sourcer.On("ReleaseBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	var logger *logrus.Logger
	logger, fx.hook = test.NewNullLogger()
	fx.provider = NewBackupTaskProvider(BackupTaskProviderParams{
		Node:          node,
		Logger:        logger,
		Cfg:           config.Backup{},
		Sourcer:       fx.sourcer,
		Backends:      &fakeBackupBackendProvider{backend: fx.backend},
		DedupePlanner: fx.planner,
	})
	fx.provider.planPollInterval = 5 * time.Millisecond
	fx.provider.SetCompletionRecorder(fx.recorder)
	return fx
}

// start launches the flow. The recorder pins each claimed unit to this node on
// the task the flow reads, so a claim made by this start never looks like an
// earlier one.
func (fx *dedupeFlow) start(t *testing.T) distributedtask.TaskHandle {
	t.Helper()
	task := makeTask("b1", distributedtask.TaskStatusStarted, fx.payload)
	for _, cls := range fx.payload.Nodes[fx.node].Classes {
		unitID := fmt.Sprintf("%s/%s", fx.node, cls)
		task.Units[unitID] = &distributedtask.Unit{ID: unitID, NodeID: fx.claimedBy}
	}
	fx.recorder.claimTask = task
	handle, err := fx.provider.StartTask(task)
	require.NoError(t, err)
	return handle
}

func (fx *dedupeFlow) run(t *testing.T) {
	t.Helper()
	waitDone(t, fx.start(t))
}

func (fx *dedupeFlow) getUploads() []dedupeUpload {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return append([]dedupeUpload(nil), fx.uploads...)
}

func (fx *dedupeFlow) globalWrites() int {
	n := 0
	for _, call := range fx.backend.Calls {
		if call.Method == "PutObject" && call.Arguments.Get(2) == GlobalBackupFile {
			n++
		}
	}
	return n
}

// serveGlobal serves raw for every global descriptor read of the backup.
func (fx *dedupeFlow) serveGlobal(raw []byte, err error) *mock.Call {
	return fx.backend.On("GetObject", mock.Anything, "b1", GlobalBackupFile).Return(raw, err)
}

// withBase points the payload at base, whose global descriptor read returns
// (raw, err). No node has a descriptor in base, so each uploads in full.
func (fx *dedupeFlow) withBase(raw []byte, err error) *mock.Call {
	fx.payload.BaseBackupID = "base"
	fx.backend.On("GetObject", mock.Anything, "base/"+fx.node, BackupFile).Return(nil, backup.ErrNotFound{})
	return fx.backend.On("GetObject", mock.Anything, "base", GlobalBackupFile).Return(raw, err)
}

func waitDone(t *testing.T, handle distributedtask.TaskHandle) {
	t.Helper()
	select {
	case <-handle.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the flow did not finish")
	}
}

func marshalDescriptor(t *testing.T, desc backup.DistributedBackupDescriptor) []byte {
	t.Helper()
	raw, err := json.Marshal(desc)
	require.NoError(t, err)
	return raw
}

func TestBackupTaskProviderDedupe(t *testing.T) {
	published := backup.DistributedBackupDescriptor{
		ID:                     "b1",
		Status:                 backup.Started,
		Version:                VersionDedupeReplicas,
		DedupeReplicas:         true,
		DedupeDesignatedShards: 2,
		DedupeDesignations:     map[string]map[string]string{"Article": {"s1": "node-2", "s2": "node-2"}},
	}

	t.Run("planner publishes the plan before uploading and uploads with its designations", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-1")
		wantPlan := map[string]map[string]string{"Article": {"s1": "node-2", "s2": "node-1"}}
		fx.planner.plan = &DedupePlan{Designations: wantPlan, Cutoffs: map[string]int64{"Article": 1}, CandidateShards: 2}
		baseDesignations := map[string]map[string]string{"Article": {"s1": "node-2"}}
		fx.withBase(marshalDescriptor(t, backup.DistributedBackupDescriptor{
			ID: "base", Status: backup.Success, DedupeDesignations: baseDesignations,
		}), nil)
		// The re-read after publishing serves a plan unlike the one written, so
		// only an upload from the stored plan matches it.
		fx.serveGlobal(nil, backup.ErrNotFound{}).Twice()
		fx.serveGlobal(marshalDescriptor(t, published), nil)

		fx.run(t)

		calls := fx.planner.recordedCalls()
		require.Len(t, calls, 1)
		assert.Equal(t, fx.payload.Classes, calls[0].classes)
		assert.Equal(t, map[string]struct{}{"node-1": {}, "node-2": {}}, calls[0].participants)
		assert.Equal(t, baseDesignations, calls[0].preferred, "the base designations are offered to the planner")
		assert.Equal(t, 1, fx.globalWrites())
		uploads := fx.getUploads()
		require.Len(t, uploads, 1)
		assert.Equal(t, backup.Started, uploads[0].global.Status, "the plan is published before the upload")
		assert.Equal(t, wantPlan, uploads[0].global.DedupeDesignations)
		assert.Equal(t, VersionDedupeReplicas, uploads[0].global.Version)
		assert.True(t, uploads[0].global.DedupeReplicas)
		assert.Equal(t, 2, uploads[0].global.DedupeDesignatedShards)
		assert.Zero(t, uploads[0].global.DedupeFallbackShards)

		assert.Equal(t, published.DedupeDesignations, uploads[0].designations, "the upload uses the stored plan")
		version, dedupe := fx.backend.getMetaStamp()
		assert.Equal(t, VersionDedupeReplicas, version)
		assert.True(t, dedupe)
		assert.Empty(t, fx.recorder.getFailures())
		assert.ElementsMatch(t, []string{"node-1/Article", "node-1/Book"}, fx.recorder.getCompletions())
	})

	t.Run("non-planner waits for the published plan and uploads with it", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-2")
		fx.serveGlobal(nil, backup.ErrNotFound{}).Once()
		fx.serveGlobal(marshalDescriptor(t, published), nil)

		fx.run(t)

		uploads := fx.getUploads()
		require.Len(t, uploads, 1)
		assert.Equal(t, published.DedupeDesignations, uploads[0].designations)
		version, dedupe := fx.backend.getMetaStamp()
		assert.Equal(t, VersionDedupeReplicas, version)
		assert.True(t, dedupe)
		assert.Zero(t, fx.globalWrites(), "only the planner writes the Started descriptor")
		assert.Empty(t, fx.planner.recordedCalls())
		assert.Empty(t, fx.recorder.getFailures())
		assert.Equal(t, []string{"node-2/Article"}, fx.recorder.getCompletions())
	})

	t.Run("non-planner never takes a terminal descriptor for a plan", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-2")
		cancelled := published
		cancelled.Status = backup.Cancelled
		var reads atomic.Int32
		fx.serveGlobal(marshalDescriptor(t, cancelled), nil).Run(func(mock.Arguments) { reads.Add(1) })

		handle := fx.start(t)
		require.Eventually(t, func() bool { return reads.Load() >= 3 }, 5*time.Second, time.Millisecond,
			"the non-planner keeps polling past a terminal descriptor")
		handle.Terminate()
		waitDone(t, handle)

		assert.Empty(t, fx.getUploads())
		assert.Empty(t, fx.recorder.getFailures())
	})

	t.Run("non-planner fails its units when the published plan stays unreadable", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-2")
		var reads atomic.Int32
		fx.serveGlobal(nil, errors.New("backend unavailable")).Run(func(mock.Arguments) { reads.Add(1) })

		fx.run(t)

		assert.Equal(t, int32(planReadAttempts), reads.Load())
		assert.Equal(t, []string{"node-2/Article"}, fx.recorder.getFailures())
		assert.Empty(t, fx.recorder.getRetryableFailures())
		for _, msg := range fx.recorder.getFailureMessages() {
			assert.Contains(t, msg, "backend unavailable")
		}
		assert.Empty(t, fx.getUploads())
	})

	t.Run("non-planner fails no unit when the read that would reach the bound ends with the flow", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-2")
		fx.serveGlobal(nil, errors.New("backend unavailable")).Times(planReadAttempts - 1)
		reading := make(chan struct{})
		fx.serveGlobal(nil, errors.New("read aborted")).Run(func(a mock.Arguments) {
			close(reading)
			<-a.Get(0).(context.Context).Done()
		})

		handle := fx.start(t)
		select {
		case <-reading:
		case <-time.After(5 * time.Second):
			t.Fatal("the non-planner never made its last bounded read")
		}
		handle.Terminate()
		waitDone(t, handle)

		assert.Empty(t, fx.recorder.getFailures())
		assert.Empty(t, fx.getUploads())
	})

	t.Run("non-planner resets its read-failure count after a successful read", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-2")
		readErr := errors.New("backend unavailable")
		// Four read errors in total. Only the not-found read between them keeps
		// the failure count below planReadAttempts.
		fx.serveGlobal(nil, readErr).Twice()
		fx.serveGlobal(nil, backup.ErrNotFound{}).Once()
		fx.serveGlobal(nil, readErr).Twice()
		fx.serveGlobal(marshalDescriptor(t, published), nil)

		fx.run(t)

		uploads := fx.getUploads()
		require.Len(t, uploads, 1)
		assert.Equal(t, published.DedupeDesignations, uploads[0].designations)
		assert.Empty(t, fx.recorder.getFailures())
		assert.Equal(t, []string{"node-2/Article"}, fx.recorder.getCompletions())
	})

	t.Run("a plan with fallback shards is published and the backup continues", func(t *testing.T) {
		tests := []struct {
			name             string
			setup            func(*dedupeFlow)
			wantDesignations map[string]map[string]string
			wantWarning      string
		}{
			{
				name: "every shard falls back",
				setup: func(fx *dedupeFlow) {
					fx.planner.plan = &DedupePlan{CandidateShards: 2}
				},
			},
			{
				name: "some shards fall back",
				setup: func(fx *dedupeFlow) {
					fx.planner.plan = &DedupePlan{
						Designations:    map[string]map[string]string{"Article": {"s1": "node-1"}},
						Cutoffs:         map[string]int64{"Article": 1},
						CandidateShards: 2,
					}
				},
				wantDesignations: map[string]map[string]string{"Article": {"s1": "node-1"}},
			},
			{
				name: "base descriptor read error plans without base designations",
				setup: func(fx *dedupeFlow) {
					fx.planner.plan = &DedupePlan{
						Designations:    map[string]map[string]string{"Article": {"s1": "node-1", "s2": "node-2"}},
						Cutoffs:         map[string]int64{"Article": 1},
						CandidateShards: 2,
					}
					fx.withBase(nil, errors.New("backend unavailable"))
				},
				wantDesignations: map[string]map[string]string{"Article": {"s1": "node-1", "s2": "node-2"}},
				wantWarning:      `base backup "base" unreadable`,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				fx := newDedupeFlow(t, "node-1")
				fx.backend.serveWrittenGlobalBackupMeta = true
				fx.serveGlobal(nil, backup.ErrNotFound{})
				tc.setup(fx)

				fx.run(t)

				calls := fx.planner.recordedCalls()
				require.Len(t, calls, 1)
				assert.Nil(t, calls[0].preferred)
				got := fx.backend.glMeta
				assert.Equal(t, backup.Started, got.Status)
				assert.Equal(t, tc.wantDesignations, got.DedupeDesignations)
				assert.Equal(t, len(tc.wantDesignations) > 0, got.DedupeReplicas)
				assert.Equal(t, len(tc.wantDesignations["Article"]), got.DedupeDesignatedShards)
				assert.Equal(t, 2-len(tc.wantDesignations["Article"]), got.DedupeFallbackShards)
				uploads := fx.getUploads()
				require.Len(t, uploads, 1)
				assert.Equal(t, tc.wantDesignations, uploads[0].designations)
				assert.Empty(t, fx.recorder.getFailures())
				assert.ElementsMatch(t, []string{"node-1/Article", "node-1/Book"}, fx.recorder.getCompletions())
				if tc.wantWarning != "" {
					assert.True(t, slices.ContainsFunc(fx.hook.AllEntries(), func(e *logrus.Entry) bool {
						return e.Level == logrus.WarnLevel && strings.Contains(e.Message, tc.wantWarning)
					}), "want a warning containing %q", tc.wantWarning)
				}
			})
		}
	})

	t.Run("cancel during convergence stops planning and prevents uploads", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-1")
		fx.serveGlobal(nil, backup.ErrNotFound{})
		fx.planner.blockUntilCancelled = true
		fx.planner.plan = &DedupePlan{CandidateShards: 2}

		handle := fx.start(t)
		require.Eventually(t, func() bool { return len(fx.planner.recordedCalls()) > 0 }, 5*time.Second, time.Millisecond)
		cancelled := fx.planner.recordedCalls()[0].cancelled
		assert.False(t, cancelled(), "a running flow is not cancelled")
		handle.Terminate()
		waitDone(t, handle)

		assert.True(t, cancelled(), "the ended flow reports cancelled to the planner")
		assert.Zero(t, fx.globalWrites(), "no plan is published")
		fx.sourcer.AssertNotCalled(t, "BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		assert.Empty(t, fx.recorder.getFailures())
	})

	t.Run("cancel during the base descriptor read plans nothing and logs no read failure", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-1")
		fx.serveGlobal(nil, backup.ErrNotFound{})
		reading := make(chan struct{})
		fx.withBase(nil, errors.New("read aborted")).Run(func(a mock.Arguments) {
			close(reading)
			<-a.Get(0).(context.Context).Done()
		})

		handle := fx.start(t)
		select {
		case <-reading:
		case <-time.After(5 * time.Second):
			t.Fatal("the planner never read the base descriptor")
		}
		handle.Terminate()
		waitDone(t, handle)

		for _, entry := range fx.hook.AllEntries() {
			assert.NotContains(t, entry.Message, "unreadable")
		}
		assert.Empty(t, fx.planner.recordedCalls())
		assert.Zero(t, fx.globalWrites())
		fx.sourcer.AssertNotCalled(t, "BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		assert.Empty(t, fx.recorder.getFailures())
	})

	t.Run("restart after claim with no published plan fails with a planning-interruption reason", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-1")
		fx.claimedBy = "node-1"
		fx.serveGlobal(nil, backup.ErrNotFound{})

		fx.run(t)

		assert.ElementsMatch(t, []string{"node-1/Article", "node-1/Book"}, fx.recorder.getFailures())
		assert.Empty(t, fx.recorder.getRetryableFailures())
		for _, msg := range fx.recorder.getFailureMessages() {
			assert.Contains(t, msg, "interrupted")
			assert.Contains(t, msg, "new backup ID")
		}
		assert.Empty(t, fx.planner.recordedCalls())
		assert.Zero(t, fx.globalWrites())
		fx.sourcer.AssertNotCalled(t, "BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("restart after publication reuses the plan without replanning", func(t *testing.T) {
		fx := newDedupeFlow(t, "node-1")
		fx.claimedBy = "node-1"
		fx.serveGlobal(marshalDescriptor(t, published), nil)

		fx.run(t)

		assert.Empty(t, fx.planner.recordedCalls())
		assert.Zero(t, fx.globalWrites())
		uploads := fx.getUploads()
		require.Len(t, uploads, 1)
		assert.Equal(t, published.DedupeDesignations, uploads[0].designations)
		assert.Empty(t, fx.recorder.getFailures())
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

	shardingState := []byte(`{"physical":{"s1":{"belongsToNodes":["node-1","node-2"]}}}`)
	published := backup.DistributedBackupDescriptor{
		ID:                     "b1",
		Status:                 backup.Started,
		Version:                VersionDedupeReplicas,
		DedupeReplicas:         true,
		DedupeDesignatedShards: 1,
		DedupeFallbackShards:   1,
		DedupeCutoffsMs:        map[string]int64{"Article": 1234},
		DedupeDesignations:     map[string]map[string]string{"Article": {"s1": "node-1"}},
	}
	nodeMeta := func(size int64, shards ...*backup.ShardDescriptor) []byte {
		raw, _ := json.Marshal(backup.BackupDescriptor{
			Status:                  backup.Success,
			PreCompressionSizeBytes: size,
			Classes:                 []backup.ClassDescriptor{{Name: "Article", ShardingState: shardingState, Shards: shards}},
		})
		return raw
	}
	// completeDedupeBackup runs OnTaskCompleted for a successful dedupe task
	// over a backend serving the given global descriptor read and node-1 meta.
	completeDedupeBackup := func(t *testing.T, globalRaw []byte, globalErr error, node1Meta []byte) (*fakeBackend, error) {
		t.Helper()
		logger, _ := test.NewNullLogger()
		be := newFakeBackend()
		be.On("GetObject", mock.Anything, "b1", GlobalBackupFile).Return(globalRaw, globalErr)
		be.On("GetObject", mock.Anything, "b1", BackupFile).Return(nil, backup.ErrNotFound{})
		be.On("GetObject", mock.Anything, "b1/node-1", BackupFile).Return(node1Meta, nil)
		be.On("GetObject", mock.Anything, "b1/node-2", BackupFile).Return(nodeMeta(0), nil)
		be.On("PutObject", mock.Anything, "b1", GlobalBackupFile, mock.Anything).Return(nil)

		provider := NewBackupTaskProvider(BackupTaskProviderParams{
			Node:     "node-1",
			Logger:   logger,
			Cfg:      config.Backup{},
			Backends: &fakeBackupBackendProvider{backend: be},
		})
		payload := makePayload("b1")
		payload.DedupeReplicas = true
		return be, provider.OnTaskCompleted(makeTask("b1", distributedtask.TaskStatusFinished, payload))
	}
	publishedRaw, err := json.Marshal(published)
	require.NoError(t, err)

	t.Run("dedupe artifact keeps the published plan", func(t *testing.T) {
		be, err := completeDedupeBackup(t, publishedRaw, nil,
			nodeMeta(1000, &backup.ShardDescriptor{Name: "s1", Node: "node-1", PreCompressionSizeBytes: 1000}))
		require.NoError(t, err)

		got := be.glMeta
		assert.Equal(t, backup.Success, got.Status)
		assert.Equal(t, VersionDedupeReplicas, got.Version)
		assert.True(t, got.DedupeReplicas)
		assert.Equal(t, 1, got.DedupeDesignatedShards)
		assert.Equal(t, 1, got.DedupeFallbackShards)
		assert.Equal(t, published.DedupeCutoffsMs, got.DedupeCutoffsMs)
		assert.Equal(t, published.DedupeDesignations, got.DedupeDesignations)
		assert.Equal(t, int64(1000), got.DedupeSkippedBytes, "sizes are attributed to the skipping replica")
		assert.Equal(t, int64(1000), got.Nodes["node-2"].PreCompressionSizeBytes)
		assert.Equal(t, int64(2000), got.PreCompressionSizeBytes)
	})

	t.Run("designated shard missing from its archiver fails the backup", func(t *testing.T) {
		be, err := completeDedupeBackup(t, publishedRaw, nil, nodeMeta(0))
		require.NoError(t, err)

		got := be.glMeta
		assert.Equal(t, backup.Failed, got.Status)
		assert.Contains(t, got.Error, "designated shard")
		assert.True(t, got.DedupeReplicas, "a failed artifact still carries the published plan")
		assert.Zero(t, got.DedupeSkippedBytes, "a failed backup must not attribute sizes")
	})

	t.Run("unreadable published plan fails closed", func(t *testing.T) {
		be, err := completeDedupeBackup(t, nil, errors.New("backend unavailable"),
			nodeMeta(1000, &backup.ShardDescriptor{Name: "s1", Node: "node-1", PreCompressionSizeBytes: 1000}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backend unavailable")
		be.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, GlobalBackupFile, mock.Anything)
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
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}, nil, nil),
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
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}, nil, nil),
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
	backend   *fakeBackend
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
		backend:   fs.backend,
		req:       &BackupRequest{ID: backupID, Backend: "gcs", Include: []string{cls}},
	}
}

func TestBackupDTMDedupe(t *testing.T) {
	t.Run("dedupe request is admitted and proposed with dedupe fields", func(t *testing.T) {
		fixture := newDTMProposeFixture(t)
		fixture.scheduler.dedupeMode = license.FeatureLicensed
		fixture.scheduler.backupper.dedupePlanner = &fakeDedupePlanner{}
		fixture.req.DedupeReplicas = true
		fixture.req.DedupeConvergenceTimeoutSeconds = 30

		resp, err := fixture.scheduler.Backup(context.Background(), nil, fixture.req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotEmpty(t, fixture.dtm.proposedPayload)
		proposed, err := unmarshalTaskPayload(fixture.dtm.proposedPayload)
		require.NoError(t, err)
		assert.True(t, proposed.DedupeReplicas)
		assert.Equal(t, 30, proposed.DedupeConvergenceTimeoutSeconds)
		assert.Empty(t, fixture.scheduler.backupper.lastOp.get().ID, "a DTM backup never takes the 2PC slot")
	})

	t.Run("unlicensed dedupe request is refused before it is proposed", func(t *testing.T) {
		fixture := newDTMProposeFixture(t)
		fixture.scheduler.dedupeMode = license.FeatureUnlicensed
		fixture.req.DedupeReplicas = true

		_, err := fixture.scheduler.Backup(context.Background(), nil, fixture.req)
		require.Error(t, err)
		assert.Empty(t, fixture.dtm.proposedPayload)
	})
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
			backupper:    newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}, nil, nil),
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

func (a *recordingAuthorizer) AuthorizeAndRequireActiveNamespace(ctx context.Context, pr *models.Principal, verb string, _ string, resources ...string) error {
	return a.Authorize(ctx, pr, verb, resources...)
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
			backupper:  newCoordinator(&fakeSelector{}, &fakeClient{}, &fakeSchemaManger{}, logger, &fakeNodeResolver{}, &fakeBackupBackendProvider{backend: be}, nil, nil),
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
