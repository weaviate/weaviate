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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Handler.Shutdown cancels the backupper and restorer ctx and is idempotent.
func TestHandler_ShutdownCancelsParticipantContexts(t *testing.T) {
	t.Parallel()
	m := createManager(nil, nil, nil, nil)

	assert.NoError(t, m.backupper.shutdownCtx.Err())
	assert.NoError(t, m.restorer.shutdownCtx.Err())

	m.Shutdown()
	assert.ErrorIs(t, m.backupper.shutdownCtx.Err(), context.Canceled)
	assert.ErrorIs(t, m.restorer.shutdownCtx.Err(), context.Canceled)

	m.Shutdown()
	assert.ErrorIs(t, m.backupper.shutdownCtx.Err(), context.Canceled)
}

// Handler.Wait blocks until participant goroutines exit.
func TestHandler_WaitDrainsInflightGoroutines(t *testing.T) {
	t.Parallel()
	m := createManager(nil, nil, nil, nil)

	m.inflight.Add(1)
	released := make(chan struct{})
	go func() {
		<-released
		m.inflight.Done()
	}()

	m.Shutdown()

	start := time.Now()
	m.wait(50 * time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	select {
	case <-m.drained:
		t.Fatal("drained closed while inflight goroutine still running")
	default:
	}

	close(released)
	start = time.Now()
	m.wait(5 * time.Second)
	assert.Less(t, time.Since(start), 1*time.Second)
	select {
	case <-m.drained:
	default:
		t.Fatal("drained not closed after inflight goroutine exited")
	}
}

// OnCanCommit rejects after Handler.Shutdown for both backup and restore.
func TestHandler_OnCanCommitRejectsAfterShutdown(t *testing.T) {
	t.Parallel()
	for _, op := range []Op{OpCreate, OpRestore} {
		t.Run(string(op), func(t *testing.T) {
			t.Parallel()
			m := createManager(nil, nil, nil, nil)
			m.Shutdown()

			req := &Request{Method: op, ID: "x", Backend: "s3"}
			resp := m.OnCanCommit(context.Background(), req)
			assert.Contains(t, resp.Err, errShuttingDown.Error())
		})
	}
}

// Scheduler.Shutdown is idempotent; Wait blocks until both coordinators drain.
func TestScheduler_ShutdownIdempotentAndDrains(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	s := &Scheduler{
		logger:         logger,
		shutdownCancel: shutdownCancel,
		drained:        make(chan struct{}),
		backupper:      newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
		restorer:       newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
	}

	s.backupper.inflight.Add(1)
	released := make(chan struct{})
	go func() {
		<-released
		s.backupper.inflight.Done()
	}()

	s.Shutdown()
	s.Shutdown() // idempotent

	assert.ErrorIs(t, shutdownCtx.Err(), context.Canceled)

	start := time.Now()
	s.wait(50 * time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	select {
	case <-s.drained:
		t.Fatal("drained closed while inflight goroutine still running")
	default:
	}

	close(released)
	start = time.Now()
	s.wait(5 * time.Second)
	assert.Less(t, time.Since(start), 1*time.Second)
	select {
	case <-s.drained:
	default:
		t.Fatal("drained not closed after all inflight goroutines exited")
	}
}

// Backup coord: shutdown persists Cancelled descriptor + aborts participants.
func TestCoordinator_BackupShutdownCancelsAndAborts(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		backupID     = "shutdown-backup"
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A"}
		nodeResolver = newFakeNodeResolver(nodes)
		any          = mock.Anything
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		abortReq     = &AbortRequest{OpCreate, backupID, backendName, "", "", ""}
	)

	fc := newFakeCoordinator(nodeResolver)
	fc.selector.On("Shards", mock.Anything, classes[0]).Return(nodes, nil)
	fc.client.On("CanCommit", any, nodes[0], any).Return(cresp, nil)
	fc.client.On("CanCommit", any, nodes[1], any).Return(cresp, nil)
	// Commit returns nil so the polling loop enters; pre-cancelled shutdownCtx
	// then trips the select arm.
	fc.client.On("Commit", any, any, any).Return(nil)
	fc.client.On("Status", any, any, any).Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpCreate}, nil).Maybe()
	fc.client.On("Abort", any, nodes[0], abortReq).Return(nil).Once()
	fc.client.On("Abort", any, nodes[1], abortReq).Return(nil).Once()
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	shutdownCancel()

	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil, shutdownCtx)
	c.timeoutNextRound = time.Second // ensure ctx.Done arm wins the select

	req := newReq(classes, backendName, backupID)
	store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
	err := c.Backup(context.Background(), store, &req)
	assert.NoError(t, err)
	c.inflight.Wait()

	got := fc.backend.glMeta
	assert.Equal(t, backup.Cancelled, got.Status)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[0], abortReq)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[1], abortReq)
}

// Restore coord: shutdown also sends abort RPCs despite toleratePartialFailure=true.
func TestCoordinator_RestoreShutdownAbortsParticipants(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		backupID     = "shutdown-restore"
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A"}
		nodeResolver = newFakeNodeResolver(nodes)
		any          = mock.Anything
		cresp        = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		abortReq     = &AbortRequest{OpRestore, backupID, backendName, "", "", ""}
		desc         = &backup.DistributedBackupDescriptor{
			ID:     backupID,
			Status: backup.Success,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[0]: {Classes: classes, Status: backup.Success},
				nodes[1]: {Classes: classes, Status: backup.Success},
			},
		}
	)

	fc := newFakeCoordinator(nodeResolver)
	fc.selector.On("Shards", mock.Anything, classes[0]).Return(nodes, nil)
	fc.client.On("CanCommit", any, nodes[0], any).Return(cresp, nil)
	fc.client.On("CanCommit", any, nodes[1], any).Return(cresp, nil)
	fc.client.On("Commit", any, any, any).Return(nil)
	fc.client.On("Status", any, any, any).Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil).Maybe()
	fc.client.On("Abort", any, nodes[0], abortReq).Return(nil).Once()
	fc.client.On("Abort", any, nodes[1], abortReq).Return(nil).Once()
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Twice()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	shutdownCancel() // shutdown fires before the inflight goroutine starts.

	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil, shutdownCtx)
	c.timeoutNextRound = time.Second

	req := newReq([]string{}, backendName, "")
	store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
	err := c.Restore(context.Background(), store, &req, desc, nil)
	assert.NoError(t, err)
	c.inflight.Wait()

	got := fc.backend.glMeta
	assert.Equal(t, backup.Cancelled, got.Status)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[0], abortReq)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[1], abortReq)
}

// Participant restore: Handler.Shutdown cancels the local restore ctx.
func TestRestorer_ShutdownCancelsLocalRestore(t *testing.T) {
	t.Parallel()
	var (
		backendName = "gcs"
		backupID    = "shutdown-restore-participant"
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		ctx         = context.Background()
		cls         = "Class-A"
		rawbytes    = []byte("hello")
		req         = Request{
			Method:   OpRestore,
			ID:       backupID,
			Classes:  []string{cls},
			Backend:  backendName,
			Duration: time.Hour,
		}
	)
	rawShardingStateBytes, _ := json.Marshal(&sharding.State{
		IndexID: cls,
		Physical: map[string]sharding.Physical{"shard-1": {
			Name:           "shard-1",
			BelongsToNodes: []string{nodeName},
		}},
	})
	rawClassBytes, _ := json.Marshal(&models.Class{Class: cls})
	metadata := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     time.Now().UTC(),
		Status:        backup.Success,
		Version:       "1",
		ServerVersion: "1",
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: rawShardingStateBytes,
			Shards: []*backup.ShardDescriptor{{
				Name: "shard-1", Node: nodeName,
				Files:                 []string{"dir1/file1"},
				DocIDCounterPath:      "counter.txt",
				ShardVersionPath:      "version.txt",
				PropLengthTrackerPath: "prop.txt",
				DocIDCounter:          rawbytes,
				Version:               rawbytes,
				PropLengthTracker:     rawbytes,
			}},
		}},
	}

	backend := newFakeBackend()
	sourcer := &fakeSourcer{}
	bytes := marshalMeta(metadata)
	backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
	backend.On("SourceDataPath").Return(t.TempDir())
	// WriteToFile blocks on its own ctx; shutdown should propagate cancellation.
	writeStarted := make(chan struct{})
	writeOnce := false
	backend.On("WriteToFile", mock.Anything, nodeHome, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			if !writeOnce {
				writeOnce = true
				close(writeStarted)
			}
			callCtx := args.Get(0).(context.Context)
			<-callCtx.Done()
		}).
		Return(context.Canceled).Maybe()

	m := createManager(sourcer, nil, backend, nil)

	resp := m.OnCanCommit(ctx, &req)
	assert.Empty(t, resp.Err)
	assert.NoError(t, m.OnCommit(ctx, &StatusRequest{Method: OpRestore, ID: req.ID, Backend: req.Backend}))

	select {
	case <-writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not reach WriteToFile in time")
	}

	m.Shutdown()

	st := m.restorer.waitForCompletion(req.Backend, req.ID, 40, 50)
	assert.Equal(t, backup.Failed, st.Status)
	assert.Contains(t, st.Err, context.Canceled.Error())
}

// Participant backup: Handler.Shutdown cancels the local upload ctx.
func TestBackupper_ShutdownCancelsLocalUpload(t *testing.T) {
	t.Parallel()
	var (
		cls         = "Class-A"
		backendName = "gcs"
		backupID    = "shutdown-backup-participant"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = Request{
			Method:   OpCreate,
			ID:       backupID,
			Classes:  []string{cls},
			Backend:  backendName,
			Duration: time.Hour,
		}
		any = mock.Anything
	)

	sourcePath := t.TempDir()
	sourcer := &fakeSourcer{}
	sourcer.On("Backupable", ctx, req.Classes).Return(nil)

	// Block BackupDescriptors so the upload goroutine is parked when Shutdown fires.
	descriptorStarted := make(chan struct{})
	descriptorRelease := make(chan struct{})
	defer close(descriptorRelease)
	ch := make(chan backup.ClassDescriptor)
	go func() {
		close(descriptorStarted)
		<-descriptorRelease
		close(ch)
	}()
	var roCh <-chan backup.ClassDescriptor = ch
	sourcer.On("BackupDescriptors", any, backupID, any, any).Return(roCh).Maybe()
	sourcer.On("ReleaseBackup", ctx, backupID, any).Return(nil).Maybe()

	backend := newFakeBackend()
	backend.On("HomeDir", any, any, any).Return(path)
	backend.On("SourceDataPath").Return(sourcePath)
	backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
	backend.On("Initialize", ctx, nodeHome).Return(nil)
	backend.On("PutObject", any, nodeHome, BackupFile, any).Return(nil).Maybe()
	backend.On("Write", any, nodeHome, any, any).Return(any, nil).Maybe()

	m := createManager(sourcer, nil, backend, nil)

	resp := m.OnCanCommit(ctx, &req)
	assert.Empty(t, resp.Err)
	assert.NoError(t, m.OnCommit(ctx, &StatusRequest{Method: OpCreate, ID: req.ID, Backend: req.Backend}))

	select {
	case <-descriptorStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("backup did not reach BackupDescriptors in time")
	}

	m.Shutdown()

	got := m.backupper.waitForCompletion(40, 50)
	// descriptorRelease is still blocked, so Success is only reachable if
	// Shutdown didn't cancel the upload.
	assert.NotEqual(t, backup.Success, got)
}

// Backup/Restore reject with errShuttingDown after Shutdown.
func TestScheduler_RejectsRequestsAfterShutdown(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	s := &Scheduler{
		logger:         logger,
		shutdownCancel: shutdownCancel,
		drained:        make(chan struct{}),
		backupper:      newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
		restorer:       newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
	}

	s.Shutdown()

	req := &BackupRequest{ID: "x", Backend: "s3"}
	_, err := s.Backup(context.Background(), nil, req)
	assert.ErrorContains(t, err, errShuttingDown.Error())

	_, err = s.Restore(context.Background(), nil, req, false)
	assert.ErrorContains(t, err, errShuttingDown.Error())

	// Cancel is not gated; no assertion here.
}

// Shutdown blocks until in-flight RLock holders release, preventing the
// inflight.Add inside the coordinator from racing the drain goroutine's Wait.
func TestScheduler_ShutdownWaitsForInflightCalls(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	s := &Scheduler{
		logger:         logger,
		shutdownCancel: shutdownCancel,
		drained:        make(chan struct{}),
		backupper:      newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
		restorer:       newCoordinator(nil, nil, nil, logger, nil, nil, shutdownCtx),
	}

	inFlight := make(chan struct{})
	released := make(chan struct{})
	go func() {
		s.shutdownMu.RLock()
		close(inFlight)
		<-released
		s.shutdownMu.RUnlock()
	}()
	<-inFlight

	shutdownReturned := make(chan struct{})
	go func() {
		s.Shutdown()
		close(shutdownReturned)
	}()

	select {
	case <-shutdownReturned:
		t.Fatal("Shutdown returned while RLock was held")
	case <-time.After(100 * time.Millisecond):
	}

	close(released)

	select {
	case <-shutdownReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after RLock released")
	}
}

// waitForCoordinator exits early on shutdownCtx cancel.
func TestWaitForCoordinator_AbortsOnShutdown(t *testing.T) {
	t.Parallel()
	s := shardSyncChan{coordChan: make(chan interface{}, 5)}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- s.waitForCoordinator(shutdownCtx, time.Hour, "id-1")
	}()

	time.Sleep(20 * time.Millisecond) // let the goroutine enter the select
	shutdownCancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "node shutting down")
	case <-time.After(2 * time.Second):
		t.Fatal("waitForCoordinator did not return after shutdownCtx cancel")
	}
}
