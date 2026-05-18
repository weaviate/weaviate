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

// TestHandler_ShutdownCancelsParticipantContexts verifies that Handler.Shutdown
// cancels the shutdown ctx shared by both the backupper and the restorer.
func TestHandler_ShutdownCancelsParticipantContexts(t *testing.T) {
	t.Parallel()
	m := createManager(nil, nil, nil, nil)

	assert.NoError(t, m.backupper.shutdownCtx.Err(), "backupper ctx should not be cancelled before shutdown")
	assert.NoError(t, m.restorer.shutdownCtx.Err(), "restorer ctx should not be cancelled before shutdown")

	m.Shutdown()

	assert.ErrorIs(t, m.backupper.shutdownCtx.Err(), context.Canceled)
	assert.ErrorIs(t, m.restorer.shutdownCtx.Err(), context.Canceled)

	// Idempotent.
	m.Shutdown()
	assert.ErrorIs(t, m.backupper.shutdownCtx.Err(), context.Canceled)
}

// TestScheduler_ShutdownIdempotentAndDrains verifies that Scheduler.Shutdown is
// safe to call multiple times and that Wait blocks until both coordinators'
// inflight goroutines have exited.
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

	// Simulate an in-flight backup goroutine.
	s.backupper.inflight.Add(1)
	released := make(chan struct{})
	go func() {
		<-released
		s.backupper.inflight.Done()
	}()

	s.Shutdown()
	s.Shutdown() // must not panic / re-fire.

	assert.ErrorIs(t, shutdownCtx.Err(), context.Canceled)

	// Wait returns on timeout while the goroutine is still in flight.
	start := time.Now()
	s.Wait(50 * time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	select {
	case <-s.drained:
		t.Fatal("drained should still be open while inflight goroutine is running")
	default:
	}

	// Once the inflight goroutine completes, Wait returns immediately.
	close(released)
	start = time.Now()
	s.Wait(5 * time.Second)
	assert.Less(t, time.Since(start), 1*time.Second)
	select {
	case <-s.drained:
	default:
		t.Fatal("drained should be closed once all inflight goroutines exit")
	}
}

// TestCoordinator_BackupShutdownCancelsAndAborts verifies that on node
// shutdown an in-flight backup coordinator persists a Cancelled descriptor
// and dispatches abort RPCs to all participants.
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
	// Commit succeeds so the goroutine enters the polling loop with both
	// participants still active; the pre-cancelled shutdownCtx then trips the
	// select arm and flips the descriptor to Cancelled.
	fc.client.On("Commit", any, any, any).Return(nil)
	fc.client.On("Status", any, any, any).Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpCreate}, nil).Maybe()
	fc.client.On("Abort", any, nodes[0], abortReq).Return(nil).Once()
	fc.client.On("Abort", any, nodes[1], abortReq).Return(nil).Once()
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	shutdownCancel() // shutdown before the inflight goroutine starts

	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil, shutdownCtx)
	c.timeoutNextRound = time.Second // large so the ctx.Done branch wins the select

	req := newReq(classes, backendName, backupID)
	store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
	err := c.Backup(context.Background(), store, &req)
	assert.NoError(t, err)
	c.inflight.Wait()

	got := fc.backend.glMeta
	assert.Equal(t, backup.Cancelled, got.Status, "descriptor must be Cancelled after shutdown")
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[0], abortReq)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[1], abortReq)
}

// TestCoordinator_RestoreShutdownAbortsParticipants verifies that on shutdown
// the restore coordinator also sends abort RPCs, even though restore otherwise
// tolerates partial failures (regression guard for Fix #2).
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
	assert.Equal(t, backup.Cancelled, got.Status, "restore descriptor must be Cancelled after shutdown")
	// Without Fix #2 these assertions would fail: restore tolerates partial
	// failure, so the original code skipped abortAll entirely on shutdown.
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[0], abortReq)
	fc.client.AssertCalled(t, "Abort", mock.Anything, nodes[1], abortReq)
}

// TestRestorer_ShutdownCancelsLocalRestore verifies that a participant-side
// restore in flight reacts to Handler.Shutdown (regression guard for Fix #1).
// Pre-fix, restorer.restore wrapped the work in context.Background() so the
// local restore continued running past node shutdown.
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
	// WriteToFile honours ctx cancellation. If the restore ctx is not wired to
	// shutdownCtx the call would block forever and waitForCompletion would
	// time out. With the wiring in place, Handler.Shutdown cancels the ctx and
	// WriteToFile returns ctx.Err(), which propagates as a Failed restore.
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
		t.Fatal("restore goroutine did not reach WriteToFile in time")
	}

	// Fire shutdown while the restore is mid-upload.
	m.Shutdown()

	st := m.restorer.waitForCompletion(req.Backend, req.ID, 40, 50)
	assert.Equal(t, backup.Failed, st.Status,
		"restore must terminate after Handler.Shutdown propagates ctx cancellation")
	assert.Contains(t, st.Err, context.Canceled.Error())
}

// TestBackupper_ShutdownCancelsLocalUpload mirrors the restorer test for the
// participant-side backupper to confirm the shutdownCtx wiring is intact.
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

	// Block BackupDescriptors until we trigger shutdown so the upload ctx is
	// known to be alive when Shutdown fires.
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
		t.Fatal("backup goroutine did not reach BackupDescriptors in time")
	}

	m.Shutdown()

	got := m.backupper.waitForCompletion(40, 50)
	// Successful completion is impossible because descriptorRelease is still
	// blocked: if the upload were not cancelled by Shutdown, waitForCompletion
	// would time out and return "".
	assert.NotEqual(t, backup.Success, got, "shutdown must terminate the upload before BackupDescriptors yields")
}

// TestScheduler_RejectsRequestsAfterShutdown verifies that Backup/Restore/Cancel
// reject with errShuttingDown after Shutdown. Without this gate a request
// arriving between Shutdown and HTTP server close would race the inflight
// WaitGroup (Add while Wait is running with counter 0 → panic).
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

	err = s.Cancel(context.Background(), nil, "s3", "x", "", "")
	assert.ErrorContains(t, err, errShuttingDown.Error())
}

// TestWaitForCoordinator_AbortsOnShutdown verifies that the participant's
// can-commit→commit wait reacts to shutdownCtx. Pre-fix this would block
// until _BookingPeriod expired.
func TestWaitForCoordinator_AbortsOnShutdown(t *testing.T) {
	t.Parallel()
	s := shardSyncChan{coordChan: make(chan interface{}, 5)}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		// Use a long expiration so the timer arm cannot win the race.
		done <- s.waitForCoordinator(shutdownCtx, time.Hour, "id-1")
	}()

	// Give the goroutine a chance to enter the select.
	time.Sleep(20 * time.Millisecond)
	shutdownCancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "node shutting down")
	case <-time.After(2 * time.Second):
		t.Fatal("waitForCoordinator did not return after shutdownCtx cancel")
	}
}
