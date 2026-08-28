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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ErrAny represent a random error
var (
	ErrAny = errors.New("any error")
	any    = mock.Anything
)

func (r *restorer) waitForCompletion(backend, id string, n, ms int) Status {
	for i := 0; i < n; i++ {
		delay := time.Millisecond * time.Duration(ms)
		time.Sleep(delay)
		status, err := r.status(backend, id)
		if err != nil {
			continue
		}
		if status.Status == backup.Success || status.Status == backup.Failed {
			return status
		}
	}
	return Status{}
}

func TestRestoreStatus(t *testing.T) {
	t.Parallel()
	var (
		backendType = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil, nil)
		starTime    = time.Now().UTC()
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
	)
	// initial state
	_, err := m.restorer.status(backendType, id)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("must return an error if backup doesn't exist")
	}
	// active state
	m.restorer.lastOp.reqState = reqState{
		Starttime: starTime,
		ID:        id,
		Status:    backup.Transferring,
		Path:      path,
	}
	st, err := m.restorer.status(backendType, id)
	if err != nil {
		t.Errorf("get active status: %v", err)
	}
	expected := Status{Path: path, StartedAt: starTime, Status: backup.Transferring}
	if expected != st {
		t.Errorf("get active status: got=%v want=%v", st, expected)
	}
	// cached status
	m.restorer.lastOp.reset()
	st.CompletedAt = starTime
	m.restorer.restoreStatusMap.Store("s3/"+id, st)
	st, err = m.restorer.status(backendType, id)
	if err != nil {
		t.Errorf("fetch status from map: %v", err)
	}
	expected.CompletedAt = starTime
	if expected != st {
		t.Errorf("fetch status from map got=%v want=%v", st, expected)
	}
}

func TestManagerCoordinatedRestore(t *testing.T) {
	var (
		backendName = "gcs"
		rawbytes    = []byte("hello")
		timept      = time.Now().UTC()
		// Article matches the chunk fixture registered in fakes_test.go.
		cls      = "Article"
		backupID = "2"
		ctx      = context.Background()
		nodeHome = backupID + "/" + nodeName
		path     = "bucket/backups/" + nodeHome
		req      = Request{
			Method:   OpRestore,
			ID:       backupID,
			Classes:  []string{cls},
			Backend:  backendName,
			Duration: time.Millisecond * 20,
		}
	)
	rawShardingStateBytes, _ := json.Marshal(&sharding.State{
		IndexID: cls,
		Physical: map[string]sharding.Physical{"cT9eTErXgmTX": {
			Name:           "cT9eTErXgmTX",
			BelongsToNodes: []string{nodeName},
		}},
	})
	rawClassBytes, _ := json.Marshal(&models.Class{
		Class: cls,
	})

	metadata := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     timept,
		Version:       Version,
		ServerVersion: "1.23",
		Status:        backup.Success,
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: rawShardingStateBytes,
			Chunks:        map[int32][]string{1: {"dir1/file1", "dir2/file2"}},
			Shards: []*backup.ShardDescriptor{
				{
					Name: "Shard1", Node: "Node-1",
					Files:                 []string{"dir1/file1", "dir2/file2"},
					DocIDCounterPath:      "counter.txt",
					ShardVersionPath:      "version.txt",
					PropLengthTrackerPath: "prop.txt",
					DocIDCounter:          rawbytes,
					Version:               rawbytes,
					PropLengthTracker:     rawbytes,
				},
			},
		}},
	}

	t.Run("GetMetadataFile", func(t *testing.T) {
		backend := newFakeBackend()
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		bm := createManager(nil, nil, backend, nil)
		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, errMetaNotFound.Error())
		assert.Equal(t, resp.Timeout, time.Duration(0))
	})

	t.Run("RejectSingleNodeBackup", func(t *testing.T) {
		backend := newFakeBackend()
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("GetObject", ctx, backupID, BackupFile).Return(marshalMeta(metadata), nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		bm := createManager(nil, nil, backend, nil)
		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, errLegacySingleNode.Error())
		assert.Equal(t, time.Duration(0), resp.Timeout)
	})

	t.Run("RejectLegacyBackup", func(t *testing.T) {
		tests := []struct {
			name          string
			version       string
			serverVersion string
			wantErr       error
		}{
			{name: "uncompressed 1.0", version: "1.0", serverVersion: metadata.ServerVersion, wantErr: errLegacyUncompressed},
			{name: "uncompressed 1", version: "1", serverVersion: metadata.ServerVersion, wantErr: errLegacyUncompressed},
			{name: "flat file structure", version: metadata.Version, serverVersion: "1.22", wantErr: errLegacyFlatFS},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				legacy := metadata
				legacy.Version = tc.version
				legacy.ServerVersion = tc.serverVersion
				backend := newFakeBackend()
				backend.On("GetObject", ctx, nodeHome, BackupFile).Return(marshalMeta(legacy), nil)
				backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
				bm := createManager(nil, nil, backend, nil)
				resp := bm.OnCanCommit(ctx, &req)
				assert.Contains(t, resp.Err, tc.wantErr.Error())
				assert.Equal(t, time.Duration(0), resp.Timeout)
			})
		}
	})

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		// simulate work by delaying return of SourceDataPath()
		backend.On("SourceDataPath").Return(t.TempDir()).After(time.Minute * 2)
		m := createManager(sourcer, nil, backend, nil)
		resp := m.OnCanCommit(ctx, &req)
		assert.Equal(t, resp.Err, "")
		resp = m.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, "already in progress")
		assert.Equal(t, time.Duration(0), resp.Timeout)
	})

	t.Run("Success", func(t *testing.T) {
		req := req
		req.Duration = time.Hour
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("Read", any, nodeHome, chunkKey(cls, 1), mock.Anything).Return(int64(0), nil)
		m := createManager(sourcer, nil, backend, nil)
		resp1 := m.OnCanCommit(ctx, &req)
		want1 := &CanCommitResponse{
			Method:  OpRestore,
			ID:      req.ID,
			Timeout: _TimeoutShardCommit,
		}
		assert.Equal(t, want1, resp1)
		err := m.OnCommit(ctx, &StatusRequest{Method: OpRestore, ID: req.ID, Backend: req.Backend})
		assert.Nil(t, err)
		lastStatus := m.restorer.waitForCompletion(req.Backend, req.ID, 12, 50)
		assert.Nil(t, err)
		assert.Equal(t, lastStatus.Status, backup.Success)
	})

	t.Run("Abort", func(t *testing.T) {
		req := req
		req.Duration = time.Hour
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("Read", any, nodeHome, chunkKey(cls, 1), mock.Anything).Return(int64(0), nil)
		m := createManager(sourcer, nil, backend, nil)
		resp1 := m.OnCanCommit(ctx, &req)
		want1 := &CanCommitResponse{
			Method:  OpRestore,
			ID:      req.ID,
			Timeout: _TimeoutShardCommit,
		}
		assert.Equal(t, want1, resp1)
		err := m.OnAbort(ctx, &AbortRequest{Method: OpRestore, ID: req.ID})
		assert.Nil(t, err)
		lastStatus := m.restorer.waitForCompletion(req.Backend, req.ID, 10, 50)

		assert.Nil(t, err)
		assert.Equal(t, lastStatus.Status, backup.Failed)
	})
}

func TestRestoreOnStatus(t *testing.T) {
	t.Parallel()
	var (
		backendType = "s3"
		id          = "1234"
		m           = createManager(nil, nil, nil, nil)
		ctx         = context.Background()
		starTime    = time.Now().UTC()
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = StatusRequest{
			Method:  OpRestore,
			ID:      id,
			Backend: backendType,
		}
	)
	// initial state
	got := m.OnStatus(ctx, &req)
	if !strings.Contains(got.Err, "not found") {
		t.Errorf("must return an error if backup doesn't exist")
	}
	// active state
	m.restorer.lastOp.reqState = reqState{
		Starttime: starTime,
		ID:        id,
		Status:    backup.Transferring,
		Path:      path,
	}
	got = m.OnStatus(ctx, &req)
	expected := StatusResponse{Method: OpRestore, ID: req.ID, Status: backup.Transferring}
	if expected != *got {
		t.Errorf("get active status: got=%v want=%v", got, expected)
	}
	// cached status
	m.restorer.lastOp.reset()
	st := Status{Path: path, StartedAt: starTime, Status: backup.Transferring, CompletedAt: starTime}
	m.restorer.restoreStatusMap.Store("s3/"+id, st)
	got = m.OnStatus(ctx, &req)
	if expected != *got {
		t.Errorf("fetch status from map got=%v want=%v", st, expected)
	}
}

func marshalMeta(m backup.BackupDescriptor) []byte {
	bytes, _ := json.MarshalIndent(m, "", "")
	return bytes
}

func TestRestoreAllCancellation(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		backupID = "test-backup"
		cls      = "TestClass"
	)

	t.Run("CancellationBeforeRestore", func(t *testing.T) {
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("test/path")

		restorer := newRestorer("node1", nil, sourcer, &fakeBackupBackendProvider{backend: backend}, false)
		restorer.lastOp.set(backup.Transferring)

		desc := &backup.BackupDescriptor{
			ID:            backupID,
			ServerVersion: "1.23",
			Version:       "1",
			StartedAt:     time.Now().UTC(),
			Classes: []backup.ClassDescriptor{
				{Name: cls, Shards: []*backup.ShardDescriptor{}},
			},
		}

		// Create a cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		err := restorer.restoreAll(cancelledCtx, desc, 50, nodeStore{
			objectStore: objectStore{backend: backend, backupId: backupID},
		}, "", "", false, &stagedDirs{})

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "restore cancelled")
		assert.Equal(t, backup.Cancelled, restorer.lastOp.get().Status)
	})
}

func TestWithCancellation(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		backupID = "test-backup"
	)

	t.Run("OnAbortSendsSignal", func(t *testing.T) {
		shardChan := shardSyncChan{coordChan: make(chan interface{}, 5)}
		shardChan.lastOp.reqState = reqState{ID: backupID}

		abortReq := AbortRequest{Method: OpRestore, ID: backupID}
		err := shardChan.OnAbort(ctx, &abortReq)
		assert.Nil(t, err)

		// Check that signal was sent
		select {
		case received := <-shardChan.coordChan:
			assert.Equal(t, abortReq, received)
		case <-time.After(100 * time.Millisecond):
			t.Error("abort signal should have been sent")
		}
	})
}

func TestRestoreBookingExpiration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dedupe  bool
		want    time.Duration
		booking time.Duration
	}{
		{name: "legacy clamped to shard commit timeout", booking: 10 * time.Minute, want: _TimeoutShardCommit},
		{name: "legacy below the clamp", booking: 5 * time.Second, want: 5 * time.Second},
		{name: "dedupe honors the widened booking", dedupe: true, booking: _TimeoutDedupeRestoreCanCommit + _BookingPeriod, want: _TimeoutDedupeRestoreCanCommit + _BookingPeriod},
		{name: "dedupe clamped to the widened limit", dedupe: true, booking: 10 * time.Minute, want: _TimeoutDedupeRestoreCanCommit + _BookingPeriod},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := newFakeBackend()
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/1")
			backend.On("SourceDataPath").Return(t.TempDir())
			r := newRestorer(nodeName, logrus.New(), &fakeSourcer{}, nil, false)
			store := nodeStore{objectStore{backend, "1/" + nodeName, "", "", nodeName}}
			req := &Request{Method: OpRestore, ID: "1", Duration: tc.booking, DedupeReplicas: tc.dedupe}

			ret, err := r.startRestore(req, store, func(context.Context, *stagedDirs) error { return nil })
			require.NoError(t, err)
			assert.Equal(t, tc.want, ret.Timeout)

			require.NoError(t, r.OnAbort(context.Background(), &AbortRequest{ID: req.ID}))
			require.Eventually(t, func() bool { return r.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestRestoreFailureCleansStaging(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		stage   bool
		workErr error
		wantOwn bool
	}{
		{name: "failure removes only dirs this attempt staged", stage: true, workErr: ErrAny, wantOwn: false},
		{name: "failure before staging removes nothing", workErr: ErrAny, wantOwn: false},
		{name: "success keeps staged dirs for raft apply", stage: true, wantOwn: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dataPath := t.TempDir()
			backend := newFakeBackend()
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/1")
			backend.On("SourceDataPath").Return(dataPath)
			r := newRestorer(nodeName, logrus.New(), &fakeSourcer{}, nil, false)
			store := nodeStore{objectStore{backend, "1/" + nodeName, "", "", nodeName}}

			foreign := filepath.Join(dataPath, TempDirectory, "Class-Foreign")
			require.NoError(t, os.MkdirAll(foreign, os.ModePerm))
			require.NoError(t, os.WriteFile(filepath.Join(foreign, "chunk-1"), []byte("stale"), 0o644))
			own := filepath.Join(dataPath, TempDirectory, "Class-A")

			req := &Request{Method: OpRestore, ID: "1"}
			_, err := r.startRestore(req, store, func(_ context.Context, staged *stagedDirs) error {
				if tc.stage {
					staged.record(own)
					if err := os.MkdirAll(own, os.ModePerm); err != nil {
						return err
					}
					if err := os.WriteFile(filepath.Join(own, "chunk-1"), []byte("data"), 0o644); err != nil {
						return err
					}
				}
				return tc.workErr
			})
			require.NoError(t, err)
			require.Eventually(t, func() bool { return r.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)

			_, statErr := os.Stat(foreign)
			require.NoError(t, statErr)
			_, statErr = os.Stat(own)
			if tc.wantOwn {
				require.NoError(t, statErr)
			} else {
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}

func TestRestoreFailureKeepsPriorAttemptStaging(t *testing.T) {
	t.Parallel()
	dataPath := t.TempDir()
	backend := newFakeBackend()
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/1")
	backend.On("SourceDataPath").Return(dataPath)
	r := newRestorer(nodeName, logrus.New(), &fakeSourcer{}, nil, false)
	store := nodeStore{objectStore{backend, "1/" + nodeName, "", "", nodeName}}

	staged := filepath.Join(dataPath, TempDirectory, "Class-A")
	req1 := &Request{Method: OpRestore, ID: "1"}
	_, err := r.startRestore(req1, store, func(_ context.Context, s *stagedDirs) error {
		s.record(staged)
		if err := os.MkdirAll(staged, os.ModePerm); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(staged, "chunk-1"), []byte("data"), 0o644)
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return r.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
	require.DirExists(t, staged)

	req2 := &Request{Method: OpRestore, ID: "2", Duration: 20 * time.Millisecond}
	_, err = r.startRestore(req2, store, func(context.Context, *stagedDirs) error { return nil })
	require.NoError(t, err)
	require.Eventually(t, func() bool { return r.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)

	require.DirExists(t, staged)
	require.FileExists(t, filepath.Join(staged, "chunk-1"))
}

func TestOnAbortAttemptGate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		slotAttempt string
		reqAttempt  string
		wantSignal  bool
	}{
		{name: "same attempt aborts", slotAttempt: "a1", reqAttempt: "a1", wantSignal: true},
		{name: "foreign attempt is ignored", slotAttempt: "a1", reqAttempt: "a2", wantSignal: false},
		{name: "legacy abort without attempt", slotAttempt: "a1", reqAttempt: "", wantSignal: true},
		{name: "legacy slot without attempt", slotAttempt: "", reqAttempt: "a2", wantSignal: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := shardSyncChan{coordChan: make(chan interface{}, 5), logger: logrus.New()}
			require.Empty(t, c.lastOp.renew("1", tc.slotAttempt, "p", "", ""))
			require.NoError(t, c.OnAbort(context.Background(), &AbortRequest{ID: "1", AttemptID: tc.reqAttempt}))
			assert.Equal(t, tc.wantSignal, len(c.coordChan) == 1)
		})
	}
}
