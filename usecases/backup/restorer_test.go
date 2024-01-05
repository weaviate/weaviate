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

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// ErrAny represent a random error
var (
	ErrAny = errors.New("any error")
	any    = mock.Anything
)

func (r *restorer) Restore(ctx context.Context,
	req *Request,
	desc *backup.BackupDescriptor,
	store nodeStore,
) (*models.BackupRestoreResponse, error) {
	status := string(backup.Started)
	returnData := &models.BackupRestoreResponse{
		Classes: req.Classes,
		ID:      req.ID,
		Backend: req.Backend,
		Status:  &status,
		Path:    store.HomeDir(),
	}
	if _, err := r.restore(ctx, req, desc, store); err != nil {
		return nil, err
	}
	return returnData, nil
}

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
	m.restorer.lastOp.reqStat = reqStat{
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

func TestRestoreRequestValidation(t *testing.T) {
	var (
		cls         = "MyClass"
		backendName = "s3"
		rawbytes    = []byte("hello")
		id          = "1234"
		timept      = time.Now().UTC()
		ctx         = context.Background()
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		}
	)
	meta := backup.BackupDescriptor{
		ID:            id,
		StartedAt:     timept,
		Version:       "1",
		ServerVersion: "1",
		Status:        string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
		}},
	}

	t.Run("BackendFailure", func(t *testing.T) { //  backend provider fails
		backend := newFakeBackend()
		m2 := createManager(nil, nil, backend, ErrAny)
		_, err := m2.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
	})

	t.Run("GetMetadataFile", func(t *testing.T) {
		backend := newFakeBackend()
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, ErrAny)
		backend.On("GetObject", ctx, req.ID, BackupFile).Return(nil, ErrAny)

		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		backend = newFakeBackend()
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("GetObject", ctx, req.ID, BackupFile).Return(nil, backup.ErrNotFound{})

		m3 := createManager(nil, nil, backend, nil)

		_, err = m3.Restore(ctx, nil, req)
		if _, ok := err.(backup.ErrNotFound); !ok {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	})

	t.Run("FailedBackup", func(t *testing.T) {
		backend := newFakeBackend()
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Failed)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backup.Failed)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("BackupWithHigherVersion", func(t *testing.T) {
		var (
			backend = newFakeBackend()
			meta    = backup.BackupDescriptor{
				ID:            id,
				StartedAt:     timept,
				Version:       "3.0",
				ServerVersion: "1",
				Status:        string(backup.Success),
				Classes: []backup.ClassDescriptor{{
					Name: cls, Schema: rawbytes, ShardingState: rawbytes,
				}},
			}
			bytes = marshalMeta(meta)
		)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), errMsgHigherVersion)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("FailedOldBackup", func(t *testing.T) {
		backend := newFakeBackend()
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Failed)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, ErrAny)
		backend.On("GetObject", ctx, id, BackupFile).Return(bytes, nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backup.Failed)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("CorruptedBackupFile", func(t *testing.T) {
		backend := newFakeBackend()
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Success)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "corrupted")
	})

	t.Run("WrongBackupFile", func(t *testing.T) {
		backend := newFakeBackend()
		bytes := marshalMeta(backup.BackupDescriptor{ID: "123", Status: string(backup.Success)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "wrong backup file")
	})

	t.Run("UnknownClass", func(t *testing.T) {
		backend := newFakeBackend()
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown")
	})

	t.Run("EmptyResultClassList", func(t *testing.T) { //  backup was successful but class list is empty
		backend := newFakeBackend()
		bytes := marshalMeta(meta)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m2 := createManager(nil, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
}

func TestManagerRestoreBackup(t *testing.T) {
	var (
		cls         = "DemoClass"
		backendName = "gcs"
		backupID    = "1"
		rawbytes    = []byte("hello")
		timept      = time.Now().UTC()
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
	)
	meta2 := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     timept,
		Version:       Version,
		ServerVersion: "1",
		Status:        string(backup.Success),

		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
			Chunks: map[int32][]string{1: {"Shard1"}},
			Shards: []*backup.ShardDescriptor{
				{
					Name: "Shard1", Node: "Node-1",
					Chunk: 1,
				},
			},
		}},
	}
	meta1 := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     timept,
		Version:       version1,
		ServerVersion: "1",
		Status:        string(backup.Success),

		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
			Chunks: map[int32][]string{1: {"Shard1"}},
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
					Chunk:                 1,
				},
			},
		}},
	}

	t.Run("ClassAlreadyExists", func(t *testing.T) {
		var (
			req1 = BackupRequest{
				ID:      backupID,
				Include: []string{cls},
				Backend: backendName,
			}
			backend  = newFakeBackend()
			sourcer  = &fakeSourcer{}
			dataPath = t.TempDir()
		)
		sourcer.On("ClassExists", cls).Return(true)
		bytes := marshalMeta(meta2)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(dataPath)
		backend.On("Read", any, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		lastStatus := m.restorer.waitForCompletion(req1.Backend, req1.ID, 10, 50)
		assert.Nil(t, err)
		assert.Equal(t, backup.Failed, lastStatus.Status)
	})

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(meta2)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		// simulate work by delaying return of SourceDataPath()
		backend.On("SourceDataPath").Return(t.TempDir()).After(time.Hour)
		m2 := createManager(sourcer, nil, backend, nil)
		_, err := m2.Restore(ctx, nil, &BackupRequest{ID: backupID})
		assert.Nil(t, err)
		m := createManager(sourcer, nil, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		// another caller
		resp2, err := m.Restore(ctx, nil, &req1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "already in progress")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Nil(t, resp2)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			req1 = BackupRequest{
				ID:      backupID,
				Include: []string{cls},
				Backend: backendName,
			}
			backend  = newFakeBackend()
			sourcer  = &fakeSourcer{}
			dataPath = t.TempDir()
		)
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(meta2)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(dataPath)
		backend.On("Read", any, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		assert.Nil(t, err)
		lastStatus := m.restorer.waitForCompletion(req1.Backend, req1.ID, 10, 50)
		assert.Equal(t, backup.Success, lastStatus.Status)
	})

	readSourceFile := func(t *testing.T, newVerion bool) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
		var bytes []byte
		if newVerion {
			bytes = marshalMeta(meta2)
			backend.On("Read", any, nodeHome, mock.Anything, mock.Anything).Return(any, ErrAny)
		} else {
			bytes = marshalMeta(meta1)
			backend.On("WriteToFile", any, nodeHome, mock.Anything, mock.Anything).Return(ErrAny)
		}

		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		m := createManager(sourcer, nil, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		lastStatus := m.restorer.waitForCompletion(req1.Backend, req1.ID, 10, 50)

		assert.Nil(t, err)
		assert.Equal(t, backup.Failed, lastStatus.Status)
		//
	}
	t.Run("ReadSourceFile", func(t *testing.T) {
		readSourceFile(t, true)
	})
	t.Run("ReadSourceFileV1", func(t *testing.T) {
		readSourceFile(t, false)
	})

	t.Run("RestoreClassFails", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		schema := fakeSchemaManger{errRestoreClass: ErrAny, nodeName: nodeName}
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(meta2)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("Read", any, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, &schema, backend, nil)
		resp1, err := m.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		lastStatus := m.restorer.waitForCompletion(req1.Backend, req1.ID, 10, 50)

		assert.Nil(t, err)
		assert.Equal(t, lastStatus.Status, backup.Failed)
	})
}

func TestManagerCoordinatedRestore(t *testing.T) {
	var (
		backendName = "gcs"
		rawbytes    = []byte("hello")
		timept      = time.Now().UTC()
		cls         = "Class-A"
		backupID    = "2"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = Request{
			Method:   OpRestore,
			ID:       backupID,
			Classes:  []string{cls},
			Backend:  backendName,
			Duration: time.Millisecond * 20,
		}
	)

	metadata := backup.BackupDescriptor{
		ID:            backupID,
		StartedAt:     timept,
		Version:       "1",
		ServerVersion: "1",
		Status:        string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
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
		backend.On("HomeDir", mock.Anything).Return(path)
		bm := createManager(nil, nil, backend, nil)
		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, errMetaNotFound.Error())
		assert.Equal(t, resp.Timeout, time.Duration(0))
	})

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
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
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("WriteToFile", any, nodeHome, mock.Anything, mock.Anything).Return(nil)
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
		lastStatus := m.restorer.waitForCompletion(req.Backend, req.ID, 10, 50)
		assert.Nil(t, err)
		assert.Equal(t, lastStatus.Status, backup.Success)
	})

	t.Run("Abort", func(t *testing.T) {
		req := req
		req.Duration = time.Hour
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(t.TempDir())
		backend.On("WriteToFile", any, nodeHome, mock.Anything, mock.Anything).Return(nil)
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
	m.restorer.lastOp.reqStat = reqStat{
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
