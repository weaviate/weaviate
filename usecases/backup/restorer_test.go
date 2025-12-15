//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
		Version:       "1",
		ServerVersion: "1",
		Status:        string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: rawShardingStateBytes,
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

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		backend := newFakeBackend()
		sourcer := &fakeSourcer{}
		sourcer.On("ClassExists", cls).Return(false)
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
		sourcer.On("ClassExists", cls).Return(false)
		bytes := marshalMeta(metadata)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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
		lastStatus := m.restorer.waitForCompletion(req.Backend, req.ID, 12, 50)
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
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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
