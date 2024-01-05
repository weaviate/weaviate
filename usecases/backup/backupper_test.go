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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

const (
	nodeName = "Node-1"
)

var errNotFound = backup.NewErrNotFound(errors.New("not found"))

func (r *backupper) waitForCompletion(n, ms int) backup.Status {
	for i := 0; i < n; i++ {
		time.Sleep(time.Millisecond * time.Duration(ms))
		if i < 1 {
			continue
		}
		if x := r.lastOp.get(); x.Status != "" {
			return x.Status
		}
	}
	return ""
}

func TestBackupStatus(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		id          = "1234"
		ctx         = context.Background()
		starTime    = time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC)
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		rawstatus   = string(backup.Transferring)
		want        = &models.BackupCreateStatusResponse{
			ID:      id,
			Path:    path,
			Status:  &rawstatus,
			Backend: backendName,
		}
	)

	t.Run("ActiveState", func(t *testing.T) {
		m := createManager(nil, nil, nil, nil)
		m.backupper.lastOp.reqStat = reqStat{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		st, err := m.backupper.Status(ctx, backendName, id)
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("GetBackupProvider", func(t *testing.T) {
		m := createManager(nil, nil, nil, ErrAny)
		_, err := m.backupper.Status(ctx, backendName, id)
		assert.NotNil(t, err)
	})

	t.Run("MetadataNotFound", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, ErrAny)
		backend.On("GetObject", ctx, id, BackupFile).Return(nil, ErrAny)
		m := createManager(nil, nil, backend, nil)
		_, err := m.backupper.Status(ctx, backendName, id)
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Transferring)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m := createManager(nil, nil, backend, nil)
		got, err := m.backupper.Status(ctx, backendName, id)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("ReadFromMetadataError", func(t *testing.T) {
		backend := &fakeBackend{}
		st := string(backup.Failed)
		bytes := marshalMeta(backup.BackupDescriptor{Status: st, Error: "error1"})
		want = &models.BackupCreateStatusResponse{
			ID:      id,
			Path:    path,
			Status:  &st,
			Backend: backendName,
		}
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m := createManager(nil, nil, backend, nil)
		_, err := m.backupper.Status(ctx, backendName, id)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "error1")
	})
}

func TestBackupOnStatus(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		id          = "1234"
		ctx         = context.Background()
		starTime    = time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC)
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = StatusRequest{
			Method:  OpCreate,
			ID:      id,
			Backend: backendName,
		}
	)

	t.Run("ActiveState", func(t *testing.T) {
		m := createManager(nil, nil, nil, nil)
		m.backupper.lastOp.reqStat = reqStat{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		want := &StatusResponse{
			Method: OpCreate,
			ID:     id,
			Status: backup.Transferring,
		}
		st := m.OnStatus(ctx, &req)
		assert.Equal(t, want, st)
	})

	t.Run("GetBackupProvider", func(t *testing.T) {
		want := &StatusResponse{
			Method: OpCreate,
			ID:     id,
			Status: backup.Failed,
		}
		m := createManager(nil, nil, nil, ErrAny)
		got := m.OnStatus(ctx, &req)
		assert.Contains(t, got.Err, req.Backend)
		want.Err = got.Err
		assert.Equal(t, want, got)
	})

	t.Run("MetadataNotFound", func(t *testing.T) {
		want := &StatusResponse{
			Method: OpCreate,
			ID:     id,
			Status: backup.Failed,
		}
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, ErrAny)
		backend.On("GetObject", ctx, id, BackupFile).Return(nil, ErrAny)

		m := createManager(nil, nil, backend, nil)
		got := m.OnStatus(ctx, &req)
		assert.Contains(t, got.Err, errMetaNotFound.Error())
		want.Err = got.Err
		assert.Equal(t, want, got)
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		want := &StatusResponse{
			Method: OpCreate,
			ID:     id,
			Status: backup.Success,
		}
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Success)})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m := createManager(nil, nil, backend, nil)
		got := m.OnStatus(ctx, &req)
		assert.Equal(t, want, got)
	})
}

func TestManagerCreateBackup(t *testing.T) {
	t.Parallel()
	var (
		cls         = "Class-A"
		cls2        = "Class-B"
		backendName = "gcs"
		backupID    = "1"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
	)

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}

		sourcer := &fakeSourcer{}
		// first
		sourcer.On("Backupable", ctx, req1.Include).Return(nil)
		sourcer.On("CreateBackup", ctx, any).Return(nil, nil)
		sourcer.On("ReleaseBackup", ctx, any).Return(nil)
		var ch <-chan backup.ClassDescriptor
		sourcer.On("BackupDescriptors", any, any, any).Return(ch) // just block

		backend := &fakeBackend{}
		// second
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("HomeDir", any).Return(path)
		sourcer.On("Backupable", any, req1.Include).Return(nil)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		sourcer.On("CreateBackup", ctx, any).Return(nil, ErrAny)
		sourcer.On("ReleaseBackup", ctx, any).Return(nil)
		m := createManager(sourcer, nil, backend, nil)
		resp1, err := m.Backup(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		resp2, err := m.Backup(ctx, nil, &req1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "already in progress")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Nil(t, resp2)
	})

	t.Run("InitMetadata", func(t *testing.T) {
		classes := []string{cls}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, errNotFound)

		backend.On("Initialize", ctx, nodeHome).Return(errors.New("init meta failed"))
		bm := createManager(sourcer, nil, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "init")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("Success", func(t *testing.T) {
		var (
			classes    = []string{cls}
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)
		sourcer.On("Backupable", ctx, classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)

		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, errNotFound)

		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", any, nodeHome, BackupFile, mock.Anything).Return(nil).Twice()
		backend.On("Write", any, nodeHome, any, any).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		resp, err := m.Backup(ctx, nil, &req)

		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp, want1)
		m.backupper.waitForCompletion(10, 50)
		assert.Equal(t, string(backup.Success), backend.meta.Status)
		assert.Equal(t, backend.meta.Error, "")
	})

	t.Run("PutFile", func(t *testing.T) {
		var (
			classes    = []string{cls}
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)
		sourcer.On("Backupable", ctx, classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, errNotFound)

		backend.On("Initialize", ctx, nodeHome).Return(nil)

		backend.On("Write", any, nodeHome, any, any).Return(any, ErrAny).Once()
		backend.On("PutObject", any, nodeHome, BackupFile, any).Return(nil).Once()
		m := createManager(sourcer, nil, backend, nil)

		resp, err := m.Backup(ctx, nil, &req)

		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp, want1)
		m.backupper.waitForCompletion(10, 50)

		assert.Equal(t, string(backup.Transferring), backend.meta.Status)
		assert.Contains(t, backend.meta.Error, "pipe")
	})

	t.Run("ClassDescriptor", func(t *testing.T) {
		var (
			classes     = []string{cls}
			sourcer     = &fakeSourcer{}
			sourcePath  = t.TempDir()
			errNotFound = errNotFound
			backend     = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, classes).Return(nil)
		cs := genClassDescriptions(t, sourcePath, cls, cls2)
		cs[1].Error = ErrAny
		ch := fakeBackupDescriptor(cs...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, errNotFound)

		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		m := createManager(sourcer, nil, backend, nil)

		resp, err := m.Backup(ctx, nil, &req)

		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp, want1)
		m.backupper.waitForCompletion(10, 50)
		assert.Nil(t, err)
		assert.Equal(t, backend.meta.Status, string(backup.Transferring))
		assert.Equal(t, backend.meta.Error, ErrAny.Error())
	})
}

func TestManagerCoordinatedBackup(t *testing.T) {
	t.Parallel()
	var (
		cls         = "Class-A"
		cls2        = "Class-B"
		backendName = "gcs"
		backupID    = "1"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		req         = Request{
			Method:   OpCreate,
			ID:       backupID,
			Classes:  []string{cls, cls2},
			Backend:  backendName,
			Duration: time.Millisecond * 20,
		}
		any = mock.Anything
	)

	t.Run("BackendUnregistered", func(t *testing.T) {
		backendError := errors.New("I do not exist")
		bm := createManager(nil, nil, nil, backendError)
		ret := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, ret.Err, backendName)
	})

	t.Run("ClassNotBackupable", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, req.Classes).Return(ErrAny)
		bm := createManager(sourcer, nil, backend, nil)

		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, ErrAny.Error())
		assert.Equal(t, resp.Timeout, time.Duration(0))
	})

	t.Run("InitializeBackend", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		backend.On("Initialize", ctx, nodeHome).Return(errors.New("init meta failed"))
		bm := createManager(sourcer, nil, backend, nil)

		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, "init")
		assert.Equal(t, resp.Timeout, time.Duration(0))
	})

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		// first
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, nil)
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil)
		var ch <-chan backup.ClassDescriptor
		sourcer.On("BackupDescriptors", any, any, any).Return(ch)

		backend := &fakeBackend{}
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("Initialize", ctx, mock.Anything).Return(nil)
		m := createManager(sourcer, nil, backend, nil)
		// second
		resp1 := m.OnCanCommit(ctx, &req)
		want1 := &CanCommitResponse{
			Method:  OpCreate,
			ID:      req.ID,
			Timeout: req.Duration,
		}
		assert.Equal(t, resp1, want1)
		resp := m.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, "already in progress")
		assert.Equal(t, resp.Timeout, time.Duration(0))
	})

	t.Run("Success", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{OpCreate, req.ID, _TimeoutShardCommit, ""}
		assert.Equal(t, got, want)

		err := m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName})
		assert.Nil(t, err)
		m.backupper.waitForCompletion(20, 50)
		assert.Equal(t, string(backup.Success), backend.meta.Status)
		assert.Equal(t, "", backend.meta.Error)
	})

	t.Run("AbortBeforeCommit", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)

		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{OpCreate, req.ID, _TimeoutShardCommit, ""}
		assert.Equal(t, got, want)

		err := m.OnAbort(ctx, &AbortRequest{OpCreate, req.ID, backendName})
		assert.Nil(t, err)
		m.backupper.waitForCompletion(20, 50)
		assert.Contains(t, m.backupper.lastAsyncError.Error(), "abort")
	})

	t.Run("AbortCommit", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
			m          = createManager(sourcer, nil, backend, nil)
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch).RunFn = func(a mock.Arguments) {
			m.OnAbort(ctx, &AbortRequest{OpCreate, req.ID, backendName})
			// give the abort request time to propagate
			time.Sleep(10 * time.Millisecond)
		}
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		// backend
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{OpCreate, req.ID, _TimeoutShardCommit, ""}
		assert.Equal(t, got, want)

		err := m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName})
		assert.Nil(t, err)
		m.backupper.waitForCompletion(20, 50)
		errMsg := context.Canceled.Error()
		assert.Equal(t, string(backup.Transferring), backend.meta.Status)
		assert.Equal(t, errMsg, backend.meta.Error)
		assert.Contains(t, m.backupper.lastAsyncError.Error(), errMsg)
	})

	t.Run("ExpirationTimeout", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, backupID, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Millisecond * 10
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{OpCreate, req.ID, req.Duration, ""}
		assert.Equal(t, got, want)

		m.backupper.waitForCompletion(20, 50)
		assert.Contains(t, m.backupper.lastAsyncError.Error(), "timed out")
	})
}

func genClassDescriptions(t *testing.T, sourcePath string, classes ...string) []backup.ClassDescriptor {
	ret := make([]backup.ClassDescriptor, len(classes))
	rawbytes := []byte("raw")
	subDir := filepath.Join(sourcePath, "dir1")
	if err := os.MkdirAll(subDir, os.ModePerm); err != nil {
		t.Fatalf("create test subdirectory %s: %v", subDir, err)
	}
	files := []string{"dir1/file1", "dir1/file2", "counter.txt", "version.txt", "prop.txt"}
	for _, p := range files {
		p = filepath.Join(sourcePath, p)
		if err := os.WriteFile(p, rawbytes, os.ModePerm); err != nil {
			t.Fatalf("create test file %s: %v", p, err)
		}
	}

	for i, cls := range classes {
		ret[i] = backup.ClassDescriptor{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
			Shards: []*backup.ShardDescriptor{
				{
					Name: "Shard1", Node: "Node-1",
					Files:                 files[0:2],
					DocIDCounterPath:      files[2],
					ShardVersionPath:      files[3],
					PropLengthTrackerPath: files[4],
					DocIDCounter:          rawbytes,
					Version:               rawbytes,
					PropLengthTracker:     rawbytes,
				},
			},
		}
	}
	return ret
}

func fakeBackupDescriptor(descs ...backup.ClassDescriptor) <-chan backup.ClassDescriptor {
	ch := make(chan backup.ClassDescriptor, len(descs))
	go func() {
		for _, cls := range descs {
			ch <- cls
		}
		close(ch)
	}()

	return ch
}

func createManager(sourcer Sourcer, schema schemaManger, backend modulecapabilities.BackupBackend, backendErr error) *Handler {
	backends := &fakeBackupBackendProvider{backend, backendErr}
	if sourcer == nil {
		sourcer = &fakeSourcer{}
	}
	if schema == nil {
		schema = &fakeSchemaManger{nodeName: nodeName}
	}

	logger, _ := test.NewNullLogger()
	return NewHandler(logger, &fakeAuthorizer{}, schema, sourcer, backends)
}
