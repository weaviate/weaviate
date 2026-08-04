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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
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
		if x := r.lastOp.get(); x.Status == backup.Success || x.Status == backup.Failed || x.Status == backup.Cancelled {
			return x.Status
		}
	}
	return ""
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
		m.backupper.lastOp.reqState = reqState{
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
		bytes := marshalMeta(backup.BackupDescriptor{Status: backup.Success})
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		m := createManager(nil, nil, backend, nil)
		got := m.OnStatus(ctx, &req)
		assert.Equal(t, want, got)
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
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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
		sourcer.On("BackupDescriptors", any, any, any, any).Return(ch)

		backend := &fakeBackend{}
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, backup.ErrNotFound{})
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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
		// The commit-time re-resolve runs on the uploader's own cancellable ctx.
		sourcer.On("Backupable", any, any).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: _TimeoutShardCommit}
		assert.Equal(t, got, want)

		err := m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName, "", "", ""})
		assert.Nil(t, err)
		m.backupper.waitForCompletion(20, 50)
		status, errMsg := backend.getMetaStatus()
		assert.Equal(t, backup.Success, status)
		assert.Equal(t, "", errMsg)
	})

	t.Run("RoleSelectionReachesTheSnapshotter", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
			roles      = []string{"ns1:reader", "ns1:writer"}
			users      = []string{"ns1:alice"}
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m, rbacSnapshotter, dynUserSnapshotter := createManagerWithSnapshotters(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		req.Roles = roles
		req.Users = users
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: _TimeoutShardCommit}
		require.Equal(t, want, got)

		require.NoError(t, m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName, "", "", ""}))
		m.backupper.waitForCompletion(20, 50)
		status, errMsg := backend.getMetaStatus()
		require.Equal(t, backup.Success, status)
		require.Equal(t, "", errMsg)

		// The node must snapshot the requested roles and nothing else. An empty
		// selection here means this node dumped the whole RBAC store while the
		// descriptor still advertised the subset the caller asked for.
		gotRoles, calledRbac := rbacSnapshotter.snapshotted()
		require.True(t, calledRbac, "the participant never took an RBAC snapshot")
		assert.Equal(t, roles, gotRoles)

		gotUsers, calledDynUser := dynUserSnapshotter.snapshotted()
		require.True(t, calledDynUser, "the participant never took a dynamic user snapshot")
		assert.Equal(t, users, gotUsers)
	})

	t.Run("NodeMissingFromBaseBackupUploadsFull", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
			baseID     = "base-1"
			baseHome   = baseID + "/" + nodeName
		)

		// capture the base descriptors passed to the uploader to prove this
		// node deduplicates against nothing (full upload)
		var gotBaseDescrs []*backup.BackupDescriptor
		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		// The commit-time re-resolve runs on the uploader's own cancellable ctx.
		sourcer.On("Backupable", any, any).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch).Run(func(a mock.Arguments) {
			gotBaseDescrs = a.Get(3).([]*backup.BackupDescriptor)
		})
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		// this node has no descriptor in the base backup
		backend.On("GetObject", any, baseHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		req.BaseBackupID = baseID
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: _TimeoutShardCommit}
		assert.Equal(t, got, want)

		err := m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName, "", "", ""})
		assert.NoError(t, err)
		m.backupper.waitForCompletion(20, 50)
		status, errMsg := backend.getMetaStatus()
		assert.Equal(t, backup.Success, status)
		assert.Equal(t, "", errMsg)
		assert.Empty(t, gotBaseDescrs)
		// the descriptor must reflect the full backup we took, not advertise a base
		assert.Empty(t, backend.getMetaBaseBackupID())
	})

	t.Run("AbortBeforeCommit", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)

		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: _TimeoutShardCommit}
		assert.Equal(t, got, want)

		err := m.OnAbort(ctx, &AbortRequest{OpCreate, req.ID, backendName, "", "", ""})
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
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch).RunFn = func(a mock.Arguments) {
			m.OnAbort(ctx, &AbortRequest{OpCreate, req.ID, backendName, "", "", ""})
			// give the abort request time to propagate
			time.Sleep(10 * time.Millisecond)
		}
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		// backend
		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, nodeHome, mock.Anything, mock.Anything).Return(any, nil)

		req := req
		req.Duration = time.Hour
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: _TimeoutShardCommit}
		assert.Equal(t, got, want)

		err := m.OnCommit(ctx, &StatusRequest{OpCreate, req.ID, backendName, "", "", ""})
		assert.Nil(t, err)
		m.backupper.waitForCompletion(20, 50)
		status, metaErr := backend.getMetaStatus()
		assert.Equal(t, backup.Cancelled, status)
		wantErr := context.Canceled.Error()
		assert.Equal(t, wantErr, metaErr)
		assert.Contains(t, m.backupper.lastAsyncError.Error(), wantErr)
	})

	t.Run("ExpirationTimeout", func(t *testing.T) {
		var (
			sourcePath = t.TempDir()
			sourcer    = &fakeSourcer{}
			backend    = newFakeBackend()
		)

		sourcer.On("Backupable", ctx, req.Classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
		sourcer.On("BackupDescriptors", any, backupID, mock.Anything, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)

		backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		backend.On("SourceDataPath").Return(sourcePath)
		backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
		backend.On("Initialize", ctx, nodeHome).Return(nil)
		backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(nil).Once()
		backend.On("Write", mock.Anything, backupID, mock.Anything, mock.Anything).Return(any, nil)
		m := createManager(sourcer, nil, backend, nil)

		req := req
		req.Duration = time.Millisecond * 10
		got := m.OnCanCommit(ctx, &req)
		want := &CanCommitResponse{Method: OpCreate, ID: req.ID, Timeout: req.Duration}
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
	m, _, _ := createManagerWithSnapshotters(sourcer, schema, backend, backendErr)
	return m
}

// createManagerWithSnapshotters also returns the two snapshot fakes the handler was
// built with, so a test can assert on the selection that reached them.
func createManagerWithSnapshotters(sourcer Sourcer, schema schemaManger, backend modulecapabilities.BackupBackend, backendErr error) (*Handler, *fakeRbacBackupWrapper, *fakeDynUserBackupWrapper) {
	backends := &fakeBackupBackendProvider{backend, backendErr}
	if sourcer == nil {
		sourcer = &fakeSourcer{}
	}
	if schema == nil {
		schema = &fakeSchemaManger{nodeName: nodeName}
	}

	logger, _ := test.NewNullLogger()
	rbac := &fakeRbacBackupWrapper{}
	dynUser := &fakeDynUserBackupWrapper{}
	return NewHandler(logger, config.Backup{}, mocks.NewMockAuthorizer(), schema, sourcer, backends, rbac, dynUser), rbac, dynUser
}

// fakeRbacBackupWrapper satisfies RBACSnapshotter (variadic Snapshot). It records the
// selection it was called with. Discarding that argument would leave every hop between
// the participant request and this call unpinned, so a dropped selection would still
// produce a green suite while each node snapshotted the whole RBAC store.
type fakeRbacBackupWrapper struct {
	mu       sync.Mutex
	gotRoles []string
	called   bool
}

func (r *fakeRbacBackupWrapper) Snapshot(roles ...string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.called = true
	r.gotRoles = roles
	return nil, nil
}

func (r *fakeRbacBackupWrapper) Restore([]byte, bool) error {
	return nil
}

// snapshotted returns the recorded selection and whether Snapshot ran at all. The
// backup runs on its own goroutine, so the lock is required even after waiting.
func (r *fakeRbacBackupWrapper) snapshotted() ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gotRoles, r.called
}

// fakeDynUserBackupWrapper satisfies dynUserSnapshotter (variadic Snapshot). It records
// its selection for the same reason fakeRbacBackupWrapper does.
type fakeDynUserBackupWrapper struct {
	mu       sync.Mutex
	gotUsers []string
	called   bool
}

func (d *fakeDynUserBackupWrapper) Snapshot(userIDs ...string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.called = true
	d.gotUsers = userIDs
	return nil, nil
}

func (d *fakeDynUserBackupWrapper) Restore([]byte, bool) error {
	return nil
}

func (d *fakeDynUserBackupWrapper) snapshotted() ([]string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gotUsers, d.called
}

func TestResolveBaseBackupChain(t *testing.T) {
	ctx := context.Background()
	gzipCompression := backup.CompressionGZIP
	bucket := "test-bucket"
	path := "test-path"

	// t0 is the oldest; a valid chain has strictly increasing StartedAt from the
	// full backup up to the backup that depends on it (childStartedAt).
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t0.Add(2 * time.Hour)
	t3 := t0.Add(3 * time.Hour)
	t4 := t0.Add(4 * time.Hour)

	type fetchMetaFunc func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error)

	tests := []struct {
		name              string
		baseBackupID      string
		childStartedAt    time.Time
		compressionType   backup.CompressionType
		setupFetchMeta    func() fetchMetaFunc
		errorContains     []string
		expectedResultIDs []string
	}{
		{
			name:            "EmptyBaseBackupID",
			baseBackupID:    "",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					t.Fatal("fetchMeta should not be called for empty base backup ID")
					return nil, nil
				}
			},
			expectedResultIDs: nil,
		},
		{
			name:            "SingleBaseBackup",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t0,
					}, nil
				}
			},
			expectedResultIDs: []string{"backup-1"},
		},
		{
			name:            "ChainOfMultipleBackups",
			baseBackupID:    "backup-3",
			childStartedAt:  t4,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				descriptors := map[string]*backup.BackupDescriptor{
					"backup-1": {
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t0,
					},
					"backup-2": {
						ID:              "backup-2",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-1",
						Status:          backup.Success,
						StartedAt:       t1,
					},
					"backup-3": {
						ID:              "backup-3",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-2",
						Status:          backup.Success,
						StartedAt:       t2,
					},
				}
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return descriptors[backupID], nil
				}
			},
			expectedResultIDs: []string{"backup-3", "backup-2", "backup-1"},
		},
		{
			// Base re-created after its dependent: same id, newer StartedAt.
			name:            "BaseNewerThanChild",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t2,
					}, nil
				}
			},
			errorContains: []string{"base backup \"backup-1\"", "is not older", "re-created"},
		},
		{
			// Equal StartedAt is rejected: the base must be strictly older.
			name:            "BaseEqualToChild",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t1,
					}, nil
				}
			},
			errorContains: []string{"base backup \"backup-1\"", "is not older"},
		},
		{
			// A deeper link was re-created: backup-2 is newer than backup-3 which depends on it.
			name:            "StaleBaseMidChain",
			baseBackupID:    "backup-3",
			childStartedAt:  t4,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				descriptors := map[string]*backup.BackupDescriptor{
					"backup-1": {
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t0,
					},
					"backup-2": {
						ID:              "backup-2",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-1",
						Status:          backup.Success,
						StartedAt:       t3,
					},
					"backup-3": {
						ID:              "backup-3",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-2",
						Status:          backup.Success,
						StartedAt:       t2,
					},
				}
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return descriptors[backupID], nil
				}
			},
			errorContains: []string{"base backup \"backup-2\"", "is not older"},
		},
		{
			name:            "CircularReferenceDetection",
			baseBackupID:    "backup-1",
			childStartedAt:  t2,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				descriptors := map[string]*backup.BackupDescriptor{
					"backup-1": {
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-2",
						Status:          backup.Success,
						StartedAt:       t1,
					},
					"backup-2": {
						ID:              "backup-2",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-1",
						Status:          backup.Success,
						StartedAt:       t0,
					},
				}
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return descriptors[backupID], nil
				}
			},
			errorContains: []string{"circular references in backup ids detected", "backup-1"},
		},
		{
			name:            "ErrorFetchingBackup",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return nil, errors.New("network error")
				}
			},
			errorContains: []string{"could not fetch base backup", "network error"},
		},
		{
			name:            "SelfReferentialBackup",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "backup-1",
						Status:          backup.Success,
						StartedAt:       t0,
					}, nil
				}
			},
			errorContains: []string{"circular references in backup ids detected"},
		},
		{
			name:            "WrongCompressionType",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				noneCompression := backup.CompressionNone
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &noneCompression,
						BaseBackupID:    "",
						Status:          backup.Success,
						StartedAt:       t0,
					}, nil
				}
			},
			errorContains: []string{"backup \"backup-1\" has compression type", "expected"},
		},
		{
			name:            "LegacyBaseByStructureVersion",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						Version:         "1.0",
						ServerVersion:   "1.23",
						Status:          backup.Success,
						StartedAt:       t0,
					}, nil
				}
			},
			errorContains: []string{"base backup \"backup-1\"", "older than v1.21"},
		},
		{
			name:            "LegacyBaseDeeperInChain",
			baseBackupID:    "backup-2",
			childStartedAt:  t2,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				descriptors := map[string]*backup.BackupDescriptor{
					"backup-1": {
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						Version:         Version,
						ServerVersion:   "1.22",
						Status:          backup.Success,
						StartedAt:       t0,
					},
					"backup-2": {
						ID:              "backup-2",
						CompressionType: &gzipCompression,
						Version:         Version,
						ServerVersion:   "1.23",
						BaseBackupID:    "backup-1",
						Status:          backup.Success,
						StartedAt:       t1,
					},
				}
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return descriptors[backupID], nil
				}
			},
			errorContains: []string{"base backup \"backup-1\"", "older than v1.23"},
		},
		{
			name:            "FailedBackupStatus",
			baseBackupID:    "backup-1",
			childStartedAt:  t1,
			compressionType: gzipCompression,
			setupFetchMeta: func() fetchMetaFunc {
				return func(ctx context.Context, backupID, bucket, path string) (*backup.BackupDescriptor, error) {
					return &backup.BackupDescriptor{
						ID:              "backup-1",
						CompressionType: &gzipCompression,
						BaseBackupID:    "",
						Status:          backup.Failed,
						StartedAt:       t0,
					}, nil
				}
			},
			errorContains: []string{"backup \"backup-1\" has status", "expected"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetchMeta := tt.setupFetchMeta()

			result, err := resolveBaseBackupChain(ctx, tt.baseBackupID, tt.childStartedAt, bucket, path, tt.compressionType, fetchMeta)

			if len(tt.errorContains) > 0 {
				assert.Error(t, err)
				assert.Nil(t, result)
				for _, errMsg := range tt.errorContains {
					assert.Contains(t, err.Error(), errMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.expectedResultIDs == nil {
					assert.Nil(t, result)
				} else {
					assert.Len(t, result, len(tt.expectedResultIDs))
					for i, expectedID := range tt.expectedResultIDs {
						assert.Equal(t, expectedID, result[i].ID)
					}
				}
			}
		})
	}
}

// Pins: a reindex admitted after the pre-capture check still fails the
// backup at commit time.
func TestBackupFailsWhenAReindexIsLiveAtCommitTime(t *testing.T) {
	t.Parallel()
	var (
		cls         = "Class-A"
		cls2        = "Class-B"
		backendName = "gcs"
		backupID    = "1"
		ctx         = context.Background()
		nodeHome    = backupID + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		any         = mock.Anything
		req         = Request{
			Method:   OpCreate,
			ID:       backupID,
			Classes:  []string{cls, cls2},
			Backend:  backendName,
			Duration: time.Millisecond * 20,
		}
	)

	tests := []struct {
		name        string
		liveAtEnd   bool
		wantStatus  backup.Status
		wantErrPart string
	}{
		{
			name:       "no migration appears; the backup stands",
			liveAtEnd:  false,
			wantStatus: backup.Success,
		},
		{
			name:        "a migration became visible during capture",
			liveAtEnd:   true,
			wantStatus:  backup.Failed,
			wantErrPart: "a runtime-reindex was live while this backup was captured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sourcePath := t.TempDir()
			sourcer := &fakeSourcer{}
			backend := newFakeBackend()

			// The admission check passes; only the commit-time one sees the task.
			sourcer.On("Backupable", ctx, req.Classes).Return(nil).Once()
			if tc.liveAtEnd {
				sourcer.On("Backupable", any, any).Return(
					fmt.Errorf("Node-1/%s: %w: shard %q has 1 active tracker(s)",
						cls, backup.ErrBackupBlockedByInFlightReindex, "shard-a"))
			} else {
				sourcer.On("Backupable", any, any).Return(nil)
			}
			ch := fakeBackupDescriptor(genClassDescriptions(t, sourcePath, cls, cls2)...)
			sourcer.On("BackupDescriptors", any, backupID, any, any).Return(ch)
			sourcer.On("ReleaseBackup", any, backupID, any).Return(nil)

			backend.On("HomeDir", any, any, any).Return(path)
			backend.On("SourceDataPath").Return(sourcePath)
			backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
			backend.On("Initialize", ctx, nodeHome).Return(nil)
			backend.On("PutObject", any, nodeHome, BackupFile, any).Return(nil)
			backend.On("Write", any, nodeHome, any, any).Return(any, nil)

			m := createManager(sourcer, nil, backend, nil)
			longReq := req
			longReq.Duration = time.Hour
			require.Equal(t, &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: _TimeoutShardCommit},
				m.OnCanCommit(ctx, &longReq))
			require.NoError(t, m.OnCommit(ctx, &StatusRequest{OpCreate, backupID, backendName, "", "", ""}))
			m.backupper.waitForCompletion(50, 100)

			status, errMsg := backend.getMetaStatus()
			require.Equal(t, tc.wantStatus, status)
			if tc.wantErrPart == "" {
				require.Empty(t, errMsg)
				return
			}
			require.Contains(t, errMsg, tc.wantErrPart)
			require.Contains(t, errMsg, backup.ErrBackupBlockedByInFlightReindex.Error(),
				"the operator needs to know which condition failed the backup")
		})
	}
}
