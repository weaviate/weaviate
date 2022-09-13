//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBackupStatus(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		id          = "1234"
		ctx         = context.Background()
		starTime    = time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC)
		path        = "bucket/backups/123"
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
		m.backupper.lastBackup.reqStat = reqStat{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			path:      path,
		}
		st, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("GetBackupProvider", func(t *testing.T) {
		m := createManager(nil, nil, nil, ErrAny)
		_, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.NotNil(t, err)
	})

	t.Run("MetdataNotFound", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, ErrAny)
		m := createManager(nil, nil, backend, nil)
		_, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Transferring)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m := createManager(nil, nil, backend, nil)
		got, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
}

func TestBackupRequestValidation(t *testing.T) {
	t.Parallel()
	var (
		cls         = "MyClass"
		backendName = "s3"
		m           = createManager(nil, nil, nil, nil)
		ctx         = context.Background()
		id          = "123"
		path        = "root/123"
	)
	t.Run("ValidateEmptyID", func(t *testing.T) {
		_, err := m.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "",
			Include: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ValidateID", func(t *testing.T) {
		_, err := m.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "A*:",
			Include: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("IncludeExclude", func(t *testing.T) {
		_, err := m.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{cls},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ResultingClassListIsEmpty", func(t *testing.T) {
		// return one class and exclude it in the request
		sourcer := &fakeSourcer{}
		sourcer.On("ListBackupable").Return([]string{cls})
		m = createManager(sourcer, nil, nil, nil)
		_, err := m.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ClassNotBackupable", func(t *testing.T) {
		// return an error in case index doesn't exist or a shard has multiple nodes
		sourcer := &fakeSourcer{}
		sourcer.On("ListBackupable").Return([]string{cls})
		sourcer.On("Backupable", ctx, []string{cls}).Return(ErrAny)
		m = createManager(sourcer, nil, nil, nil)
		_, err := m.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{},
		})
		assert.NotNil(t, err)
	})
	t.Run("GetMetadataFails", func(t *testing.T) {
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, []string{cls}).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, errors.New("can not be read"))
		bm := createManager(sourcer, nil, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("backup %s already exists", id))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
	t.Run("MetadataNotFound", func(t *testing.T) {
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, []string{cls}).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		bm := createManager(sourcer, nil, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("backup %s already exists", id))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
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
		path        = "dst/path"
		req         = BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
	)

	t.Run("BackendUnregistered", func(t *testing.T) {
		classes := []string{cls}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)

		backendError := errors.New("I do not exist")
		bm := createManager(sourcer, nil, nil, backendError)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}

		sourcer := &fakeSourcer{}
		// first
		sourcer.On("Backupable", ctx, req1.Include).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, nil)
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil)
		var ch <-chan backup.ClassDescriptor
		sourcer.On("BackupDescriptors", ctx, mock.Anything, mock.Anything).Return(ch)

		backend := &fakeBackend{}
		// first
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.ErrNotFound{})
		backend.On("HomeDir", mock.Anything).Return(path)
		sourcer.On("Backupable", ctx, req1.Include).Return(nil)
		backend.On("Initialize", ctx, mock.Anything).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, ErrAny)
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil)
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
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(errors.New("init meta failed"))
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
		classes := []string{cls}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(cls, cls2)...)
		sourcer.On("BackupDescriptors", ctx, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(nil)
		backend.On("PutObject", mock.Anything, backupID, MetaDataFilename, mock.Anything).Return(nil).Once()
		backend.On("PutFile", mock.Anything, backupID, mock.Anything, mock.Anything).Return(nil)
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
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			if i > 0 && m.backupper.lastBackup.get().Status == "" {
				break
			}
		}
		assert.Nil(t, err)
		assert.Nil(t, err)
		assert.Equal(t, backend.meta.Status, string(backup.Success))
		assert.Equal(t, backend.meta.Error, "")
	})

	t.Run("PutFile", func(t *testing.T) {
		classes := []string{cls}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		ch := fakeBackupDescriptor(genClassDescriptions(cls, cls2)...)
		sourcer.On("BackupDescriptors", ctx, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(nil)
		backend.On("PutFile", mock.Anything, backupID, mock.Anything, mock.Anything).Return(ErrAny).Once()
		backend.On("PutObject", mock.Anything, backupID, MetaDataFilename, mock.Anything).Return(nil).Once()
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
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			if i > 0 && m.backupper.lastBackup.get().Status == "" {
				break
			}
		}
		assert.Nil(t, err)
		assert.Equal(t, backend.meta.Status, string(backup.Transferring))
		assert.Equal(t, backend.meta.Error, ErrAny.Error())
	})

	t.Run("ClassDescriptor", func(t *testing.T) {
		classes := []string{cls}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		cs := genClassDescriptions(cls, cls2)
		cs[1].Error = ErrAny
		ch := fakeBackupDescriptor(cs...)
		sourcer.On("BackupDescriptors", ctx, backupID, mock.Anything).Return(ch)
		sourcer.On("ReleaseBackup", ctx, backupID, mock.Anything).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(nil)
		backend.On("PutFile", mock.Anything, backupID, mock.Anything, mock.Anything).Return(nil)
		backend.On("PutObject", mock.Anything, backupID, MetaDataFilename, mock.Anything).Return(nil).Once()
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
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			if i > 0 && m.backupper.lastBackup.get().Status == "" {
				break
			}
		}
		assert.Nil(t, err)
		assert.Equal(t, backend.meta.Status, string(backup.Transferring))
		assert.Equal(t, backend.meta.Error, ErrAny.Error())
	})
}

func genClassDescriptions(classes ...string) []backup.ClassDescriptor {
	ret := make([]backup.ClassDescriptor, len(classes))
	rawbytes := []byte("raw")
	for i, cls := range classes {
		ret[i] = backup.ClassDescriptor{
			Name: cls, Schema: rawbytes, ShardingState: rawbytes,
			Shards: []backup.ShardDescriptor{
				{
					Name: "Shard1", Node: "Node-1",
					Files:                 []string{"dir1/file1", "dir2/file2"},
					DocIDCounterPath:      "dir1/counter.txt",
					ShardVersionPath:      "dir1/version.txt",
					PropLengthTrackerPath: "dir1/prop.txt",
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

func createManager(sourcer Sourcer, schema schemaManger, backend modulecapabilities.BackupBackend, backendErr error) *Manager {
	backends := &fakeBackupBackendProvider{backend, backendErr}
	if sourcer == nil {
		sourcer = &fakeSourcer{}
	}
	if schema == nil {
		schema = &fakeSchemaManger{}
	}

	logger, _ := test.NewNullLogger()
	return NewManager(logger, &fakeAuthorizer{}, schema, sourcer, backends)
}
