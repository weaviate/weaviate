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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBackStatus(t *testing.T) {
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

	t.Run("get active state", func(t *testing.T) {
		m := createManager(nil, nil, nil)
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

	t.Run("get backup provider", func(t *testing.T) {
		m := createManager(nil, nil, ErrAny)
		_, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.NotNil(t, err)
	})

	t.Run("metdata not found", func(t *testing.T) {
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(nil, ErrAny)
		m := createManager(nil, backend, nil)
		_, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("read status from metdata", func(t *testing.T) {
		backend := &fakeBackend{}
		bytes := marshalMeta(backup.BackupDescriptor{Status: string(backup.Transferring)})
		backend.On("GetObject", ctx, id, MetaDataFilename).Return(bytes, nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		m := createManager(nil, backend, nil)
		got, err := m.BackupStatus(ctx, nil, backendName, id)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
}

func TestBackupRequestValidation(t *testing.T) {
	var (
		cls     = "MyClass"
		backend = "s3"
		m       = createManager(nil, nil, nil)
		ctx     = context.Background()
	)
	_, err := m.Backup(ctx, nil, &BackupRequest{
		Backend: backend,
		ID:      "A*:",
		Include: []string{cls},
	})
	if err == nil {
		t.Errorf("must return an error for an invalid id")
	}
	_, err = m.Backup(ctx, nil, &BackupRequest{
		Backend: backend,
		ID:      "1234",
		Include: []string{cls},
		Exclude: []string{cls},
	})
	if err == nil {
		t.Errorf("must return an error for non empty include and exclude")
	}
	// return one class and exclude it in the request
	sourcer := &fakeSourcer{}
	sourcer.On("ListBackupable").Return([]string{cls})

	m2 := createManager(sourcer, nil, nil)
	_, err = m2.Backup(ctx, nil, &BackupRequest{
		Backend: backend,
		ID:      "1234",
		Include: []string{},
		Exclude: []string{cls},
	})
	if err == nil {
		t.Errorf("must return an error if the resulting list of classes is empty")
	}
}

func TestBackupManager_CreateBackup(t *testing.T) {
	className := "DemoClass"
	className2 := "DemoClass2"
	backendName := "DemoBackend"
	backupID := "backup-id"
	backupID2 := "backup-id2"
	ctx := context.Background()
	path := "dst/path"
	t.Run("fails when index does not exist", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(
			fmt.Errorf("class %v doesn't exist", classes[0]))

		bm := createManager(sourcer, nil, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("class %v doesn't exist", classes[0]))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("fails when index has multiple shards", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(
			fmt.Errorf("class %v has %d physical shards", classes[0], 2))

		bm := createManager(sourcer, nil, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("class %v has %d physical shards", classes[0], 2))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("fails when backend not registered", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)

		backendError := errors.New("I do not exist")
		bm := createManager(sourcer, nil, backendError)

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

	t.Run("fails when error reading meta from backend", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)

		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)

		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, errors.New("can not be read"))
		bm := createManager(sourcer, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("backup %s already exists", backupID))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("fails when meta exists on backend", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		desc := backup.BackupDescriptor{}
		bytes, _ := json.Marshal(desc)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)

		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(bytes, nil)

		bm := createManager(sourcer, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("backup %s already exists", backupID))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("fails when backup creation already in progress", func(t *testing.T) {
		t.Skip("skip flaky test with timing issue")
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, nil)
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil)
		// make sure create backup takes some time, so the parallel execution has enough time to start before first one finishes
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("GetObject", ctx, backupID2, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, mock.Anything).Return(nil)
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		bm := createManager(sourcer, backend, nil)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, err := bm.Backup(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID,
				Include: classes,
			})
			time.Sleep(75 * time.Millisecond) // enough time to async create finish

			assert.Nil(t, err)
			assert.NotNil(t, meta)
			assert.Equal(t, backup.Started, backup.Status(*meta.Status))
			assert.Equal(t, path, meta.Path)
			wg.Done()
		}()
		go func() {
			time.Sleep(25 * time.Microsecond)
			meta, err := bm.Backup(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID2,
				Include: classes,
			})
			time.Sleep(75 * time.Millisecond) // enough time to async create finish

			assert.NotNil(t, err)
			assert.Nil(t, meta)
			assert.Contains(t, err.Error(), "already in progress")
			assert.IsType(t, backup.ErrUnprocessable{}, err)
			wg.Done()
		}()

		wg.Wait()
	})

	t.Run("fails when init meta fails", func(t *testing.T) {
		classes := []string{className}

		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		backend := &fakeBackend{}
		backend.On("HomeDir", mock.Anything).Return(path)
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(errors.New("init meta failed"))
		bm := createManager(sourcer, backend, nil)

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

	t.Run("successfully starts", func(t *testing.T) {
		t.Skip("skip until BackupDescriptors and ReleaseBackup are fully implemented")

		classes := []string{className}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, nil).Once()
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil).Once()
		backend := &fakeBackend{}
		backend.On("GetObject", ctx, backupID, MetaDataFilename).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(nil)
		backend.On("HomeDir", backupID).Return(path)
		backend.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		bm := createManager(sourcer, backend, nil)

		meta, err := bm.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})
		time.Sleep(10 * time.Millisecond) // enough time to async create finish

		assert.NotNil(t, meta)
		assert.Equal(t, backup.Started, backup.Status(*meta.Status))
		assert.Equal(t, path, meta.Path)
		assert.Nil(t, err)
		sourcer.AssertExpectations(t) // make sure async create called
	})

	t.Run("successfully starts for multiple classes", func(t *testing.T) {
		t.Skip("skip until BackupDescriptors and ReleaseBackup are fully implemented")

		classes := []string{className, className2}
		sourcer := &fakeSourcer{}
		sourcer.On("Backupable", ctx, classes).Return(nil)
		sourcer.On("CreateBackup", mock.Anything, mock.Anything).Return(nil, nil).Once()
		sourcer.On("ReleaseBackup", mock.Anything, mock.Anything).Return(nil).Once()
		backend := &fakeBackend{}
		backend.On("GetMeta", ctx, "", backupID).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		backend.On("Initialize", ctx, backupID).Return(nil)
		backend.On("HomeDir", backupID).Return(path)
		backend.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		bm := createManager(sourcer, backend, nil)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, err := bm.Backup(ctx, nil, &BackupRequest{
				Backend: backendName,
				ID:      backupID,
				Include: classes,
			})
			time.Sleep(75 * time.Millisecond) // enough time to async create finish

			assert.NotNil(t, meta)
			assert.Equal(t, backup.Started, backup.Status(*meta.Status))
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()

		wg.Wait()
		sourcer.AssertExpectations(t)
	})
}

// func TestBackupManager_RestoreBackup(t *testing.T) {
// 	className := "DemoClass"
// 	className2 := "DemoClass2"
// 	storageName := "DemoStorage"
// 	backupID := "SnapshotID"
// 	backupID2 := "SnapshotID2"
// 	ctx := context.Background()
// 	path := "dst/path"

// 	t.Run("fails when index already exists", func(t *testing.T) {
// 		snapshotter := &fakeSnapshotter{}
// 		bm := createManager(snapshotter, nil, nil, nil)
// 		meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("can not restore snapshot of existing index for %s", className))
// 		assert.IsType(t, backup.ErrUnprocessable{}, err)
// 	})

// 	t.Run("fails when storage not registered", func(t *testing.T) {
// 		storageError := errors.New("I do not exist")
// 		bm := createManager(nil, nil, storageError, nil)

// 		meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("find storage by name %s", storageName))
// 		assert.IsType(t, backup.ErrUnprocessable{}, err)
// 	})

// 	t.Run("fails when error reading meta from storage", func(t *testing.T) {
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(nil, errors.New("can not be read"))
// 		bm := createManager(nil, storage, nil, nil)

// 		meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("checking snapshot %s of index for %s exists on storage %s", backupID, className, storageName))
// 		assert.IsType(t, backup.ErrUnprocessable{}, err)
// 	})

// 	t.Run("fails when meta does not exist on storage", func(t *testing.T) {
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(nil, backup.NewErrNotFound(errors.New("not found")))
// 		bm := createManager(nil, storage, nil, nil)

// 		meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("snapshot %s of index for %s does not exist on storage %s", backupID, className, storageName))
// 		assert.IsType(t, backup.ErrNotFound{}, err)
// 	})

// 	t.Run("fails when meta with invalid state", func(t *testing.T) {
// 		statuses := []string{string(backup.CreateFailed), string(backup.CreateStarted), string(backup.CreateTransferring), string(backup.CreateTransferred), "And Now for Something Completely Different"}

// 		for _, status := range statuses {
// 			storage := &fakeBackend{}
// 			storage.On("GetMeta", ctx, className, backupID).Return(&backup.Snapshot{Status: status}, nil)
// 			bm := createManager(nil, storage, nil, nil)

// 			meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)

// 			assert.Nil(t, meta)
// 			assert.NotNil(t, err)
// 			assert.Contains(t, err.Error(), fmt.Sprintf("snapshot %s of index for %s on storage %s is corrupted", backupID, className, storageName))
// 			assert.IsType(t, backup.ErrNotFound{}, err)
// 		}
// 	})

// 	t.Run("fails when snapshot restoration already in progress", func(t *testing.T) {
// 		// make sure restore backup takes some time, so the parallel execution has enough time to start before first one finishes
// 		storage := &fakeBackend{getMetaStatusSleep: 50 * time.Millisecond, restoreSnapshotSleep: 50 * time.Millisecond}
// 		storage.On("GetMeta", ctx, className, backupID).Return(&backup.Snapshot{Status: string(backup.CreateSuccess)}, nil)
// 		storage.On("GetMeta", ctx, className, backupID2).Return(&backup.Snapshot{Status: string(backup.CreateSuccess)}, nil)
// 		storage.On("HomeDir", className, backupID).Return(path)
// 		storage.On("RestoreSnapshot", mock.Anything, className, backupID).Return(&backup.Snapshot{}, nil).Once()
// 		bm := createManager(nil, storage)

// 		wg := sync.WaitGroup{}
// 		wg.Add(2)

// 		go func() {
// 			meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)
// 			time.Sleep(75 * time.Millisecond) // enough time to async restore finish

// 			assert.NotNil(t, meta)
// 			assert.Equal(t, backup.RestoreStarted, meta.Status)
// 			assert.Equal(t, path, meta.Path)
// 			assert.Nil(t, err)
// 			wg.Done()
// 		}()
// 		go func() {
// 			time.Sleep(25 * time.Millisecond)
// 			meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID2)
// 			time.Sleep(75 * time.Millisecond) // enough time to async restore finish

// 			assert.Nil(t, meta)
// 			assert.NotNil(t, err)
// 			assert.Contains(t, err.Error(), fmt.Sprintf("restoration of index for %s already in progress", className))
// 			assert.IsType(t, backup.ErrUnprocessable{}, err)
// 			wg.Done()
// 		}()

// 		wg.Wait()
// 	})

// 	t.Run("successfully starts", func(t *testing.T) {
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(&backup.Snapshot{Status: string(backup.CreateSuccess)}, nil)
// 		storage.On("HomeDir", className, backupID).Return(path)
// 		storage.On("RestoreSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(&backup.Snapshot{}, nil).Once()
// 		bm := createManager(nil, storage, nil)

// 		meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)
// 		time.Sleep(10 * time.Millisecond) // enough time to async restore start

// 		assert.NotNil(t, meta)
// 		assert.Equal(t, backup.RestoreStarted, meta.Status)
// 		assert.Equal(t, path, meta.Path)
// 		assert.Nil(t, err)
// 		storage.AssertExpectations(t) // make sure async restore called
// 	})

// 	t.Run("successfully starts for multiple classes", func(t *testing.T) {
// 		// make sure restore backup takes some time, so the parallel execution has enough time to start before first one finishes
// 		storage := &fakeBackend{getMetaStatusSleep: 50 * time.Millisecond, restoreSnapshotSleep: 50 * time.Millisecond}
// 		storage.On("GetMeta", ctx, className, backupID).Return(&backup.Snapshot{Status: string(backup.CreateSuccess)}, nil)
// 		storage.On("GetMeta", ctx, className2, backupID2).Return(&backup.Snapshot{Status: string(backup.CreateSuccess)}, nil)
// 		storage.On("HomeDir", className, backupID).Return(path)
// 		storage.On("HomeDir", className2, backupID2).Return(path)
// 		storage.On("RestoreSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(&backup.Snapshot{}, nil).Twice()
// 		bm := createManager(nil, storage, nil, nil)

// 		wg := sync.WaitGroup{}
// 		wg.Add(2)

// 		go func() {
// 			meta, _, err := bm.backups.RestoreBackup(ctx, className, storageName, backupID)
// 			time.Sleep(75 * time.Millisecond) // enough time to async restore finish

// 			assert.NotNil(t, meta)
// 			assert.Equal(t, backup.RestoreStarted, meta.Status)
// 			assert.Equal(t, path, meta.Path)
// 			assert.Nil(t, err)
// 			wg.Done()
// 		}()
// 		go func() {
// 			time.Sleep(25 * time.Millisecond)
// 			meta, _, err := bm.backups.RestoreBackup(ctx, className2, storageName, backupID2)
// 			time.Sleep(75 * time.Millisecond) // enough time to async restore finish

// 			assert.NotNil(t, meta)
// 			assert.Equal(t, backup.RestoreStarted, meta.Status)
// 			assert.Equal(t, path, meta.Path)
// 			assert.Nil(t, err)
// 			wg.Done()
// 		}()

// 		wg.Wait()
// 	})
// }

// func TestBackupManager_CreateBackupStatus(t *testing.T) {
// 	className := "DemoClass"
// 	storageName := "DemoStorage"
// 	backupID := "SnapshotID"
// 	ctx := context.Background()
// 	path := "dst/path"

// 	t.Run("fails when index does not exist", func(t *testing.T) {
// 		bm := createManager(nil, nil, nil, nil)

// 		meta, err := bm.CreateBackupStatus(ctx, nil, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("can't fetch snapshot creation status of non-existing index for %s", className))
// 		assert.IsType(t, backup.ErrNotFound{}, err)
// 	})

// 	t.Run("fails when storage not registered", func(t *testing.T) {
// 		snapshotter := &fakeSnapshotter{}
// 		storageError := errors.New("I do not exist")
// 		bm := createManager(snapshotter, nil, storageError, nil)

// 		meta, err := bm.CreateBackupStatus(ctx, nil, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("find storage by name %s", storageName))
// 		assert.IsType(t, backup.ErrUnprocessable{}, err)
// 	})

// 	t.Run("fails when error reading meta from storage", func(t *testing.T) {
// 		snapshotter := &fakeSnapshotter{}
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(nil, errors.New("any type of error"))
// 		bm := createManager(snapshotter, storage, nil, nil)

// 		meta, err := bm.CreateBackupStatus(ctx, nil, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), "any type of error")
// 	})

// 	t.Run("fails when meta does not exist on storage", func(t *testing.T) {
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(nil, backup.NewErrNotFound(errors.New("not found")))
// 		bm := createManager(storage, nil)

// 		meta, err := bm.CreateBackupStatus(ctx, nil, className, storageName, backupID)

// 		assert.Nil(t, meta)
// 		assert.NotNil(t, err)
// 		assert.Contains(t, err.Error(), fmt.Sprintf("can't fetch snapshot creation status of non-existing snapshot id %s", backupID))
// 		assert.IsType(t, backup.ErrNotFound{}, err)
// 	})

// 	t.Run("successfully gets status", func(t *testing.T) {
// 		storage := &fakeBackend{}
// 		storage.On("GetMeta", ctx, className, backupID).Return(&backup.Snapshot{Status: "SOME_STATUS"}, nil)
// 		storage.On("HomeDir", className, backupID).Return(path)
// 		bm := createManager(storage, nil)

// 		meta, err := bm.CreateBackupStatus(ctx, nil, className, storageName, backupID)

// 		assert.NotNil(t, meta)
// 		assert.Equal(t, "SOME_STATUS", *meta.Status)
// 		assert.Equal(t, backupID, meta.ID)
// 		assert.Equal(t, storageName, meta.StorageName)
// 		assert.Equal(t, path, meta.Path)
// 		assert.Nil(t, err)
// 	})
// }

//func TestBackupManager_DestinationPath(t *testing.T) {
//	storageError := errors.New("I do not exist")
//	sm := createManager(nil, storageError)
//	path, err := sm.backups.HomeDir("storageName", "className", "ID")
//	require.NotNil(t, err)
//	assert.Equal(t, "", path)
//
//	storage := &fakeBackend{}
//	storage.On("HomeDir", "className", "ID").Return(path)
//	sm = createManager(storage, nil)
//	path2, err := sm.backups.HomeDir("storageName", "className", "ID")
//	require.Nil(t, err)
//	assert.Equal(t, path, path2)
//}

func createManager(sourcer Sourcer, backend modulecapabilities.BackupBackend, backendErr error) *Manager {
	backends := &fakeBackupBackendProvider{backend, backendErr}
	if sourcer == nil {
		sourcer = &fakeSourcer{}
	}
	logger, _ := test.NewNullLogger()
	return NewManager(logger, &fakeAuthorizer{}, &fakeSchemaManger{}, sourcer, backends)
}
