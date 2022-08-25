package backups

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/usecases/schema/backups"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestBackupManager_CreateBackup(t *testing.T) {
	className := "DemoClass"
	className2 := "DemoClass2"
	storageName := "DemoStorage"
	snapshotID := "SnapshotID"
	snapshotID2 := "SnapshotID2"
	ctx := context.Background()
	path := "dst/path"

	t.Run("fails when index does not exist", func(t *testing.T) {
		bm := createBackupManager(nil, nil, nil, nil)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("can not create snapshot of non-existing index for %s", className))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when index has multiple shards", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}, "b": {}}}
		bm := createBackupManager(snapshotter, nil, nil, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("snapshots for multi shard index for %s not supported yet", className))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when storage not registered", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		storageError := errors.New("I do not exist")
		bm := createBackupManager(snapshotter, nil, storageError, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("find storage by name %s", storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when error reading meta from storage", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, errors.New("can not be read"))
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when meta exists on storage", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{}, nil)
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("snapshot %s of index for %s already exists on storage %s", snapshotID, className, storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when snapshot creation already in progress", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil, nil)
		snapshotter.On("ReleaseSnapshot", mock.Anything, mock.Anything).Return(nil)
		// make sure create backup takes some time, so the parallel execution has enough time to start before first one finishes
		storage := &fakeStorage{getMetaStatusSleep: 5 * time.Millisecond, storeSnapshotSleep: 5 * time.Millisecond}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("GetMeta", ctx, className, snapshotID2).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(&snapshots.Snapshot{}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		storage.On("StoreSnapshot", mock.Anything, mock.Anything).Return(nil)
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)
			time.Sleep(10 * time.Millisecond) // enough time to async create finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.CreateStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Millisecond)
			meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID2)
			time.Sleep(10 * time.Millisecond) // enough time to async create finish

			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("snapshot of index for %s already in progress", className))
			assert.IsType(t, snapshots.ErrUnprocessable{}, err)
			wg.Done()
		}()

		wg.Wait()
	})

	t.Run("fails when init meta fails", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(nil, errors.New("init meta failed"))
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "snapshot start")
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("successfully starts", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil, nil).Once()
		snapshotter.On("ReleaseSnapshot", mock.Anything, mock.Anything).Return(nil).Once()
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(&snapshots.Snapshot{}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		storage.On("StoreSnapshot", mock.Anything, mock.Anything).Return(nil)
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)
		time.Sleep(10 * time.Millisecond) // enough time to async create finish

		assert.NotNil(t, meta)
		assert.Equal(t, snapshots.CreateStarted, meta.Status)
		assert.Equal(t, path, meta.Path)
		assert.Nil(t, err)
		snapshotter.AssertExpectations(t) // make sure async create called
	})

	t.Run("successfully starts for multiple classes", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		snapshotter.On("CreateSnapshot", mock.Anything, mock.Anything).Return(nil, nil).Twice()
		snapshotter.On("ReleaseSnapshot", mock.Anything, mock.Anything).Return(nil).Twice()
		// make sure create backup takes some time, so the parallel execution has enough time to start before first one finishes
		storage := &fakeStorage{getMetaStatusSleep: 5 * time.Millisecond, storeSnapshotSleep: 5 * time.Millisecond}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("GetMeta", ctx, className2, snapshotID2).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		storage.On("InitSnapshot", mock.Anything, className, snapshotID).Return(&snapshots.Snapshot{}, nil)
		storage.On("InitSnapshot", mock.Anything, className2, snapshotID2).Return(&snapshots.Snapshot{}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("DestinationPath", className2, snapshotID2).Return(path)
		storage.On("SetMetaStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		storage.On("StoreSnapshot", mock.Anything, mock.Anything).Return(nil)
		shardingState := &sharding.State{Physical: map[string]sharding.Physical{"a": {}}}
		bm := createBackupManager(snapshotter, storage, nil, shardingState)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, err := bm.CreateBackup(ctx, className, storageName, snapshotID)
			time.Sleep(10 * time.Millisecond) // enough time to async create finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.CreateStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Millisecond)
			meta, err := bm.CreateBackup(ctx, className2, storageName, snapshotID2)
			time.Sleep(10 * time.Millisecond) // enough time to async create finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.CreateStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()

		wg.Wait()
		snapshotter.AssertExpectations(t)
	})
}

func TestBackupManager_RestoreBackup(t *testing.T) {
	className := "DemoClass"
	className2 := "DemoClass2"
	storageName := "DemoStorage"
	snapshotID := "SnapshotID"
	snapshotID2 := "SnapshotID2"
	ctx := context.Background()
	path := "dst/path"

	t.Run("fails when index already exists", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		bm := createBackupManager(snapshotter, nil, nil, nil)

		meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("can not restore snapshot of existing index for %s", className))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when storage not registered", func(t *testing.T) {
		storageError := errors.New("I do not exist")
		bm := createBackupManager(nil, nil, storageError, nil)

		meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("find storage by name %s", storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when error reading meta from storage", func(t *testing.T) {
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, errors.New("can not be read"))
		bm := createBackupManager(nil, storage, nil, nil)

		meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when meta does not exist on storage", func(t *testing.T) {
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		bm := createBackupManager(nil, storage, nil, nil)

		meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("snapshot %s of index for %s does not exist on storage %s", snapshotID, className, storageName))
		assert.IsType(t, snapshots.ErrNotFound{}, err)
	})

	t.Run("fails when meta with invalid state", func(t *testing.T) {
		statuses := []string{string(snapshots.CreateFailed), string(snapshots.CreateStarted), string(snapshots.CreateTransferring), string(snapshots.CreateTransferred), "And Now for Something Completely Different"}

		for _, status := range statuses {
			storage := &fakeStorage{}
			storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{Status: status}, nil)
			bm := createBackupManager(nil, storage, nil, nil)

			meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)

			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("snapshot %s of index for %s on storage %s is corrupted", snapshotID, className, storageName))
			assert.IsType(t, snapshots.ErrNotFound{}, err)
		}
	})

	t.Run("fails when snapshot restoration already in progress", func(t *testing.T) {
		// make sure restore backup takes some time, so the parallel execution has enough time to start before first one finishes
		storage := &fakeStorage{getMetaStatusSleep: 5 * time.Millisecond, restoreSnapshotSleep: 5 * time.Millisecond}
		storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{Status: string(snapshots.CreateSuccess)}, nil)
		storage.On("GetMeta", ctx, className, snapshotID2).Return(&snapshots.Snapshot{Status: string(snapshots.CreateSuccess)}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("RestoreSnapshot", mock.Anything, className, snapshotID).Return(&snapshots.Snapshot{}, nil).Once()
		bm := createBackupManager(nil, storage, nil, nil)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)
			time.Sleep(10 * time.Millisecond) // enough time to async restore finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.RestoreStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Millisecond)
			meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID2)
			time.Sleep(10 * time.Millisecond) // enough time to async restore finish

			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("restoration of index for %s already in progress", className))
			assert.IsType(t, snapshots.ErrUnprocessable{}, err)
			wg.Done()
		}()

		wg.Wait()
	})

	t.Run("successfully starts", func(t *testing.T) {
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{Status: string(snapshots.CreateSuccess)}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("RestoreSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(&snapshots.Snapshot{}, nil).Once()
		bm := createBackupManager(nil, storage, nil, nil)

		meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)
		time.Sleep(10 * time.Millisecond) // enough time to async restore start

		assert.NotNil(t, meta)
		assert.Equal(t, snapshots.RestoreStarted, meta.Status)
		assert.Equal(t, path, meta.Path)
		assert.Nil(t, err)
		storage.AssertExpectations(t) // make sure async restore called
	})

	t.Run("successfully starts for multiple classes", func(t *testing.T) {
		// make sure restore backup takes some time, so the parallel execution has enough time to start before first one finishes
		storage := &fakeStorage{getMetaStatusSleep: 5 * time.Millisecond, restoreSnapshotSleep: 5 * time.Millisecond}
		storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{Status: string(snapshots.CreateSuccess)}, nil)
		storage.On("GetMeta", ctx, className2, snapshotID2).Return(&snapshots.Snapshot{Status: string(snapshots.CreateSuccess)}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		storage.On("DestinationPath", className2, snapshotID2).Return(path)
		storage.On("RestoreSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(&snapshots.Snapshot{}, nil).Twice()
		bm := createBackupManager(nil, storage, nil, nil)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			meta, _, err := bm.RestoreBackup(ctx, className, storageName, snapshotID)
			time.Sleep(10 * time.Millisecond) // enough time to async restore finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.RestoreStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()
		go func() {
			time.Sleep(time.Millisecond)
			meta, _, err := bm.RestoreBackup(ctx, className2, storageName, snapshotID2)
			time.Sleep(10 * time.Millisecond) // enough time to async restore finish

			assert.NotNil(t, meta)
			assert.Equal(t, snapshots.RestoreStarted, meta.Status)
			assert.Equal(t, path, meta.Path)
			assert.Nil(t, err)
			wg.Done()
		}()

		wg.Wait()
	})
}

func TestBackupManager_CreateBackupStatus(t *testing.T) {
	className := "DemoClass"
	storageName := "DemoStorage"
	snapshotID := "SnapshotID"
	ctx := context.Background()
	path := "dst/path"

	t.Run("fails when index does not exist", func(t *testing.T) {
		bm := createBackupManager(nil, nil, nil, nil)

		meta, err := bm.CreateBackupStatus(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("can't fetch snapshot creation status of non-existing index for %s", className))
		assert.IsType(t, snapshots.ErrNotFound{}, err)
	})

	t.Run("fails when storage not registered", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storageError := errors.New("I do not exist")
		bm := createBackupManager(snapshotter, nil, storageError, nil)

		meta, err := bm.CreateBackupStatus(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("find storage by name %s", storageName))
		assert.IsType(t, snapshots.ErrUnprocessable{}, err)
	})

	t.Run("fails when error reading meta from storage", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, errors.New("any type of error"))
		bm := createBackupManager(snapshotter, storage, nil, nil)

		meta, err := bm.CreateBackupStatus(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "any type of error")
	})

	t.Run("fails when meta does not exist on storage", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(nil, snapshots.NewErrNotFound(errors.New("not found")))
		bm := createBackupManager(snapshotter, storage, nil, nil)

		meta, err := bm.CreateBackupStatus(ctx, className, storageName, snapshotID)

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("can't fetch snapshot creation status of non-existing snapshot id %s", snapshotID))
		assert.IsType(t, snapshots.ErrNotFound{}, err)
	})

	t.Run("successfully gets status", func(t *testing.T) {
		snapshotter := &fakeSnapshotter{}
		storage := &fakeStorage{}
		storage.On("GetMeta", ctx, className, snapshotID).Return(&snapshots.Snapshot{Status: "SOME_STATUS"}, nil)
		storage.On("DestinationPath", className, snapshotID).Return(path)
		bm := createBackupManager(snapshotter, storage, nil, nil)

		meta, err := bm.CreateBackupStatus(ctx, className, storageName, snapshotID)

		assert.NotNil(t, meta)
		assert.Equal(t, "SOME_STATUS", *meta.Status)
		assert.Equal(t, snapshotID, meta.ID)
		assert.Equal(t, storageName, meta.StorageName)
		assert.Equal(t, path, meta.Path)
		assert.Nil(t, err)
	})
}

func createBackupManager(snapshotter Snapshotter, storage modulecapabilities.SnapshotStorage,
	storageErr error, shardingState *sharding.State,
) backups.BackupManager {
	snapshotters := &fakeSnapshotterProvider{snapshotter}
	storages := &fakeBackupStorageProvider{storage, storageErr}
	shardingStateFunc := func(className string) *sharding.State { return shardingState }

	logger, _ := test.NewNullLogger()
	return NewBackupManager(logger, snapshotters, storages, shardingStateFunc)
}
