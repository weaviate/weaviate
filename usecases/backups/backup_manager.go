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

package backups

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/schema/backups"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type shardingStateFunc func(className string) *sharding.State

type backupManager struct {
	db                *db.DB
	storages          BackupStorageProvider
	shardingStateFunc shardingStateFunc
	createInProgress  map[string]bool
	createLock        sync.Mutex
	restoreInProgress map[string]bool
	restoreLock       sync.Mutex
}

func NewBackupManager(db *db.DB, storages BackupStorageProvider, shardingStateFunc shardingStateFunc) backups.BackupManager {
	return &backupManager{
		db:                db,
		storages:          storages,
		shardingStateFunc: shardingStateFunc,
		createInProgress:  make(map[string]bool),
		restoreInProgress: make(map[string]bool),
	}
}

// CreateBackup is called by the User
func (bm *backupManager) CreateBackup(ctx context.Context, className,
	storageName, snapshotID string,
) (*backups.CreateMeta, error) {
	// index for requested class exists
	idx := bm.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, NewErrUnprocessable(fmt.Errorf("can not create snapshot of non-existing index for %s", className))
	}

	// multi shards not supported yet
	if bm.isMultiShard(className) {
		return nil, NewErrUnprocessable(fmt.Errorf("snapshots for multi shard index for %s not supported yet", className))
	}

	// requested storage is registered
	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	// there is no snapshot with given id on the storage, regardless of its state (valid or corrupted)
	if _, err := storage.GetMetaStatus(ctx, className, snapshotID); err != nil {
		if err.Error() != os.ErrNotExist.Error() {
			return nil, NewErrUnprocessable(errors.Wrapf(err, "checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
		}
	} else {
		return nil, NewErrUnprocessable(fmt.Errorf("snapshot %s of index for %s already exists on storage %s", snapshotID, className, storageName))
	}

	// no snapshot in progress for the class
	if !bm.setCreateInProgress(className, true) {
		return nil, NewErrUnprocessable(fmt.Errorf("snapshot of index for %s already in progress", className))
	}

	provider := newSnapshotProvider(idx, storage, className, snapshotID)
	snapshot, err := provider.start(ctx)
	if err != nil {
		bm.setCreateInProgress(className, false)
		return nil, NewErrUnprocessable(errors.Wrapf(err, "snapshot start"))
	}

	go func(ctx context.Context, provider *snapshotProvider) {
		provider.backup(ctx, snapshot)
		bm.setCreateInProgress(className, false)
	}(ctx, provider)

	return &backups.CreateMeta{
		Path:   provider.storage.DestinationPath(className, snapshotID),
		Status: backups.CS_STARTED,
	}, nil
}

func (bm *backupManager) CreateBackupStatus(ctx context.Context,
	className, storageName, snapshotID string,
) (*models.SnapshotMeta, error) {
	idx := bm.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, NewErrUnprocessable(
			fmt.Errorf("can't fetch snapshot creation status of "+
				"non-existing index for %s", className))
	}

	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	status, err := storage.GetMetaStatus(ctx, className, snapshotID)
	if err != nil {
		return nil, err
	}

	// TODO: populate Error field if snapshot failed
	return &models.SnapshotMeta{
		ID:          snapshotID,
		Path:        storage.DestinationPath(className, snapshotID),
		Status:      &status,
		StorageName: storageName,
	}, nil
}

func (bm *backupManager) RestoreBackup(ctx context.Context, className,
	storageName, snapshotID string,
) (*backups.RestoreMeta, error) {
	// index for requested class does not exist
	idx := bm.db.GetIndex(schema.ClassName(className))
	if idx != nil {
		return nil, NewErrUnprocessable(fmt.Errorf("can not restore snapshot of existing index for %s", className))
	}

	// requested storage is registered
	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	// snapshot with given id exists and is valid
	if status, err := storage.GetMetaStatus(ctx, className, snapshotID); err != nil {
		// TODO improve check, according to implementation of GetMetaStatus
		if err.Error() != "file does not exist" {
			return nil, NewErrUnprocessable(errors.Wrapf(err, "checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
		}
		return nil, NewErrNotFound(errors.Wrapf(err, "snapshot %s of index for %s does not exist on storage %s", snapshotID, className, storageName))
	} else if backups.CreateStatus(status) != backups.CS_SUCCESS {
		return nil, NewErrNotFound(fmt.Errorf("snapshot %s of index for %s on storage %s is corrupted", snapshotID, className, storageName))
	}

	// no restore in progress for the class
	if !bm.setRestoreInProgress(className, true) {
		return nil, NewErrUnprocessable(fmt.Errorf("restoration of index for %s already in progress", className))
	}

	go func(ctx context.Context, className, snapshotId string) {
		storage.RestoreSnapshot(ctx, className, snapshotID)
		// TODO after copying files from storage schema needs to be updated.
		// This most likely requires a new method since we need to create a class with existing sharding state.
		// Currently Create Class would initiate a new sharding state.
		bm.setRestoreInProgress(className, false)
	}(ctx, className, snapshotID)

	return &backups.RestoreMeta{
		Path:   storage.DestinationPath(className, snapshotID),
		Status: backups.RS_STARTED,
	}, nil
}

func (bm *backupManager) isMultiShard(className string) bool {
	physicalShards := bm.shardingStateFunc(className).Physical
	return len(physicalShards) > 1
}

func (bm *backupManager) setCreateInProgress(className string, inProgress bool) bool {
	bm.createLock.Lock()
	defer bm.createLock.Unlock()

	key := strings.ToLower(className)
	if inProgress && bm.createInProgress[key] {
		return false
	}
	if !inProgress && !bm.createInProgress[key] {
		return false
	}
	bm.createInProgress[key] = inProgress
	return true
}

func (bm *backupManager) setRestoreInProgress(className string, inProgress bool) bool {
	bm.restoreLock.Lock()
	defer bm.restoreLock.Unlock()

	key := strings.ToLower(className)
	if inProgress && bm.restoreInProgress[key] {
		return false
	}
	if !inProgress && !bm.restoreInProgress[key] {
		return false
	}
	bm.restoreInProgress[key] = inProgress
	return true
}
