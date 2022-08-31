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
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
)

type shardingStateFunc func(className string) *sharding.State

type backupManager struct {
	logger            logrus.FieldLogger
	source            SourceFactory
	storages          BackupStorageProvider
	shardingStateFunc shardingStateFunc

	createInProgress  map[string]bool
	createLock        sync.Mutex
	restoreInProgress map[string]bool
	restoreLock       sync.Mutex
}

func NewBackupManager(logger logrus.FieldLogger, snapshotters SourceFactory, storages BackupStorageProvider,
	shardingStateFunc shardingStateFunc,
) *backupManager {
	return &backupManager{
		logger:            logger,
		source:            snapshotters,
		storages:          storages,
		shardingStateFunc: shardingStateFunc,

		createInProgress:  make(map[string]bool),
		restoreInProgress: make(map[string]bool),
	}
}

// CreateBackup is called by the User
func (bm *backupManager) CreateBackup(ctx context.Context, className,
	storageName, snapshotID string,
) (*backup.CreateMeta, error) {
	// snapshotter (index) exists
	snapshotter := bm.source.SourceFactory(className)
	if snapshotter == nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("can not create snapshot of non-existing index for %s", className))
	}

	// multi shards not supported yet
	if bm.isMultiShard(className) {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("snapshots for multi shard index for %s not supported yet", className))
	}

	// requested storage is registered
	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, backup.NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	// there is no snapshot with given id on the storage, regardless of its state (valid or corrupted)
	_, err = storage.GetMeta(ctx, className, snapshotID)
	if err == nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("snapshot %s of index for %s already exists on storage %s", snapshotID, className, storageName))
	}
	if _, ok := err.(backup.ErrNotFound); !ok {
		return nil, backup.NewErrUnprocessable(errors.Wrapf(err, "checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
	}

	// no snapshot in progress for the class
	if !bm.setCreateInProgress(className, true) {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("snapshot of index for %s already in progress", className))
	}

	provider := newBackupProvider(snapshotter, storage, className, snapshotID)
	snapshot, err := provider.start(ctx)
	if err != nil {
		bm.setCreateInProgress(className, false)
		return nil, backup.NewErrUnprocessable(errors.Wrapf(err, "snapshot start"))
	}

	go func(ctx context.Context, provider *backupProvider) {
		if err := provider.backup(ctx, snapshot); err != nil {
			bm.logger.WithField("action", "create_backup").
				Error(err)
		}
		bm.setCreateInProgress(className, false)
	}(ctx, provider)

	return &backup.CreateMeta{
		Path:   storage.DestinationPath(className, snapshotID),
		Status: backup.CreateStarted,
	}, nil
}

func (bm *backupManager) CreateBackupStatus(ctx context.Context,
	className, storageName, snapshotID string,
) (*models.SnapshotMeta, error) {
	// snapshotter (index) exists
	if snapshotter := bm.source.SourceFactory(className); snapshotter == nil {
		return nil, backup.NewErrNotFound(
			fmt.Errorf("can't fetch snapshot creation status of "+
				"non-existing index for %s", className))
	}

	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, backup.NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	meta, err := storage.GetMeta(ctx, className, snapshotID)
	if err != nil && errors.As(err, &backup.ErrNotFound{}) {
		return nil, backup.NewErrNotFound(
			fmt.Errorf("can't fetch snapshot creation status of "+
				"non-existing snapshot id %s", snapshotID))
	} else if err != nil {
		return nil, err
	}

	status := string(meta.Status)

	// TODO: populate Error field if snapshot failed
	return &models.SnapshotMeta{
		ID:          snapshotID,
		Path:        storage.DestinationPath(className, snapshotID),
		Status:      &status,
		StorageName: storageName,
	}, nil
}

func (bm *backupManager) DestinationPath(storageName, className, snapshotID string) (string, error) {
	// requested storage is registered
	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return "", err
	}

	return storage.DestinationPath(className, snapshotID), nil
}

func (bm *backupManager) RestoreBackup(ctx context.Context, className,
	storageName, snapshotID string,
) (*backup.RestoreMeta, *backup.Snapshot, error) {
	timer := monitoring.NewOnceTimer(prometheus.NewTimer(monitoring.GetMetrics().SnapshotRestoreBackupInitDurations.WithLabelValues(storageName, className)))
	defer timer.ObserveDurationOnce()
	// snapshotter (index) does not exist
	if snapshotter := bm.source.SourceFactory(className); snapshotter != nil {
		return nil, nil, backup.NewErrUnprocessable(fmt.Errorf("can not restore snapshot of existing index for %s", className))
	}

	// requested storage is registered
	storage, err := bm.storages.BackupStorage(storageName)
	if err != nil {
		return nil, nil, backup.NewErrUnprocessable(errors.Wrapf(err, "find storage by name %s", storageName))
	}

	// snapshot with given id exists and is valid
	if meta, err := storage.GetMeta(ctx, className, snapshotID); err != nil {
		if _, ok := err.(backup.ErrNotFound); !ok {
			return nil, nil, backup.NewErrUnprocessable(errors.Wrapf(err, "checking snapshot %s of index for %s exists on storage %s", snapshotID, className, storageName))
		}
		return nil, nil, backup.NewErrNotFound(errors.Wrapf(err, "snapshot %s of index for %s does not exist on storage %s", snapshotID, className, storageName))
	} else if meta.Status != string(backup.CreateSuccess) {
		return nil, nil, backup.NewErrNotFound(fmt.Errorf("snapshot %s of index for %s on storage %s is corrupted", snapshotID, className, storageName))
	}

	// no restore in progress for the class
	if !bm.setRestoreInProgress(className, true) {
		return nil, nil, backup.NewErrUnprocessable(fmt.Errorf("restoration of index for %s already in progress", className))
	}

	timer.ObserveDurationOnce()

	snapshot, err := storage.RestoreSnapshot(ctx, className, snapshotID)
	if err != nil {
		bm.setRestoreInProgress(className, false)
		return nil, nil, backup.NewErrUnprocessable(errors.Wrapf(err, "restore snapshot %s of index for %s", snapshotID, className))
	}

	bm.setRestoreInProgress(className, false)

	return &backup.RestoreMeta{
		Path:   storage.DestinationPath(className, snapshotID),
		Status: backup.RestoreStarted,
	}, snapshot, nil
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
