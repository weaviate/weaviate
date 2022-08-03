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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type modulesProvider interface {
	BackupStorageProvider(providerID string) (modulecapabilities.SnapshotStorage, error)
}

type Snapshotter interface { // implemented by the index
	// Snapshot creates a snapshot which is metadata referencing files on disk.
	// While the snapshot exists, the index makes sure that those files are never
	// changed, for examle by stopping compactions.
	//
	// The index stays usable with a snapshot present, it can still accept
	// reads+writes, as the index is built in an append-only-way.
	//
	// Snapshot() fails if another snapshot exists.
	Snapshot(ctx context.Context) (snapshots.Snapshot, error)

	// ReleaseSnapshot signals to the unederlyhing index that the files have been
	// copied (or the operation aborted), and that it is safe for the index to
	// change the files, such as start compactions.
	ReleaseSnapshot(ctx context.Context, id string) error
}

type SnapshotProvider struct {
	snapshotter     Snapshotter
	storageProvider modulecapabilities.SnapshotStorage
}

func NewSnapshotProvider(snapshotter Snapshotter,
	storageProvider modulecapabilities.SnapshotStorage,
) (*SnapshotProvider, error) {
	return &SnapshotProvider{ /*TODO*/ }, nil
}

func (sm *SnapshotProvider) Backup(ctx context.Context) error {
	snapshot, err := sm.snapshotter.Snapshot(ctx)
	if err != nil {
		return errors.Wrap(err, "create snapshot")
	}

	if err := sm.storageProvider.StoreSnapshot(ctx, snapshot); err != nil {
		return errors.Wrap(err, "store snapshot")
	}

	// 3. if successful delete snapshot
	if err := sm.snapshotter.ReleaseSnapshot(ctx, snapshot.ID); err != nil {
		return errors.Wrap(err, "delete snapshot")
	}

	return nil
}

type BackupManager struct {
	modules modulesProvider
}

// CreateBackup is called by the User
func (bm *BackupManager) CreateBackup(ctx context.Context, className string,
	storageProviderName string,
) error {
	// get snapshotter from Index for className
	var snapshotter Snapshotter

	storageProvider, err := bm.modules.BackupStorageProvider(storageProviderName)
	if err != nil {
		return errors.Wrapf(err, "find backup provider %q", storageProviderName)
	}

	provider, err := NewSnapshotProvider(snapshotter, storageProvider)
	if err != nil {
		return errors.Wrap(err, "init snapshot provider")
	}

	err = provider.Backup(ctx)
	if err != nil {
		return errors.Wrap(err, "execute backup")
	}

	return nil
}

func (bm *BackupManager) RestoreBackup(ctx context.Context, className string,
	storageProviderName string, backupID string,
) error {
	panic("not implemented")
}
