package db

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"golang.org/x/sync/errgroup"
)

// CreateSnapshot creates a new active snapshot for all state in this index across
// all its shards. It is safe to copy any file referenced in the snapshot, as the
// active state in the snapshot guarantees that those files cannot be modified.
//
// There can only be one active snapshot at a time, and creating a snapshot will
// fail on any Index that already has an active snapshot.
//
// Make sure to call ReleaseSnapshot for this snapshot's ID once you have finished
// copying the files to make sure background and maintenance processes can resume.
func (i *Index) CreateSnapshot(ctx context.Context, id string) (*snapshots.Snapshot, error) {
	if err := i.initSnapshot(id); err != nil {
		return nil, err
	}

	snap := &snapshots.Snapshot{
		ID:        id,
		StartedAt: time.Now(),
		BasePath:  i.Config.RootPath,
	}

	var (
		snapshotFilesLock sync.Mutex
		snapshotFiles     []string
		g                 errgroup.Group
	)

	for _, shard := range i.Shards {
		s := shard
		g.Go(func() error {
			files, err := s.createStoreLevelSnapshot(ctx)
			if err != nil {
				return err
			}
			snapshotFilesLock.Lock()
			defer snapshotFilesLock.Unlock()
			snapshotFiles = append(snapshotFiles, files...)
			return nil
		})

		g.Go(func() error {
			files, err := s.createVectorIndexLevelSnapshot(ctx)
			if err != nil {
				return err
			}
			snapshotFilesLock.Lock()
			defer snapshotFilesLock.Unlock()
			snapshotFiles = append(snapshotFiles, files...)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	snap.Files = snapshotFiles
	snap.CompletedAt = time.Now()
	return snap, nil
}

// ReleaseSnapshot marks the specified snapshot as inactive and restarts all
// async background and maintenance processes. It errors if the snapshot does not exist
// or is already inactive.
func (i *Index) ReleaseSnapshot(ctx context.Context, id string) error {
	i.snapshotState = snapshots.State{InProgress: false}
	return nil
}

func (i *Index) initSnapshot(id string) error {
	if i.snapshotState.InProgress {
		return errors.Errorf(
			"cannot create new snapshot, snapshot ‘%s’ is not yet released, this "+
				"means its contents have not yet been fully copied to its destination, "+
				"try again later", i.snapshotState.SnapshotID)
	}

	i.snapshotState = snapshots.State{
		SnapshotID: id,
		InProgress: true,
	}

	return nil
}
