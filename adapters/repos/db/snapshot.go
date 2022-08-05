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

package db

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/usecases/config"
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

	var (
		snap = snapshots.New(id, time.Now())
		g    errgroup.Group
	)

	// preliminary write to persist a snapshot status of "started"
	if err := snap.WriteToDisk(i.Config.RootPath); err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	for _, shard := range i.Shards {
		s := shard
		g.Go(func() error {
			if err := s.createSnapshot(ctx, snap); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		err = i.resetSnapshotOnFailedCreate(ctx, snap, err)
		return nil, err
	}

	shardingState, err := i.marshalShardingState()
	if err != nil {
		err = i.resetSnapshotOnFailedCreate(ctx, snap, err)
		return nil, errors.Wrap(err, "create snapshot")
	}

	schema, err := i.marshalSchema()
	if err != nil {
		err = i.resetSnapshotOnFailedCreate(ctx, snap, err)
		return nil, errors.Wrap(err, "create snapshot")
	}

	snap.ShardingState = shardingState
	snap.Schema = schema
	snap.ServerVersion = config.ServerVersion
	snap.Status = snapshots.StatusCreated
	snap.CompletedAt = time.Now()

	if err := snap.WriteToDisk(i.Config.RootPath); err != nil {
		err = i.resetSnapshotOnFailedCreate(ctx, snap, err)
		return nil, err
	}

	return snap, nil
}

// ReleaseSnapshot marks the specified snapshot as inactive and restarts all
// async background and maintenance processes. It errors if the snapshot does not exist
// or is already inactive.
func (i *Index) ReleaseSnapshot(ctx context.Context, id string) error {
	if err := i.resumeMaintenanceCycles(ctx); err != nil {
		return err
	}

	snap, err := snapshots.ReadFromDisk(id, i.Config.RootPath)
	if err != nil {
		return errors.Wrap(err, "release snapshot")
	}

	snap.Status = snapshots.StatusReleased
	if err := snap.WriteToDisk(i.Config.RootPath); err != nil {
		return errors.Wrap(err, "release snapshot")
	}

	defer i.resetSnapshotState()
	return nil
}

func (i *Index) initSnapshot(id string) error {
	i.snapshotStateLock.Lock()
	defer i.snapshotStateLock.Unlock()

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

func (i *Index) resetSnapshotOnFailedCreate(ctx context.Context, snap *snapshots.Snapshot, err error) error {
	defer i.resetSnapshotState()

	ec := errorcompounder.ErrorCompounder{}
	ec.Add(err)
	ec.Add(i.resumeMaintenanceCycles(ctx))
	ec.Add(snap.RemoveFromDisk(i.Config.RootPath))
	return ec.ToError()
}

func (i *Index) resetSnapshotState() {
	i.snapshotStateLock.Lock()
	defer i.snapshotStateLock.Unlock()
	i.snapshotState = snapshots.State{InProgress: false}
}

func (i *Index) resumeMaintenanceCycles(ctx context.Context) error {
	var g errgroup.Group

	for _, shard := range i.Shards {
		s := shard
		g.Go(func() error {
			return s.resumeMaintenanceCycles(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "resume maintenance cycles")
	}

	return nil
}

func (i *Index) marshalShardingState() ([]byte, error) {
	b, err := i.getSchema.ShardingState(i.Config.ClassName.String()).JSON()
	if err != nil {
		return nil, errors.Wrap(err, "marshal sharding state")
	}

	return b, nil
}

func (i *Index) marshalSchema() ([]byte, error) {
	schema := i.getSchema.GetSchemaSkipAuth()

	b, err := schema.GetClass(i.Config.ClassName).MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "marshal schema")
	}

	return b, err
}
