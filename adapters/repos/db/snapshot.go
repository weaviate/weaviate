package db

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
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

	snap := snapshots.New(id, time.Now(), i.Config.RootPath)

	for _, shard := range i.Shards {
		if err := shard.createSnapshot(ctx, snap); err != nil {
			return nil, err
		}
	}

	shardingState, err := i.marshalShardingState()
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	schema, err := i.marshalSchema()
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot")
	}

	snap.ShardingState = shardingState
	snap.Schema = schema
	snap.CompletedAt = time.Now()

	if err := snap.WriteToDisk(); err != nil {
		return nil, err
	}

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
