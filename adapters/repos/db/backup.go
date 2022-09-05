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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/errorcompounder"
	"github.com/semi-technologies/weaviate/entities/schema"
	"golang.org/x/sync/errgroup"
)

type BackupState struct {
	SnapshotID string
	InProgress bool
}

// Backupable returns whether all given class can be backed up.
//
// A class cannot be backed up either if it doesn't exist or if it has more than one physical shard.
func (db *DB) Backupable(ctx context.Context, classes []string) error {
	for _, c := range classes {
		idx := db.GetIndex(schema.ClassName(c))
		if idx == nil {
			return fmt.Errorf("class %v doesn't exist", c)
		}
		if n := db.schemaGetter.ShardingState(c).CountPhysicalShards(); n > 1 {
			return fmt.Errorf("class %v has %d physical shards", c, n)
		}
	}
	return nil
}

// BackupDescriptors returns a channel of class descriptors.
// Class descriptor records everything needed to restore a class
// If an error happens a descriptor with an error will be written to the channel just before closing it.
func (db *DB) BackupDescriptors(ctx context.Context, bakid string, classes []string,
) <-chan backup.ClassDescriptor {
	ds := make(chan backup.ClassDescriptor, len(classes))
	go func() {
		for _, c := range classes {
			desc := backup.ClassDescriptor{Name: c}
			idx := db.GetIndex(schema.ClassName(c))
			if idx == nil {
				desc.Error = fmt.Errorf("class %v doesn't exist any more", c)
			} else if err := idx.descriptor(ctx, bakid, &desc); err != nil {
				desc.Error = fmt.Errorf("backup class %v descriptor: %w", c, err)
			} else {
				desc.Error = ctx.Err()
			}

			ds <- desc
			if desc.Error != nil {
				break
			}
		}
		close(ds)
	}()
	return ds
}

// ReleaseBackup release resources acquired by the index during backup
func (db *DB) ReleaseBackup(ctx context.Context, bakID, class string) error {
	idx := db.GetIndex(schema.ClassName(class))
	if idx != nil {
		return idx.ReleaseBackup(ctx, bakID)
	}
	return nil
}

func (db *DB) ClassExists(name string) bool {
	return db.GetIndex(schema.ClassName(name)) != nil
}

// descriptor record everything needed to restore a class
func (i *Index) descriptor(ctx context.Context, backupid string, desc *backup.ClassDescriptor) (err error) {
	if err := i.initSnapshot(backupid); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			i.resetSnapshotOnFailedCreate(ctx, err)
		}
	}()
	for _, s := range i.Shards {
		if err = s.beginBackup(ctx); err != nil {
			return fmt.Errorf("pause compaction and flush: %w", err)
		}
		var ddesc backup.ShardDescriptor
		if err := s.listBackupFiles(ctx, &ddesc); err != nil {
			return fmt.Errorf("list shard %v files: %w", s.name, err)
		}

		desc.Shards = append(desc.Shards, ddesc)
	}

	if desc.ShardingState, err = i.marshalShardingState(); err != nil {
		return fmt.Errorf("marshal sharding state %w", err)
	}
	if desc.Schema, err = i.marshalSchema(); err != nil {
		return fmt.Errorf("marshal schema %w", err)
	}
	return nil
}

// ReleaseBackup marks the specified snapshot as inactive and restarts all
// async background and maintenance processes. It errors if the snapshot does not exist
// or is already inactive.
func (i *Index) ReleaseBackup(ctx context.Context, id string) error {
	defer i.resetSnapshotState()
	if err := i.resumeMaintenanceCycles(ctx); err != nil {
		return err
	}
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

	i.snapshotState = BackupState{
		SnapshotID: id,
		InProgress: true,
	}

	return nil
}

func (i *Index) resetSnapshotOnFailedCreate(ctx context.Context, err error) error {
	defer i.resetSnapshotState()

	ec := errorcompounder.ErrorCompounder{}
	ec.Add(err)
	ec.Add(i.resumeMaintenanceCycles(ctx))
	return ec.ToError()
}

func (i *Index) resetSnapshotState() {
	i.snapshotStateLock.Lock()
	defer i.snapshotStateLock.Unlock()
	i.snapshotState = BackupState{InProgress: false}
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
