//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

type shardInvertedReindexTaskMissingTextFilterable struct {
	logger    logrus.FieldLogger
	files     *filterableToSearchableMigrationFiles
	stateLock *sync.RWMutex

	migrationState *filterableToSearchableMigrationState
}

func newShardInvertedReindexTaskMissingTextFilterable(migrator *Migrator,
) *shardInvertedReindexTaskMissingTextFilterable {
	return &shardInvertedReindexTaskMissingTextFilterable{
		logger:    migrator.logger,
		files:     newFilterableToSearchableMigrationFiles(migrator.db.config.RootPath),
		stateLock: new(sync.RWMutex),
	}
}

func (t *shardInvertedReindexTaskMissingTextFilterable) init() error {
	migrationState, err := t.files.loadMigrationState()
	if err != nil {
		return errors.Wrap(err, "failed loading migration state")
	}

	t.migrationState = migrationState
	return nil
}

func (t *shardInvertedReindexTaskMissingTextFilterable) GetPropertiesToReindex(ctx context.Context,
	shard reindex.ShardLike,
) ([]reindex.ReindexableProperty, error) {
	reindexableProperties := []reindex.ReindexableProperty{}

	t.stateLock.RLock()
	className := shard.ParentIndex().ConfigSnapshot().ClassName.String()
	props, ok := t.migrationState.MissingFilterableClass2Props[className]
	t.stateLock.RUnlock()

	if !ok || len(props) == 0 {
		return reindexableProperties, nil
	}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithDirtyThreshold(time.Duration(shard.ParentIndex().ConfigSnapshot().MemtablesFlushDirtyAfter) * time.Second),
	}

	for propName := range props {
		bucketNameSearchable := helpers.BucketSearchableFromPropNameLSM(propName)
		bucketNameFilterable := helpers.BucketFromPropNameLSM(propName)

		bucketSearchable := shard.Store().Bucket(bucketNameSearchable)
		bucketFilterable := shard.Store().Bucket(bucketNameFilterable)

		// exists bucket searchable of strategy map and either of
		// - exists empty filterable bucket of strategy roaring set
		//   (weaviate was restrated after filterable to searchable migration)
		// - filterable bucket does not exist
		//   (indexing comes right after filterable to searchable migration)
		if bucketSearchable != nil &&
			bucketSearchable.Strategy() == lsmkv.StrategyMapCollection {

			if bucketFilterable == nil {
				reindexableProperties = append(reindexableProperties, reindex.ReindexableProperty{
					PropertyName:    propName,
					IndexType:       reindex.IndexTypePropValue,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					NewIndex:        true,
					BucketOptions:   bucketOptions,
				})
			} else if bucketFilterable.Strategy() == lsmkv.StrategyRoaringSet {
				reindexableProperties = append(reindexableProperties, reindex.ReindexableProperty{
					PropertyName:    propName,
					IndexType:       reindex.IndexTypePropValue,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				})
			}
		}
	}

	return reindexableProperties, nil
}

func (t *shardInvertedReindexTaskMissingTextFilterable) updateMigrationStateAndSave(classCreatedFilterable string) error {
	t.stateLock.Lock()
	defer t.stateLock.Unlock()

	t.migrationState.CreatedFilterableClass2Props[classCreatedFilterable] = t.migrationState.MissingFilterableClass2Props[classCreatedFilterable]
	delete(t.migrationState.MissingFilterableClass2Props, classCreatedFilterable)
	return t.files.saveMigrationState(t.migrationState)
}

func (t *shardInvertedReindexTaskMissingTextFilterable) OnPostResumeStore(ctx context.Context, shard reindex.ShardLike) error {
	// turn off fallback mode immediately after creating filterable index and resuming store's activity
	shard.SetFallbackToSearchable(false)
	return nil
}

func (t *shardInvertedReindexTaskMissingTextFilterable) ObjectsIterator(shard reindex.ShardLike) reindex.ObjectsIterator {
	return shard.Store().Bucket(helpers.ObjectsBucketLSM).IterateObjects
}
