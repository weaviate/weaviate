//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	shard *Shard,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	t.stateLock.RLock()
	className := shard.index.Config.ClassName.String()
	props, ok := t.migrationState.Class2Props[className]
	t.stateLock.RUnlock()

	if !ok || len(props) == 0 {
		return reindexableProperties, nil
	}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithIdleThreshold(time.Duration(shard.index.Config.MemtablesFlushIdleAfter) * time.Second),
	}

	for propName := range props {
		bucketNameSearchable := helpers.BucketSearchableFromPropNameLSM(propName)
		bucketNameFilterable := helpers.BucketFromPropNameLSM(propName)

		bucketSearchable := shard.store.Bucket(bucketNameSearchable)
		bucketFilterable := shard.store.Bucket(bucketNameFilterable)

		// bucket filterable does not exist, bucket searchable of strategy map exists
		if bucketFilterable == nil &&
			bucketSearchable != nil &&
			bucketSearchable.Strategy() == lsmkv.StrategyMapCollection {

			reindexableProperties = append(reindexableProperties, ReindexableProperty{
				PropertyName:    propName,
				IndexType:       IndexTypePropValue,
				DesiredStrategy: lsmkv.StrategyRoaringSet,
				NewIndex:        true,
				BucketOptions:   bucketOptions,
			})
		}
	}

	return reindexableProperties, nil
}

func (t *shardInvertedReindexTaskMissingTextFilterable) removeClassFromMigrationStateAndSave(className string) error {
	t.stateLock.Lock()
	defer t.stateLock.Unlock()

	delete(t.migrationState.Class2Props, className)
	return t.files.saveMigrationState(t.migrationState)
}
