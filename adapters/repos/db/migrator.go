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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/sync/errgroup"
)

type Migrator struct {
	db     *DB
	logger logrus.FieldLogger
}

func (m *Migrator) AddClass(ctx context.Context, class *models.Class,
	shardState *sharding.State,
) error {
	if err := replica.ValidateConfig(class); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	idx, err := NewIndex(ctx,
		IndexConfig{
			ClassName:                 schema.ClassName(class.Class),
			RootPath:                  m.db.config.RootPath,
			ResourceUsage:             m.db.config.ResourceUsage,
			QueryMaximumResults:       m.db.config.QueryMaximumResults,
			MemtablesFlushIdleAfter:   m.db.config.MemtablesFlushIdleAfter,
			MemtablesInitialSizeMB:    m.db.config.MemtablesInitialSizeMB,
			MemtablesMaxSizeMB:        m.db.config.MemtablesMaxSizeMB,
			MemtablesMinActiveSeconds: m.db.config.MemtablesMinActiveSeconds,
			MemtablesMaxActiveSeconds: m.db.config.MemtablesMaxActiveSeconds,
			TrackVectorDimensions:     m.db.config.TrackVectorDimensions,
			ReplicationFactor:         class.ReplicationConfig.Factor,
		},
		shardState,
		// no backward-compatibility check required, since newly added classes will
		// always have the field set
		inverted.ConfigFromModel(class.InvertedIndexConfig),
		class.VectorIndexConfig.(schema.VectorIndexConfig),
		m.db.schemaGetter, m.db, m.logger, m.db.nodeResolver, m.db.remoteIndex,
		m.db.replicaClient, m.db.promMetrics, class, m.db.jobQueueCh)
	if err != nil {
		return errors.Wrap(err, "create index")
	}

	err = idx.addUUIDProperty(ctx)
	if err != nil {
		return errors.Wrapf(err, "extend idx '%s' with uuid property", idx.ID())
	}

	if class.InvertedIndexConfig.IndexTimestamps {
		err = idx.addTimestampProperties(ctx)
		if err != nil {
			return errors.Wrapf(err, "extend idx '%s' with timestamp properties", idx.ID())
		}
	}

	if m.db.config.TrackVectorDimensions {
		if err := idx.addDimensionsProperty(context.TODO()); err != nil {
			return errors.Wrap(err, "init id property")
		}
	}

	m.db.indexLock.Lock()
	m.db.indices[idx.ID()] = idx
	idx.notifyReady()
	m.db.indexLock.Unlock()

	return nil
}

func (m *Migrator) addPropertiesAndNullAndLength(ctx context.Context, prop *models.Property, idx *Index) error {
	err := idx.addProperty(ctx, prop)
	if err != nil {
		return errors.Wrapf(err, "extend idx '%s' with property", idx.ID())
	}

	if idx.invertedIndexConfig.IndexNullState {
		err = idx.addNullStateProperty(ctx, prop)
		if err != nil {
			return errors.Wrapf(err, "extend idx '%s' with nullstate properties", idx.ID())
		}
	}

	if idx.invertedIndexConfig.IndexPropertyLength {
		dt := schema.DataType(prop.DataType[0])
		// some datatypes are not added to the inverted index, so we can skip them here
		switch dt {
		case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeInt,
			schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		default:
			err = idx.addPropertyLength(ctx, prop)
			if err != nil {
				return errors.Wrapf(err, "extend idx '%s' with property length", idx.ID())
			}
		}
	}
	return nil
}

func (m *Migrator) DropClass(ctx context.Context, className string) error {
	err := m.db.DeleteIndex(schema.ClassName(className))
	if err != nil {
		return errors.Wrapf(err, "delete idx for class '%s'", className)
	}

	return nil
}

func (m *Migrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	if newClassName != nil {
		return errors.New("weaviate does not support renaming of classes")
	}

	return nil
}

func (m *Migrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot add property to a non-existing index for %s", className)
	}

	return m.addPropertiesAndNullAndLength(ctx, prop, idx)
}

// DropProperty is ignored, API compliant change
func (m *Migrator) DropProperty(ctx context.Context, className string, propertyName string) error {
	// ignore but don't error
	return nil
}

func (m *Migrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	if newName != nil {
		return errors.New("weaviate does not support renaming of properties")
	}

	return nil
}

func (m *Migrator) GetShardsStatus(ctx context.Context, className string) (map[string]string, error) {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, errors.Errorf("cannot get shards status for a non-existing index for %s", className)
	}

	return idx.getShardsStatus(ctx)
}

func (m *Migrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update shard status to a non-existing index for %s", className)
	}

	return idx.updateShardStatus(ctx, shardName, targetStatus)
}

func NewMigrator(db *DB, logger logrus.FieldLogger) *Migrator {
	return &Migrator{db: db, logger: logger}
}

func (m *Migrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schema.VectorIndexConfig,
) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update vector index config of non-existing index for %s", className)
	}

	return idx.updateVectorIndexConfig(ctx, updated)
}

func (m *Migrator) ValidateVectorIndexConfigUpdate(ctx context.Context,
	old, updated schema.VectorIndexConfig,
) error {
	// hnsw is the only supported vector index type at the moment, so no need
	// to check, we can always use that an hnsw-specific validation should be
	// used for now.
	return hnsw.ValidateUserConfigUpdate(old, updated)
}

func (m *Migrator) ValidateInvertedIndexConfigUpdate(ctx context.Context,
	old, updated *models.InvertedIndexConfig,
) error {
	return inverted.ValidateUserConfigUpdate(old, updated)
}

func (m *Migrator) UpdateInvertedIndexConfig(ctx context.Context, className string,
	updated *models.InvertedIndexConfig,
) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update inverted index config of non-existing index for %s", className)
	}

	conf := inverted.ConfigFromModel(updated)

	return idx.updateInvertedIndexConfig(ctx, conf)
}

func (m *Migrator) RecalculateVectorDimensions(ctx context.Context) error {
	count := 0
	m.logger.
		WithField("action", "reindex").
		Info("Reindexing dimensions, this may take a while")

	// Iterate over all indexes
	for _, index := range m.db.indices {
		// Iterate over all shards
		if err := index.IterateObjects(ctx, func(index *Index, shard *Shard, object *storobj.Object) error {
			count = count + 1
			err := shard.extendDimensionTrackerLSM(len(object.Vector), object.DocID())
			return err
		}); err != nil {
			return err
		}
	}
	go func() {
		for {
			m.logger.
				WithField("action", "reindex").
				Warnf("Reindexed %v objects. Reindexing dimensions complete. Please remove environment variable REINDEX_VECTOR_DIMENSIONS_AT_STARTUP before next startup", count)
			time.Sleep(5 * time.Minute)
		}
	}()

	return nil
}

func (m *Migrator) InvertedReindex(ctx context.Context, taskNames ...string) error {
	tasksProviders := map[string]func() ShardInvertedReindexTask{
		"ShardInvertedReindexTaskSetToRoaringSet": func() ShardInvertedReindexTask {
			return &ShardInvertedReindexTaskSetToRoaringSet{}
		},
	}

	tasks := map[string]ShardInvertedReindexTask{}
	for _, taskName := range taskNames {
		if taskProvider, ok := tasksProviders[taskName]; ok {
			tasks[taskName] = taskProvider()
		}
	}

	if len(tasks) == 0 {
		return nil
	}

	errgrp := &errgroup.Group{}
	for _, index := range m.db.indices {
		for _, shard := range index.Shards {
			func(shard *Shard) {
				errgrp.Go(func() error {
					reindexer := NewShardInvertedReindexer(shard, m.logger)
					for taskName, task := range tasks {
						reindexer.AddTask(task)
						m.logger.
							WithField("action", "inverted reindex").
							WithField("task", taskName).
							WithField("shard", shard.name).
							Info("About to start inverted reindexing, this may take a while")
					}
					res := reindexer.Do(ctx)
					m.logger.
						WithField("action", "inverted reindex").
						WithField("shard", shard.name).
						Info("Finished inverted reindexing")
					return res
				})
			}(shard)
		}
	}
	return errgrp.Wait()
}
