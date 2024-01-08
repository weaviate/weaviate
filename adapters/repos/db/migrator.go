//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
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
	if err := replica.ValidateConfig(class, m.db.config.Replication); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	idx, err := NewIndex(ctx,
		IndexConfig{
			ClassName:                 schema.ClassName(class.Class),
			RootPath:                  m.db.config.RootPath,
			ResourceUsage:             m.db.config.ResourceUsage,
			QueryMaximumResults:       m.db.config.QueryMaximumResults,
			QueryNestedRefLimit:       m.db.config.QueryNestedRefLimit,
			MemtablesFlushIdleAfter:   m.db.config.MemtablesFlushIdleAfter,
			MemtablesInitialSizeMB:    m.db.config.MemtablesInitialSizeMB,
			MemtablesMaxSizeMB:        m.db.config.MemtablesMaxSizeMB,
			MemtablesMinActiveSeconds: m.db.config.MemtablesMinActiveSeconds,
			MemtablesMaxActiveSeconds: m.db.config.MemtablesMaxActiveSeconds,
			TrackVectorDimensions:     m.db.config.TrackVectorDimensions,
			AvoidMMap:                 m.db.config.AvoidMMap,
			DisableLazyLoadShards:     m.db.config.DisableLazyLoadShards,
			ReplicationFactor:         class.ReplicationConfig.Factor,
		},
		shardState,
		// no backward-compatibility check required, since newly added classes will
		// always have the field set
		inverted.ConfigFromModel(class.InvertedIndexConfig),
		class.VectorIndexConfig.(schema.VectorIndexConfig),
		m.db.schemaGetter, m.db, m.logger, m.db.nodeResolver, m.db.remoteIndex,
		m.db.replicaClient, m.db.promMetrics, class, m.db.jobQueueCh, m.db.indexCheckpoints)
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

func (m *Migrator) DropClass(ctx context.Context, className string) error {
	return m.db.DeleteIndex(schema.ClassName(className))
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

	return idx.addProperty(ctx, prop)
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

func (m *Migrator) GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error) {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, errors.Errorf("cannot get shards status for a non-existing index for %s", className)
	}

	return idx.getShardsQueueSize(ctx, tenant)
}

func (m *Migrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, errors.Errorf("cannot get shards status for a non-existing index for %s", className)
	}

	return idx.getShardsStatus(ctx, tenant)
}

func (m *Migrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update shard status to a non-existing index for %s", className)
	}

	return idx.updateShardStatus(ctx, shardName, targetStatus)
}

// NewTenants creates new partitions and returns a commit func
// that can be used to either commit or rollback the partitions
func (m *Migrator) NewTenants(ctx context.Context, class *models.Class, creates []*migrate.CreateTenantPayload) (commit func(success bool), err error) {
	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return nil, fmt.Errorf("cannot find index for %q", class.Class)
	}

	shards := make(map[string]ShardLike, len(creates))
	rollback := func() {
		for name, shard := range shards {
			if err := shard.drop(); err != nil {
				m.logger.WithField("action", "drop_shard").
					WithField("class", class.Class).
					Errorf("cannot drop self created shard %s: %v", name, err)
			}
		}
	}
	commit = func(success bool) {
		if success {
			for name, shard := range shards {
				idx.shards.Store(name, shard)
			}
			return
		}
		rollback()
	}
	defer func() {
		if err != nil {
			rollback()
		}
	}()

	for _, pl := range creates {
		if shard := idx.shards.Load(pl.Name); shard != nil {
			continue
		}
		if pl.Status != models.TenantActivityStatusHOT {
			continue // skip creating inactive shards
		}

		shard, err := idx.initShard(ctx, pl.Name, class, m.db.promMetrics)
		if err != nil {
			return nil, fmt.Errorf("cannot create partition %q: %w", pl, err)
		}
		shards[pl.Name] = shard
	}

	return commit, nil
}

// UpdateTenans activates or deactivates tenant partitions and returns a commit func
// that can be used to either commit or rollback the changes
func (m *Migrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*migrate.UpdateTenantPayload) (commit func(success bool), err error) {
	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return nil, fmt.Errorf("cannot find index for %q", class.Class)
	}

	shardsToHot := make([]string, 0, len(updates))
	shardsToCold := make([]string, 0, len(updates))
	shardsHotted := make(map[string]ShardLike)
	shardsColded := make(map[string]ShardLike)

	rollbackHotted := func() {
		eg := new(errgroup.Group)
		eg.SetLimit(2 * _NUMCPU)
		for name, shard := range shardsHotted {
			name, shard := name, shard
			eg.Go(func() error {
				if err := shard.Shutdown(ctx); err != nil {
					idx.logger.WithField("action", "rollback_shutdown_shard").
						WithField("shard", shard.ID()).
						Errorf("cannot shutdown self activated shard %q: %s", name, err)
				}
				return nil
			})
		}
		eg.Wait()
	}
	rollbackColded := func() {
		for name, shard := range shardsColded {
			idx.shards.CompareAndSwap(name, nil, shard)
		}
	}
	rollback := func() {
		rollbackHotted()
		rollbackColded()
	}

	commitHotted := func() {
		for name, shard := range shardsHotted {
			idx.shards.Store(name, shard)
		}
	}
	commitColded := func() {
		for name := range shardsColded {
			idx.shards.LoadAndDelete(name)
		}

		eg := new(errgroup.Group)
		eg.SetLimit(_NUMCPU * 2)
		for name, shard := range shardsColded {
			name, shard := name, shard
			eg.Go(func() error {
				if err := shard.Shutdown(ctx); err != nil {
					idx.logger.WithField("action", "shutdown_shard").
						WithField("shard", shard.ID()).
						Errorf("cannot shutdown shard %q: %s", name, err)
				}
				return nil
			})
		}
		eg.Wait()
	}
	commit = func(success bool) {
		if !success {
			rollback()
			return
		}
		commitHotted()
		commitColded()
	}

	applyHot := func() error {
		for _, name := range shardsToHot {
			// shard already hot
			if shard := idx.shards.Load(name); shard != nil {
				continue
			}

			shard, err := idx.initShard(ctx, name, class, m.db.promMetrics)
			if err != nil {
				return fmt.Errorf("cannot activate shard '%s': %w", name, err)
			}
			shardsHotted[name] = shard
		}
		return nil
	}
	applyCold := func() error {
		idx.backupMutex.RLock()
		defer idx.backupMutex.RUnlock()

		for _, name := range shardsToCold {
			shard, ok := idx.shards.Swap(name, nil) // mark as deactivated
			if !ok {                                // shard doesn't exit (already cold)
				idx.shards.LoadAndDelete(name) // rollback nil value created by swap()
				continue
			}
			if shard != nil {
				shardsColded[name] = shard
			}
		}
		return nil
	}

	for _, tu := range updates {
		switch tu.Status {
		case models.TenantActivityStatusHOT:
			shardsToHot = append(shardsToHot, tu.Name)
		case models.TenantActivityStatusCOLD:
			shardsToCold = append(shardsToCold, tu.Name)
		}
	}

	defer func() {
		if err != nil {
			rollback()
		}
	}()

	if err := applyHot(); err != nil {
		return nil, err
	}
	if err := applyCold(); err != nil {
		return nil, err
	}

	return commit, nil
}

// DeleteTenants deletes tenants and returns a commit func
// that can be used to either commit or rollback deletion
func (m *Migrator) DeleteTenants(ctx context.Context, class *models.Class, tenants []string) (commit func(success bool), err error) {
	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return func(bool) {}, nil
	}
	return idx.dropShards(tenants)
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
	switch old.IndexType() {
	case "hnsw":
		return hnsw.ValidateUserConfigUpdate(old, updated)
	case "flat":
		return flat.ValidateUserConfigUpdate(old, updated)
	}
	return fmt.Errorf("Invalid index type: %s", old.IndexType())
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
		if err := index.IterateObjects(ctx, func(index *Index, shard ShardLike, object *storobj.Object) error {
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

func (m *Migrator) RecountProperties(ctx context.Context) error {
	count := 0
	m.logger.
		WithField("action", "recount").
		Info("Recounting properties, this may take a while")

	m.db.indexLock.Lock()
	defer m.db.indexLock.Unlock()
	// Iterate over all indexes
	for _, index := range m.db.indices {

		// Clear the shards before counting
		index.IterateShards(ctx, func(index *Index, shard ShardLike) error {
			shard.GetPropertyLengthTracker().Clear()
			return nil
		})

		// Iterate over all shards
		index.IterateObjects(ctx, func(index *Index, shard ShardLike, object *storobj.Object) error {
			count = count + 1
			props, _, err := shard.AnalyzeObject(object)
			if err != nil {
				m.logger.WithField("error", err).Error("could not analyze object")
				return nil
			}

			if err := shard.SetPropertyLengths(props); err != nil {
				m.logger.WithField("error", err).Error("could not add prop lengths")
				return nil
			}

			shard.GetPropertyLengthTracker().Flush(false)

			return nil
		})

		// Flush the GetPropertyLengthTracker() to disk
		err := index.IterateShards(ctx, func(index *Index, shard ShardLike) error {
			return shard.GetPropertyLengthTracker().Flush(false)
		})
		if err != nil {
			m.logger.WithField("error", err).Error("could not flush prop lengths")
		}

	}
	go func() {
		for {
			m.logger.
				WithField("action", "recount").
				Warnf("Recounted %v objects. Recounting properties complete. Please remove environment variable 	RECOUNT_PROPERTIES_AT_STARTUP before next startup", count)
			time.Sleep(5 * time.Minute)
		}
	}()

	return nil
}

func (m *Migrator) InvertedReindex(ctx context.Context, taskNames ...string) error {
	var errs errorcompounder.ErrorCompounder
	errs.Add(m.doInvertedReindex(ctx, taskNames...))
	errs.Add(m.doInvertedIndexMissingTextFilterable(ctx, taskNames...))
	return errs.ToError()
}

func (m *Migrator) doInvertedReindex(ctx context.Context, taskNames ...string) error {
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

	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU)
	for _, index := range m.db.indices {
		index.ForEachShard(func(name string, shard ShardLike) error {
			eg.Go(func() error {
				reindexer := NewShardInvertedReindexer(shard, m.logger)
				for taskName, task := range tasks {
					reindexer.AddTask(task)
					m.logInvertedReindexShard(shard).
						WithField("task", taskName).
						Info("About to start inverted reindexing, this may take a while")
				}
				if err := reindexer.Do(ctx); err != nil {
					m.logInvertedReindexShard(shard).
						WithError(err).
						Error("failed reindexing")
					return errors.Wrapf(err, "failed reindexing shard '%s'", shard.ID())
				}
				m.logInvertedReindexShard(shard).
					Info("Finished inverted reindexing")
				return nil
			})
			return nil
		})
	}
	return eg.Wait()
}

func (m *Migrator) doInvertedIndexMissingTextFilterable(ctx context.Context, taskNames ...string) error {
	taskName := "ShardInvertedReindexTaskMissingTextFilterable"
	taskFound := false
	for _, name := range taskNames {
		if name == taskName {
			taskFound = true
			break
		}
	}
	if !taskFound {
		return nil
	}

	task := newShardInvertedReindexTaskMissingTextFilterable(m)
	if err := task.init(); err != nil {
		m.logMissingFilterable().WithError(err).Error("failed init missing text filterable task")
		return errors.Wrap(err, "failed init missing text filterable task")
	}

	if len(task.migrationState.MissingFilterableClass2Props) == 0 {
		m.logMissingFilterable().Info("no classes to create filterable index, skipping")
		return nil
	}

	m.logMissingFilterable().Info("staring missing text filterable task")

	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU * 2)
	for _, index := range m.db.indices {
		index := index
		className := index.Config.ClassName.String()

		if _, ok := task.migrationState.MissingFilterableClass2Props[className]; !ok {
			continue
		}

		eg.Go(func() error {
			errgrpShards := &errgroup.Group{}
			index.ForEachShard(func(_ string, shard ShardLike) error {
				errgrpShards.Go(func() error {
					m.logMissingFilterableShard(shard).
						Info("starting filterable indexing on shard, this may take a while")

					reindexer := NewShardInvertedReindexer(shard, m.logger)
					reindexer.AddTask(task)

					if err := reindexer.Do(ctx); err != nil {
						m.logMissingFilterableShard(shard).
							WithError(err).
							Error("failed filterable indexing on shard")
						return errors.Wrapf(err, "failed filterable indexing for shard '%s' of index '%s'",
							shard.ID(), index.ID())
					}
					m.logMissingFilterableShard(shard).
						Info("finished filterable indexing on shard")
					return nil
				})
				return nil
			})

			if err := errgrpShards.Wait(); err != nil {
				m.logMissingFilterableIndex(index).
					WithError(err).
					Error("failed filterable indexing on index")
				return errors.Wrapf(err, "failed filterable indexing of index '%s'", index.ID())
			}

			if err := task.updateMigrationStateAndSave(className); err != nil {
				m.logMissingFilterableIndex(index).
					WithError(err).
					Error("failed updating migration state file")
				return errors.Wrapf(err, "failed updating migration state file for class '%s'", className)
			}

			m.logMissingFilterableIndex(index).
				Info("finished filterable indexing on index")

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		m.logMissingFilterable().
			WithError(err).
			Error("failed missing text filterable task")
		return errors.Wrap(err, "failed missing text filterable task")
	}

	m.logMissingFilterable().Info("finished missing text filterable task")
	return nil
}

func (m *Migrator) logInvertedReindex() *logrus.Entry {
	return m.logger.WithField("action", "inverted_reindex")
}

func (m *Migrator) logInvertedReindexShard(shard ShardLike) *logrus.Entry {
	return m.logInvertedReindex().
		WithField("index", shard.Index().ID()).
		WithField("shard", shard.ID())
}

func (m *Migrator) logMissingFilterable() *logrus.Entry {
	return m.logger.WithField("action", "ii_missing_text_filterable")
}

func (m *Migrator) logMissingFilterableIndex(index *Index) *logrus.Entry {
	return m.logMissingFilterable().WithField("index", index.ID())
}

func (m *Migrator) logMissingFilterableShard(shard ShardLike) *logrus.Entry {
	return m.logMissingFilterableIndex(shard.Index()).WithField("shard", shard.ID())
}

// As of v1.19 property's IndexInverted setting is replaced with IndexFilterable
// and IndexSearchable
// Filterable buckets use roaring set strategy and searchable ones use map strategy
// (therefore are applicable just for text/text[])
// Since both type of buckets can coexist for text/text[] props they need to be
// distinguished by their name: searchable bucket has "searchable" suffix.
// Up until v1.19 default text/text[]/string/string[] (string/string[] deprecated since v1.19)
// strategy for buckets was map, migrating from pre v1.19 to v1.19 needs to properly
// handle existing text/text[] buckets of map strategy having filterable bucket name.
//
// Enabled InvertedIndex translates in v1.19 to both InvertedFilterable and InvertedSearchable
// enabled, but since only searchable bucket exist (with filterable name), it has to be renamed
// to searchable bucket.
// Though IndexFilterable setting is enabled filterable index does not exists,
// therefore shards are switched into fallback mode, to use searchable buckets instead of
// filterable ones whenever filtered are expected.
// Fallback mode effectively sets IndexFilterable to false, although it stays enabled according
// to schema.
//
// If filterable indexes will be created (that is up to user to decide whether missing indexes
// should be created later on), shards will not be working in fallback mode, and actual filterable index
// will be used when needed.
func (m *Migrator) AdjustFilterablePropSettings(ctx context.Context) error {
	f2sm := newFilterableToSearchableMigrator(m)
	if err := f2sm.migrate(ctx); err != nil {
		return err
	}
	return f2sm.switchShardsToFallbackMode(ctx)
}
