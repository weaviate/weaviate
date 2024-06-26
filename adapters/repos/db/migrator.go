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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// offloadProvider is an interface has to be implemented by modules
// to get the backend (s3, azure blob storage, google cloud storage) module
type offloadProvider interface {
	// OffloadBackend returns the backend module for (s3, azure blob storage, google cloud storage)
	OffloadBackend(backend string) (modulecapabilities.OffloadCloud, bool)
}

type Migrator struct {
	db     *DB
	cloud  modulecapabilities.OffloadCloud
	logger logrus.FieldLogger
	nodeId string
}

func NewMigrator(db *DB, logger logrus.FieldLogger) *Migrator {
	return &Migrator{db: db, logger: logger}
}

func (m *Migrator) SetNode(nodeID string) {
	m.nodeId = nodeID
}

func (m *Migrator) SetOffloadProvider(provider offloadProvider, moduleName string) {
	cloud, enabled := provider.OffloadBackend(moduleName)
	if !enabled {
		m.logger.Debug(fmt.Sprintf("module %s is not enabled", moduleName))
	}
	m.cloud = cloud
	m.logger.Info(fmt.Sprintf("module %s is enabled", moduleName))
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
			MemtablesFlushDirtyAfter:  m.db.config.MemtablesFlushDirtyAfter,
			MemtablesInitialSizeMB:    m.db.config.MemtablesInitialSizeMB,
			MemtablesMaxSizeMB:        m.db.config.MemtablesMaxSizeMB,
			MemtablesMinActiveSeconds: m.db.config.MemtablesMinActiveSeconds,
			MemtablesMaxActiveSeconds: m.db.config.MemtablesMaxActiveSeconds,
			MaxSegmentSize:            m.db.config.MaxSegmentSize,
			HNSWMaxLogSize:            m.db.config.HNSWMaxLogSize,
			HNSWWaitForCachePrefill:   m.db.config.HNSWWaitForCachePrefill,
			TrackVectorDimensions:     m.db.config.TrackVectorDimensions,
			AvoidMMap:                 m.db.config.AvoidMMap,
			DisableLazyLoadShards:     m.db.config.DisableLazyLoadShards,
			ReplicationFactor:         NewAtomicInt64(class.ReplicationConfig.Factor),
			AsyncReplicationEnabled:   class.ReplicationConfig.AsyncEnabled,
		},
		shardState,
		// no backward-compatibility check required, since newly added classes will
		// always have the field set
		inverted.ConfigFromModel(class.InvertedIndexConfig),
		convertToVectorIndexConfig(class.VectorIndexConfig),
		convertToVectorIndexConfigs(class.VectorConfig),
		m.db.schemaGetter, m.db, m.logger, m.db.nodeResolver, m.db.remoteIndex,
		m.db.replicaClient, m.db.promMetrics, class, m.db.jobQueueCh, m.db.indexCheckpoints,
		m.db.memMonitor)
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

// UpdateIndex ensures that the local index is up2date with the latest sharding
// state (shards/tenants) and index properties that may have been added in the
// case that the local node was down during a class update operation.
//
// This method is relevant when the local node is a part of a cluster,
// particularly with the introduction of the v2 RAFT-based schema
func (m *Migrator) UpdateIndex(ctx context.Context, incomingClass *models.Class,
	incomingSS *sharding.State,
) error {
	idx := m.db.GetIndex(schema.ClassName(incomingClass.Class))

	{ // add index if missing
		if idx == nil {
			if err := m.AddClass(ctx, incomingClass, incomingSS); err != nil {
				return fmt.Errorf(
					"add missing class %s during update index: %w",
					incomingClass.Class, err)
			}
			return nil
		}
	}

	{ // add/remove missing shards
		if incomingSS.PartitioningEnabled {
			if err := m.updateIndexTenants(ctx, idx, incomingClass, incomingSS); err != nil {
				return err
			}
		} else {
			if err := m.updateIndexAddShards(ctx, idx, incomingClass, incomingSS); err != nil {
				return err
			}
		}
	}

	{ // add missing properties
		if err := m.updateIndexAddMissingProperties(ctx, idx, incomingClass); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) updateIndexTenants(ctx context.Context, idx *Index,
	incomingClass *models.Class, incomingSS *sharding.State,
) error {
	if err := m.updateIndexAddTenants(ctx, idx, incomingClass, incomingSS); err != nil {
		return err
	}
	return m.updateIndexDeleteTenants(ctx, idx, incomingSS)
}

func (m *Migrator) updateIndexAddTenants(ctx context.Context, idx *Index,
	incomingClass *models.Class, incomingSS *sharding.State,
) error {
	for shardName, phys := range incomingSS.Physical {
		// Only load the tenant if activity status == HOT
		if schemaUC.IsLocalActiveTenant(&phys, m.db.schemaGetter.NodeName()) {
			if _, err := idx.initLocalShard(ctx, shardName, incomingClass); err != nil {
				return fmt.Errorf("add missing tenant shard %s during update index: %w", shardName, err)
			}
		}
	}
	return nil
}

func (m *Migrator) updateIndexDeleteTenants(ctx context.Context,
	idx *Index, incomingSS *sharding.State,
) error {
	var toRemove []string

	idx.ForEachShard(func(name string, _ ShardLike) error {
		if _, ok := incomingSS.Physical[name]; !ok {
			toRemove = append(toRemove, name)
		}
		return nil
	})

	if len(toRemove) > 0 {
		if err := idx.dropShards(toRemove); err != nil {
			return fmt.Errorf("drop tenant shards %v during update index: %w", toRemove, err)
		}
	}
	return nil
}

func (m *Migrator) updateIndexAddShards(ctx context.Context, idx *Index,
	incomingClass *models.Class, incomingSS *sharding.State,
) error {
	for _, shardName := range incomingSS.AllLocalPhysicalShards() {
		if _, err := idx.initLocalShard(ctx, shardName, incomingClass); err != nil {
			return fmt.Errorf("add missing shard %s during update index: %w", shardName, err)
		}
	}
	return nil
}

func (m *Migrator) updateIndexAddMissingProperties(ctx context.Context, idx *Index,
	incomingClass *models.Class,
) error {
	for _, prop := range incomingClass.Properties {
		// Returning an error in idx.ForEachShard stops the range.
		// So if one shard is missing the property bucket, we know
		// that the property needs to be added to the index, and
		// don't need to continue iterating over all shards
		errMissingProp := errors.New("missing prop")
		err := idx.ForEachShard(func(name string, shard ShardLike) error {
			bucket := shard.Store().Bucket(helpers.BucketFromPropNameLSM(prop.Name))
			if bucket == nil {
				return errMissingProp
			}
			return nil
		})
		if errors.Is(err, errMissingProp) {
			if err := idx.addProperty(ctx, prop); err != nil {
				return fmt.Errorf("add missing prop %s during update index: %w", prop.Name, err)
			}
		}
	}
	return nil
}

func (m *Migrator) AddProperty(ctx context.Context, className string, prop ...*models.Property) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot add property to a non-existing index for %s", className)
	}

	return idx.addProperty(ctx, prop...)
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

func (m *Migrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string, schemaVersion uint64) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update shard status to a non-existing index for %s", className)
	}

	return idx.updateShardStatus(ctx, shardName, targetStatus, schemaVersion)
}

// NewTenants creates new partitions
func (m *Migrator) NewTenants(ctx context.Context, class *models.Class, creates []*schemaUC.CreateTenantPayload) error {
	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return fmt.Errorf("cannot find index for %q", class.Class)
	}

	ec := &errorcompounder.ErrorCompounder{}
	for _, pl := range creates {
		if pl.Status != models.TenantActivityStatusHOT {
			continue // skip creating inactive shards
		}

		_, err := idx.getOrInitLocalShard(ctx, pl.Name)
		ec.Add(err)
	}
	return ec.ToError()
}

// UpdateTenants activates or deactivates tenant partitions and returns a commit func
// that can be used to either commit or rollback the changes
func (m *Migrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*schemaUC.UpdateTenantPayload) error {
	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return fmt.Errorf("cannot find index for %q", class.Class)
	}

	updatesHot := make([]string, 0, len(updates))
	updatesCold := make([]string, 0, len(updates))
	for _, update := range updates {
		switch update.Status {
		case models.TenantActivityStatusHOT:
			updatesHot = append(updatesHot, update.Name)
		case models.TenantActivityStatusCOLD:
			updatesCold = append(updatesCold, update.Name)
		}
	}

	ec := &errorcompounder.ErrorCompounder{}

	for _, name := range updatesHot {
		shard, err := idx.getOrInitLocalShard(ctx, name)
		ec.Add(err)
		if err != nil {
			continue
		}

		// if the shard is a lazy load shard, we need to force its activation now
		asLL, ok := shard.(*LazyLoadShard)
		if !ok {
			continue
		}

		name := name // prevent loop variable capture
		enterrors.GoWrapper(func() {
			// The timeout is rather arbitrary. It's meant to be so high that it can
			// never stop a valid tenant activation use case, but low enough to
			// prevent a context-leak.
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
			defer cancel()

			if err := asLL.Load(ctx); err != nil {
				idx.logger.WithFields(logrus.Fields{
					"action": "tenant_activation_lazy_laod_shard",
					"shard":  name,
				}).WithError(err).Errorf("loading shard %q failed", name)
			}
		}, idx.logger)
	}

	if len(updatesCold) > 0 {
		idx.backupMutex.RLock()
		defer idx.backupMutex.RUnlock()

		eg := enterrors.NewErrorGroupWrapper(m.logger)
		eg.SetLimit(_NUMCPU * 2)

		for _, name := range updatesCold {
			name := name
			eg.Go(func() error {
				shard := func() ShardLike {
					idx.shardInUseLocks.Lock(name)
					defer idx.shardInUseLocks.Unlock(name)

					return idx.shards.Load(name)
				}()

				if shard == nil {
					return nil // shard already does not exist or inactive
				}

				idx.shardCreateLocks.Lock(name)
				defer idx.shardCreateLocks.Unlock(name)

				idx.shards.LoadAndDelete(name)

				if err := shard.Shutdown(ctx); err != nil {
					if !errors.Is(err, errAlreadyShutdown) {
						ec.Add(err)
						idx.logger.WithField("action", "shutdown_shard").
							WithField("shard", shard.ID()).Error(err)
					}
					m.logger.WithField("shard", shard.Name()).Debug("was already shut or dropped")
				}
				return nil
			})
		}
		eg.Wait()
	}
	return ec.ToError()
}

// DeleteTenants deletes tenants
// CAUTION: will not delete inactive tenants (shard files will not be removed)
func (m *Migrator) DeleteTenants(ctx context.Context, class string, tenants []string) error {
	if idx := m.db.GetIndex(schema.ClassName(class)); idx != nil {
		return idx.dropShards(tenants)
	}
	return nil
}

func (m *Migrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schemaConfig.VectorIndexConfig,
) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update vector index config of non-existing index for %s", className)
	}

	return idx.updateVectorIndexConfig(ctx, updated)
}

func (m *Migrator) UpdateVectorIndexConfigs(ctx context.Context,
	className string, updated map[string]schemaConfig.VectorIndexConfig,
) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update vector config of non-existing index for %s", className)
	}

	return idx.updateVectorIndexConfigs(ctx, updated)
}

func (m *Migrator) ValidateVectorIndexConfigUpdate(
	old, updated schemaConfig.VectorIndexConfig,
) error {
	// hnsw is the only supported vector index type at the moment, so no need
	// to check, we can always use that an hnsw-specific validation should be
	// used for now.
	switch old.IndexType() {
	case vectorindex.VectorIndexTypeHNSW:
		return hnsw.ValidateUserConfigUpdate(old, updated)
	case vectorindex.VectorIndexTypeFLAT:
		return flat.ValidateUserConfigUpdate(old, updated)
	case vectorindex.VectorIndexTypeDYNAMIC:
		return dynamic.ValidateUserConfigUpdate(old, updated)
	}
	return fmt.Errorf("Invalid index type: %s", old.IndexType())
}

func (m *Migrator) ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaConfig.VectorIndexConfig,
) error {
	for vecName := range old {
		if err := m.ValidateVectorIndexConfigUpdate(old[vecName], updated[vecName]); err != nil {
			return fmt.Errorf("vector %q", vecName)
		}
	}
	return nil
}

func (m *Migrator) ValidateInvertedIndexConfigUpdate(old, updated *models.InvertedIndexConfig,
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

func (m *Migrator) UpdateReplicationFactor(ctx context.Context, className string, factor int64) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update replication factor of non-existing index for %s", className)
	}

	idx.Config.ReplicationFactor.Store(factor)
	return nil
}

func (m *Migrator) UpdateAsyncReplication(ctx context.Context, className string, enabled bool) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update inverted index config of non-existing index for %s", className)
	}

	return idx.updateAsyncReplication(ctx, enabled)
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
			if shard.hasTargetVectors() {
				for vecName, vec := range object.Vectors {
					if err := shard.extendDimensionTrackerForVecLSM(len(vec), object.DocID, vecName); err != nil {
						return err
					}
				}
			} else {
				if err := shard.extendDimensionTrackerLSM(len(object.Vector), object.DocID); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	f := func() {
		for {
			m.logger.
				WithField("action", "reindex").
				Warnf("Reindexed %v objects. Reindexing dimensions complete. Please remove environment variable REINDEX_VECTOR_DIMENSIONS_AT_STARTUP before next startup", count)
			time.Sleep(5 * time.Minute)
		}
	}
	enterrors.GoWrapper(f, m.logger)

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

			shard.GetPropertyLengthTracker().Flush()

			return nil
		})

		// Flush the GetPropertyLengthTracker() to disk
		err := index.IterateShards(ctx, func(index *Index, shard ShardLike) error {
			return shard.GetPropertyLengthTracker().Flush()
		})
		if err != nil {
			m.logger.WithField("error", err).Error("could not flush prop lengths")
		}

	}
	f := func() {
		for {
			m.logger.
				WithField("action", "recount").
				Warnf("Recounted %v objects. Recounting properties complete. Please remove environment variable 	RECOUNT_PROPERTIES_AT_STARTUP before next startup", count)
			time.Sleep(5 * time.Minute)
		}
	}
	enterrors.GoWrapper(f, m.logger)

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

	eg := enterrors.NewErrorGroupWrapper(m.logger)
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
			}, name)
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

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)
	for _, index := range m.db.indices {
		index := index
		className := index.Config.ClassName.String()

		if _, ok := task.migrationState.MissingFilterableClass2Props[className]; !ok {
			continue
		}

		eg.Go(func() error {
			errgrpShards := enterrors.NewErrorGroupWrapper(m.logger)
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
				}, shard.ID())
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
		}, index.ID())
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

func (m *Migrator) WaitForStartup(ctx context.Context) error {
	return m.db.WaitForStartup(ctx)
}

// Shutdown no-op if db was never loaded
func (m *Migrator) Shutdown(ctx context.Context) error {
	if !m.db.StartupComplete() {
		return nil
	}
	m.logger.Info("closing loaded database ...")
	return m.db.Shutdown(ctx)
}
