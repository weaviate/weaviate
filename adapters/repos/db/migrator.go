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
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/flat"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// provider is an interface has to be implemented by modules
// to get the backend (s3, azure blob storage, google cloud storage) module
type provider interface {
	// OffloadBackend returns the backend module for (s3, azure blob storage, google cloud storage)
	OffloadBackend(backend string) (modulecapabilities.OffloadCloud, bool)
}

type processor interface {
	UpdateTenantsProcess(ctx context.Context,
		class string, req *command.TenantProcessRequest) (uint64, error)
}

type Migrator struct {
	db      *DB
	cloud   modulecapabilities.OffloadCloud
	logger  logrus.FieldLogger
	cluster processor
	nodeId  string

	classLocks *esync.KeyLocker
}

func NewMigrator(db *DB, logger logrus.FieldLogger) *Migrator {
	return &Migrator{
		db:         db,
		logger:     logger,
		classLocks: esync.NewKeyLocker(),
	}
}

func (m *Migrator) SetNode(nodeID string) {
	m.nodeId = nodeID
}

func (m *Migrator) SetCluster(c processor) {
	m.cluster = c
}

func (m *Migrator) SetOffloadProvider(provider provider, moduleName string) {
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

	indexID := indexID(schema.ClassName(class.Class))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx != nil {
		return fmt.Errorf("index for class %v already found locally", idx.ID())
	}

	idx, err := NewIndex(ctx,
		IndexConfig{
			ClassName:                                    schema.ClassName(class.Class),
			RootPath:                                     m.db.config.RootPath,
			ResourceUsage:                                m.db.config.ResourceUsage,
			QueryMaximumResults:                          m.db.config.QueryMaximumResults,
			QueryNestedRefLimit:                          m.db.config.QueryNestedRefLimit,
			MemtablesFlushDirtyAfter:                     m.db.config.MemtablesFlushDirtyAfter,
			MemtablesInitialSizeMB:                       m.db.config.MemtablesInitialSizeMB,
			MemtablesMaxSizeMB:                           m.db.config.MemtablesMaxSizeMB,
			MemtablesMinActiveSeconds:                    m.db.config.MemtablesMinActiveSeconds,
			MemtablesMaxActiveSeconds:                    m.db.config.MemtablesMaxActiveSeconds,
			MinMMapSize:                                  m.db.config.MinMMapSize,
			LazySegmentsDisabled:                         m.db.config.LazySegmentsDisabled,
			MaxReuseWalSize:                              m.db.config.MaxReuseWalSize,
			SegmentsCleanupIntervalSeconds:               m.db.config.SegmentsCleanupIntervalSeconds,
			SeparateObjectsCompactions:                   m.db.config.SeparateObjectsCompactions,
			CycleManagerRoutinesFactor:                   m.db.config.CycleManagerRoutinesFactor,
			IndexRangeableInMemory:                       m.db.config.IndexRangeableInMemory,
			MaxSegmentSize:                               m.db.config.MaxSegmentSize,
			TrackVectorDimensions:                        m.db.config.TrackVectorDimensions,
			AvoidMMap:                                    m.db.config.AvoidMMap,
			DisableLazyLoadShards:                        m.db.config.DisableLazyLoadShards,
			ForceFullReplicasSearch:                      m.db.config.ForceFullReplicasSearch,
			TransferInactivityTimeout:                    m.db.config.TransferInactivityTimeout,
			LSMEnableSegmentsChecksumValidation:          m.db.config.LSMEnableSegmentsChecksumValidation,
			ReplicationFactor:                            class.ReplicationConfig.Factor,
			AsyncReplicationEnabled:                      class.ReplicationConfig.AsyncEnabled,
			DeletionStrategy:                             class.ReplicationConfig.DeletionStrategy,
			ShardLoadLimiter:                             m.db.shardLoadLimiter,
			HNSWMaxLogSize:                               m.db.config.HNSWMaxLogSize,
			HNSWDisableSnapshots:                         m.db.config.HNSWDisableSnapshots,
			HNSWSnapshotIntervalSeconds:                  m.db.config.HNSWSnapshotIntervalSeconds,
			HNSWSnapshotOnStartup:                        m.db.config.HNSWSnapshotOnStartup,
			HNSWSnapshotMinDeltaCommitlogsNumber:         m.db.config.HNSWSnapshotMinDeltaCommitlogsNumber,
			HNSWSnapshotMinDeltaCommitlogsSizePercentage: m.db.config.HNSWSnapshotMinDeltaCommitlogsSizePercentage,
			HNSWWaitForCachePrefill:                      m.db.config.HNSWWaitForCachePrefill,
			HNSWFlatSearchConcurrency:                    m.db.config.HNSWFlatSearchConcurrency,
			HNSWAcornFilterRatio:                         m.db.config.HNSWAcornFilterRatio,
			VisitedListPoolMaxSize:                       m.db.config.VisitedListPoolMaxSize,
			QuerySlowLogEnabled:                          m.db.config.QuerySlowLogEnabled,
			QuerySlowLogThreshold:                        m.db.config.QuerySlowLogThreshold,
			InvertedSorterDisabled:                       m.db.config.InvertedSorterDisabled,
		},
		shardState,
		// no backward-compatibility check required, since newly added classes will
		// always have the field set
		inverted.ConfigFromModel(class.InvertedIndexConfig),
		convertToVectorIndexConfig(class.VectorIndexConfig),
		convertToVectorIndexConfigs(class.VectorConfig),
		m.db.router, m.db.schemaGetter, m.db, m.logger, m.db.nodeResolver, m.db.remoteIndex,
		m.db.replicaClient, &m.db.config.Replication, m.db.promMetrics, class, m.db.jobQueueCh, m.db.scheduler, m.db.indexCheckpoints,
		m.db.memMonitor, m.db.reindexer)
	if err != nil {
		return errors.Wrap(err, "create index")
	}

	m.db.indexLock.Lock()
	m.db.indices[idx.ID()] = idx
	m.db.indexLock.Unlock()

	return nil
}

func (m *Migrator) DropClass(ctx context.Context, className string, hasFrozen bool) error {
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	if err := m.db.DeleteIndex(schema.ClassName(className)); err != nil {
		return err
	}

	if m.cloud != nil && hasFrozen {
		return m.cloud.Delete(ctx, className, "", "")
	}

	return nil
}

func (m *Migrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	if newClassName != nil {
		return errors.New("weaviate does not support renaming of classes")
	}

	return nil
}

func (m *Migrator) LoadShard(ctx context.Context, class, shard string) error {
	idx := m.db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("could not find collection %s", class)
	}
	return idx.LoadLocalShard(ctx, shard, false)
}

func (m *Migrator) DropShard(ctx context.Context, class, shard string) error {
	idx := m.db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("could not find collection %s", class)
	}
	return idx.dropShards([]string{shard})
}

func (m *Migrator) ShutdownShard(ctx context.Context, class, shard string) error {
	idx := m.db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return fmt.Errorf("could not find collection %s", class)
	}

	idx.shardCreateLocks.Lock(shard)
	defer idx.shardCreateLocks.Unlock(shard)

	shardLike, ok := idx.shards.LoadAndDelete(shard)
	if !ok {
		return fmt.Errorf("could not find shard %s", shard)
	}
	if err := shardLike.Shutdown(ctx); err != nil {
		if !errors.Is(err, errAlreadyShutdown) {
			return errors.Wrapf(err, "shutdown shard %q", shard)
		}
		idx.logger.WithField("shard", shardLike.Name()).Debug("was already shut or dropped")
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
			if err := m.updateIndexTenants(ctx, idx, incomingSS); err != nil {
				return err
			}
		} else {
			if err := m.updateIndexShards(ctx, idx, incomingSS); err != nil {
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
	incomingSS *sharding.State,
) error {
	if err := m.updateIndexTenantsStatus(ctx, idx, incomingSS); err != nil {
		return err
	}
	return m.updateIndexDeleteTenants(ctx, idx, incomingSS)
}

func (m *Migrator) updateIndexTenantsStatus(ctx context.Context, idx *Index,
	incomingSS *sharding.State,
) error {
	nodeName := m.db.schemaGetter.NodeName()
	for shardName, phys := range incomingSS.Physical {
		if !phys.IsLocalShard(nodeName) {
			continue
		}

		if phys.Status == models.TenantActivityStatusHOT {
			// Only load the tenant if activity status == HOT.
			if err := idx.LoadLocalShard(ctx, shardName, false); err != nil {
				return fmt.Errorf("add missing tenant shard %s during update index: %w", shardName, err)
			}
		} else {
			// Shutdown the tenant if activity status != HOT
			if err := idx.UnloadLocalShard(ctx, shardName); err != nil {
				return fmt.Errorf("shutdown tenant shard %s during update index: %w", shardName, err)
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

	if len(toRemove) == 0 {
		return nil
	}

	if err := idx.dropShards(toRemove); err != nil {
		return fmt.Errorf("drop tenant shards %v during update index: %w", toRemove, err)
	}

	if m.cloud != nil {
		// TODO-offload: currently we send all tenants and if it did find one in the cloud will delete
		// better to filter the passed shards and get the frozen only
		if err := idx.dropCloudShards(ctx, m.cloud, toRemove, m.nodeId); err != nil {
			return fmt.Errorf("drop tenant shards %v during update index: %w", toRemove, err)
		}
	}

	return nil
}

func (m *Migrator) updateIndexShards(ctx context.Context, idx *Index,
	incomingSS *sharding.State,
) error {
	requestedShards := incomingSS.AllLocalPhysicalShards()
	existingShards := make(map[string]ShardLike)

	if err := idx.ForEachShard(func(name string, shard ShardLike) error {
		existingShards[name] = shard
		return nil
	}); err != nil {
		return fmt.Errorf("failed to iterate over loaded shards: %w", err)
	}

	// Initialize missing shards and shutdown unneeded ones
	for shardName := range existingShards {
		if !slices.Contains(requestedShards, shardName) {
			if err := idx.UnloadLocalShard(ctx, shardName); err != nil {
				// TODO: an error should be returned but keeping the old behavior for now
				m.logger.WithField("shard", shardName).Error("shutdown shard during update index: %w", err)
				continue
			}
		}
	}

	for _, shardName := range requestedShards {
		if _, exists := existingShards[shardName]; !exists {
			if err := idx.initLocalShard(ctx, shardName); err != nil {
				return fmt.Errorf("add missing shard %s during update index: %w", shardName, err)
			}
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
		// Ensure we iterate over loaded shard to avoid force loading a lazy loaded shard
		err := idx.ForEachLoadedShard(func(name string, shard ShardLike) error {
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
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

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
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, errors.Errorf("cannot get shards status for a non-existing index for %s", className)
	}

	return idx.getShardsQueueSize(ctx, tenant)
}

func (m *Migrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, errors.Errorf("cannot get shards status for a non-existing index for %s", className)
	}

	return idx.getShardsStatus(ctx, tenant)
}

func (m *Migrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string, schemaVersion uint64) error {
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update shard status to a non-existing index for %s", className)
	}

	return idx.updateShardStatus(ctx, shardName, targetStatus, schemaVersion)
}

// NewTenants creates new partitions
func (m *Migrator) NewTenants(ctx context.Context, class *models.Class, creates []*schemaUC.CreateTenantPayload) error {
	indexID := indexID(schema.ClassName(class.Class))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return fmt.Errorf("cannot find index for %q", class.Class)
	}

	ec := errorcompounder.New()
	for _, pl := range creates {
		if pl.Status != models.TenantActivityStatusHOT {
			continue // skip creating inactive shards
		}

		err := idx.initLocalShard(ctx, pl.Name)
		ec.Add(err)
	}
	return ec.ToError()
}

// UpdateTenants activates or deactivates tenant partitions and returns a commit func
// that can be used to either commit or rollback the changes
func (m *Migrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*schemaUC.UpdateTenantPayload, implicitTenantActivation bool) error {
	indexID := indexID(schema.ClassName(class.Class))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(class.Class))
	if idx == nil {
		return fmt.Errorf("cannot find index for %q", class.Class)
	}

	hot := make([]string, 0, len(updates))
	cold := make([]string, 0, len(updates))
	freezing := make([]string, 0, len(updates))
	frozen := make([]string, 0, len(updates))
	unfreezing := make([]string, 0, len(updates))

	for _, tenant := range updates {
		switch tenant.Status {
		case models.TenantActivityStatusHOT:
			hot = append(hot, tenant.Name)
		case models.TenantActivityStatusCOLD:
			cold = append(cold, tenant.Name)
		case models.TenantActivityStatusFROZEN:
			frozen = append(frozen, tenant.Name)

		case types.TenantActivityStatusFREEZING: // never arrives from user
			freezing = append(freezing, tenant.Name)
		case types.TenantActivityStatusUNFREEZING: // never arrives from user
			unfreezing = append(unfreezing, tenant.Name)
		}
	}

	ec := errorcompounder.NewSafe()
	if len(hot) > 0 {
		m.logger.WithField("action", "tenants_to_hot").Debug(hot)
		idx.shardTransferMutex.RLock()
		defer idx.shardTransferMutex.RUnlock()

		eg := enterrors.NewErrorGroupWrapper(m.logger)
		eg.SetLimit(_NUMCPU * 2)

		for _, name := range hot {
			name := name // prevent loop variable capture
			// enterrors.GoWrapper(func() {
			// The timeout is rather arbitrary. It's meant to be so high that it can
			// never stop a valid tenant activation use case, but low enough to
			// prevent a context-leak.

			eg.Go(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
				defer cancel()

				if err := idx.LoadLocalShard(ctx, name, implicitTenantActivation); err != nil {
					ec.Add(err)
					idx.logger.WithFields(logrus.Fields{
						"action": "tenant_activation_lazy_load_shard",
						"shard":  name,
					}).WithError(err).Errorf("loading shard %q failed", name)
				}
				return nil
			})
		}

		eg.Wait()
	}

	if len(cold) > 0 {
		m.logger.WithField("action", "tenants_to_cold").Debug(cold)
		idx.shardTransferMutex.RLock()
		defer idx.shardTransferMutex.RUnlock()

		eg := enterrors.NewErrorGroupWrapper(m.logger)
		eg.SetLimit(_NUMCPU * 2)

		for _, name := range cold {
			name := name

			eg.Go(func() error {
				idx.closeLock.RLock()
				defer idx.closeLock.RUnlock()

				if idx.closed {
					m.logger.WithField("index", idx.ID()).Debug("index is already shut down or dropped")
					ec.Add(errAlreadyShutdown)
					return nil
				}

				idx.shardCreateLocks.Lock(name)
				defer idx.shardCreateLocks.Unlock(name)

				shard, ok := idx.shards.LoadAndDelete(name)
				if !ok {
					m.logger.WithField("shard", name).Debug("already shut down or dropped")
					return nil // shard already does not exist or inactive
				}

				m.logger.WithField("shard", name).Debug("starting shutdown")

				if err := shard.Shutdown(ctx); err != nil {
					if errors.Is(err, errAlreadyShutdown) {
						m.logger.WithField("shard", shard.Name()).Debug("already shut down or dropped")
					} else {
						idx.logger.
							WithField("action", "shard_shutdown").
							WithField("shard", shard.ID()).
							Error(err)
						ec.Add(err)
					}
				}

				return nil
			})
		}

		eg.Wait()
	}

	if len(frozen) > 0 {
		m.logger.WithField("action", "tenants_to_frozen").Debug(frozen)
		m.frozen(ctx, idx, frozen, ec)
	}

	if len(freezing) > 0 {
		m.logger.WithField("action", "tenants_to_freezing").Debug(freezing)
		m.freeze(ctx, idx, class.Class, freezing, ec)
	}

	if len(unfreezing) > 0 {
		m.logger.WithField("action", "tenants_to_unfreezing").Debug(unfreezing)
		m.unfreeze(ctx, idx, class.Class, unfreezing, ec)
	}

	return ec.ToError()
}

// DeleteTenants deletes tenants
// CAUTION: will not delete inactive tenants (shard files will not be removed)
func (m *Migrator) DeleteTenants(ctx context.Context, class string, tenants []*models.Tenant) error {
	indexID := indexID(schema.ClassName(class))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(class))
	if idx == nil {
		return nil
	}

	// Collect tenant names and frozen tenant names
	allTenantNames := make([]string, 0, len(tenants))
	frozenTenants := make([]string, 0, len(tenants))

	for _, tenant := range tenants {
		allTenantNames = append(allTenantNames, tenant.Name)
		if tenant.ActivityStatus == models.TenantActivityStatusFROZEN ||
			tenant.ActivityStatus == models.TenantActivityStatusFREEZING {
			frozenTenants = append(frozenTenants, tenant.Name)
		}
	}

	if err := idx.dropShards(allTenantNames); err != nil {
		return err
	}

	if m.cloud != nil && len(frozenTenants) > 0 {
		if err := idx.dropCloudShards(ctx, m.cloud, frozenTenants, m.nodeId); err != nil {
			return fmt.Errorf("drop tenant shards %v during update index: %w", frozenTenants, err)
		}
	}

	return nil
}

func (m *Migrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schemaConfig.VectorIndexConfig,
) error {
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update vector index config of non-existing index for %s", className)
	}

	return idx.updateVectorIndexConfig(ctx, updated)
}

func (m *Migrator) UpdateVectorIndexConfigs(ctx context.Context,
	className string, updated map[string]schemaConfig.VectorIndexConfig,
) error {
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

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
	return fmt.Errorf("invalid index type: %s", old.IndexType())
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
	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update inverted index config of non-existing index for %s", className)
	}

	conf := inverted.ConfigFromModel(updated)

	return idx.updateInvertedIndexConfig(ctx, conf)
}

func (m *Migrator) UpdateReplicationConfig(ctx context.Context, className string, cfg *models.ReplicationConfig) error {
	if cfg == nil {
		return nil
	}

	indexID := indexID(schema.ClassName(className))

	m.classLocks.Lock(indexID)
	defer m.classLocks.Unlock(indexID)

	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update replication factor of non-existing index for %s", className)
	}

	if err := idx.updateReplicationConfig(ctx, cfg); err != nil {
		return fmt.Errorf("update replication config for class %q: %w", className, err)
	}

	return nil
}

func (m *Migrator) RecalculateVectorDimensions(ctx context.Context) error {
	count := 0
	m.logger.
		WithField("action", "reindex").
		Info("Reindexing dimensions, this may take a while")

	m.db.indexLock.Lock()
	defer m.db.indexLock.Unlock()

	// Iterate over all indexes
	for _, index := range m.db.indices {
		err := index.ForEachShard(func(name string, shard ShardLike) error {
			return shard.resetDimensionsLSM()
		})
		if err != nil {
			m.logger.WithField("action", "reindex").WithError(err).Warn("could not reset vector dimensions")
			return err
		}

		// Iterate over all shards
		err = index.IterateObjects(ctx, func(index *Index, shard ShardLike, object *storobj.Object) error {
			count = count + 1
			return object.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
				if err = shard.extendDimensionTrackerLSM(dims, object.DocID, targetVector); err != nil {
					return fmt.Errorf("failed to extend dimension tracker for vector %q: %w", targetVector, err)
				}
				return nil
			})
		})
		if err != nil {
			m.logger.WithField("action", "reindex").WithError(err).Warn("could not extend vector dimensions")
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
		err := index.IterateShards(ctx, func(index *Index, shard ShardLike) error {
			shard.GetPropertyLengthTracker().Clear()
			return nil
		})
		if err != nil {
			m.logger.WithField("error", err).Error("could not clear prop lengths")
		}

		// Iterate over all shards
		err = index.IterateObjects(ctx, func(index *Index, shard ShardLike, object *storobj.Object) error {
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
		if err != nil {
			m.logger.WithField("error", err).Error("could not iterate over objects")
		}

		// Flush the GetPropertyLengthTracker() to disk
		err = index.IterateShards(ctx, func(index *Index, shard ShardLike) error {
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

func (m *Migrator) InvertedReindex(ctx context.Context, taskNamesWithArgs map[string]any) error {
	var errs errorcompounder.ErrorCompounder
	errs.Add(m.doInvertedReindex(ctx, taskNamesWithArgs))
	errs.Add(m.doInvertedIndexMissingTextFilterable(ctx, taskNamesWithArgs))
	return errs.ToError()
}

func (m *Migrator) doInvertedReindex(ctx context.Context, taskNamesWithArgs map[string]any) error {
	tasks := map[string]ShardInvertedReindexTask{}
	for name, args := range taskNamesWithArgs {
		switch name {
		case "ShardInvertedReindexTaskSetToRoaringSet":
			tasks[name] = &ShardInvertedReindexTaskSetToRoaringSet{}
		case "ShardInvertedReindexTask_SpecifiedIndex":
			if args == nil {
				return fmt.Errorf("no args given for %q reindex task", name)
			}
			argsMap, ok := args.(map[string][]string)
			if !ok {
				return fmt.Errorf("invalid args given for %q reindex task", name)
			}
			classNamesWithPropertyNames := map[string]map[string]struct{}{}
			for class, props := range argsMap {
				classNamesWithPropertyNames[class] = map[string]struct{}{}
				for _, prop := range props {
					classNamesWithPropertyNames[class][prop] = struct{}{}
				}
			}
			tasks[name] = &ShardInvertedReindexTask_SpecifiedIndex{
				classNamesWithPropertyNames: classNamesWithPropertyNames,
			}
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

func (m *Migrator) doInvertedIndexMissingTextFilterable(ctx context.Context, taskNamesWithArgs map[string]any) error {
	if _, ok := taskNamesWithArgs["ShardInvertedReindexTaskMissingTextFilterable"]; !ok {
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
