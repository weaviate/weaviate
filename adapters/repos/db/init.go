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
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/usecases/multitenancy"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tenantactivity"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/replica"
	migratefs "github.com/weaviate/weaviate/usecases/schema/migrate/fs"
)

// init gets the current schema and creates one index object per class.
// The indices will in turn create shards, which will either read an
// existing db file from disk, or create a new one if none exists
func (db *DB) init(ctx context.Context) error {
	if err := os.MkdirAll(db.config.RootPath, 0o777); err != nil {
		return fmt.Errorf("create root path directory at %s: %w", db.config.RootPath, err)
	}

	// As of v1.22, db files are stored in a hierarchical structure
	// rather than a flat one. If weaviate is started with files
	// that are still in the flat structure, we will migrate them
	// over.
	if err := db.migrateFileStructureIfNecessary(); err != nil {
		return err
	}

	if asyncEnabled() {
		// init the index checkpoint file
		var err error
		db.indexCheckpoints, err = indexcheckpoint.New(db.config.RootPath, db.logger)
		if err != nil {
			return errors.Wrap(err, "init index checkpoint")
		}
	}

	objects := db.schemaGetter.GetSchemaSkipAuth().Objects
	if objects != nil {
		for _, class := range objects.Classes {
			invertedConfig := class.InvertedIndexConfig
			if invertedConfig == nil {
				// for backward compatibility, this field was introduced in v1.0.4,
				// prior schemas will not yet have the field. Init with the defaults
				// which were previously hard-coded.
				// In this method we are essentially reading the schema from disk, so
				// it could have been created before v1.0.4
				invertedConfig = &models.InvertedIndexConfig{
					CleanupIntervalSeconds: config.DefaultCleanupIntervalSeconds,
					Bm25: &models.BM25Config{
						K1: config.DefaultBM25k1,
						B:  config.DefaultBM25b,
					},
					UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
				}
			}
			if err := replica.ValidateConfig(class, db.config.Replication); err != nil {
				return fmt.Errorf("replication config: %w", err)
			}

			collection := schema.ClassName(class.Class).String()
			indexRouter := router.NewBuilder(
				collection,
				multitenancy.IsMultiTenant(class.MultiTenancyConfig),
				db.nodeSelector,
				db.schemaGetter,
				db.schemaReader,
				db.replicationFSM,
			).Build()
			shardResolver := resolver.NewShardResolver(collection, multitenancy.IsMultiTenant(class.MultiTenancyConfig), db.schemaGetter)
			idx, err := NewIndex(ctx, IndexConfig{
				ClassName:                                    schema.ClassName(class.Class),
				RootPath:                                     db.config.RootPath,
				ResourceUsage:                                db.config.ResourceUsage,
				QueryMaximumResults:                          db.config.QueryMaximumResults,
				QueryHybridMaximumResults:                    db.config.QueryHybridMaximumResults,
				QueryNestedRefLimit:                          db.config.QueryNestedRefLimit,
				MemtablesFlushDirtyAfter:                     db.config.MemtablesFlushDirtyAfter,
				MemtablesInitialSizeMB:                       db.config.MemtablesInitialSizeMB,
				MemtablesMaxSizeMB:                           db.config.MemtablesMaxSizeMB,
				MemtablesMinActiveSeconds:                    db.config.MemtablesMinActiveSeconds,
				MemtablesMaxActiveSeconds:                    db.config.MemtablesMaxActiveSeconds,
				MinMMapSize:                                  db.config.MinMMapSize,
				LazySegmentsDisabled:                         db.config.LazySegmentsDisabled,
				SegmentInfoIntoFileNameEnabled:               db.config.SegmentInfoIntoFileNameEnabled,
				WriteMetadataFilesEnabled:                    db.config.WriteMetadataFilesEnabled,
				MaxReuseWalSize:                              db.config.MaxReuseWalSize,
				SegmentsCleanupIntervalSeconds:               db.config.SegmentsCleanupIntervalSeconds,
				SeparateObjectsCompactions:                   db.config.SeparateObjectsCompactions,
				CycleManagerRoutinesFactor:                   db.config.CycleManagerRoutinesFactor,
				IndexRangeableInMemory:                       db.config.IndexRangeableInMemory,
				MaxSegmentSize:                               db.config.MaxSegmentSize,
				TrackVectorDimensions:                        db.config.TrackVectorDimensions,
				TrackVectorDimensionsInterval:                db.config.TrackVectorDimensionsInterval,
				UsageEnabled:                                 db.config.UsageEnabled,
				AvoidMMap:                                    db.config.AvoidMMap,
				DisableLazyLoadShards:                        db.config.DisableLazyLoadShards,
				ForceFullReplicasSearch:                      db.config.ForceFullReplicasSearch,
				TransferInactivityTimeout:                    db.config.TransferInactivityTimeout,
				LSMEnableSegmentsChecksumValidation:          db.config.LSMEnableSegmentsChecksumValidation,
				ReplicationFactor:                            class.ReplicationConfig.Factor,
				AsyncReplicationEnabled:                      class.ReplicationConfig.AsyncEnabled,
				AsyncReplicationConfig:                       class.ReplicationConfig.AsyncConfig,
				DeletionStrategy:                             class.ReplicationConfig.DeletionStrategy,
				ShardLoadLimiter:                             db.shardLoadLimiter,
				HNSWMaxLogSize:                               db.config.HNSWMaxLogSize,
				HNSWDisableSnapshots:                         db.config.HNSWDisableSnapshots,
				HNSWSnapshotIntervalSeconds:                  db.config.HNSWSnapshotIntervalSeconds,
				HNSWSnapshotOnStartup:                        db.config.HNSWSnapshotOnStartup,
				HNSWSnapshotMinDeltaCommitlogsNumber:         db.config.HNSWSnapshotMinDeltaCommitlogsNumber,
				HNSWSnapshotMinDeltaCommitlogsSizePercentage: db.config.HNSWSnapshotMinDeltaCommitlogsSizePercentage,
				HNSWWaitForCachePrefill:                      db.config.HNSWWaitForCachePrefill,
				HNSWFlatSearchConcurrency:                    db.config.HNSWFlatSearchConcurrency,
				HNSWAcornFilterRatio:                         db.config.HNSWAcornFilterRatio,
				HNSWGeoIndexEF:                               db.config.HNSWGeoIndexEF,
				VisitedListPoolMaxSize:                       db.config.VisitedListPoolMaxSize,
				QuerySlowLogEnabled:                          db.config.QuerySlowLogEnabled,
				QuerySlowLogThreshold:                        db.config.QuerySlowLogThreshold,
				InvertedSorterDisabled:                       db.config.InvertedSorterDisabled,
				MaintenanceModeEnabled:                       db.config.MaintenanceModeEnabled,
				SPFreshEnabled:                               db.config.SPFreshEnabled,
			},
				inverted.ConfigFromModel(invertedConfig),
				convertToVectorIndexConfig(class.VectorIndexConfig),
				convertToVectorIndexConfigs(class.VectorConfig),
				indexRouter, shardResolver, db.schemaGetter, db.schemaReader, db, db.logger, db.nodeResolver, db.remoteIndex,
				db.replicaClient, &db.config.Replication, db.promMetrics, class, db.jobQueueCh, db.scheduler, db.indexCheckpoints,
				db.memMonitor, db.reindexer, db.bitmapBufPool)
			if err != nil {
				return errors.Wrap(err, "create index")
			}

			db.indexLock.Lock()
			db.indices[idx.ID()] = idx
			db.indexLock.Unlock()
		}
	}

	// Collecting metrics that _can_ be aggregated on a node level,
	// i.e. replacing className and shardName labels with "n/a",
	// should be delegated to nodeWideMetricsObserver to centralize
	// control over how these metrics are aggregated.
	//
	// See also https://github.com/weaviate/weaviate/issues/4396
	//
	// NB: nodeWideMetricsObserver only tracks object_count if
	// node-level aggregation is enabled -- a decision made during
	// its original implementation.
	if db.promMetrics != nil {
		db.metricsObserver = newNodeWideMetricsObserver(db)
		db.metricsObserver.Start()
	}

	return nil
}

func (db *DB) LocalTenantActivity(filter tenantactivity.UsageFilter) tenantactivity.ByCollection {
	return db.metricsObserver.Usage(filter)
}

func (db *DB) migrateFileStructureIfNecessary() error {
	fsMigrationPath := path.Join(db.config.RootPath, "migration1.22.fs.hierarchy")
	exists, err := diskio.FileExists(fsMigrationPath)
	if err != nil {
		return err
	}
	if !exists {
		if err = db.migrateToHierarchicalFS(); err != nil {
			return fmt.Errorf("migrate to hierarchical fs: %w", err)
		}
		if _, err = os.Create(fsMigrationPath); err != nil {
			return fmt.Errorf("create hierarchical fs indicator: %w", err)
		}
	}
	return nil
}

func (db *DB) migrateToHierarchicalFS() error {
	before := time.Now()

	if err := migratefs.MigrateToHierarchicalFS(db.config.RootPath, db.schemaReader); err != nil {
		return err
	}
	db.logger.WithField("action", "hierarchical_fs_migration").
		Debugf("fs migration took %s\n", time.Since(before))
	return nil
}
