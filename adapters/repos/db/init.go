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
	"os"
	"path"
	"sync/atomic"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/tenantactivity"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
				}
			}
			if err := replica.ValidateConfig(class, db.config.Replication); err != nil {
				return fmt.Errorf("replication config: %w", err)
			}

			idx, err := NewIndex(ctx, IndexConfig{
				ClassName:                 schema.ClassName(class.Class),
				RootPath:                  db.config.RootPath,
				ResourceUsage:             db.config.ResourceUsage,
				QueryMaximumResults:       db.config.QueryMaximumResults,
				QueryNestedRefLimit:       db.config.QueryNestedRefLimit,
				MemtablesFlushDirtyAfter:  db.config.MemtablesFlushDirtyAfter,
				MemtablesInitialSizeMB:    db.config.MemtablesInitialSizeMB,
				MemtablesMaxSizeMB:        db.config.MemtablesMaxSizeMB,
				MemtablesMinActiveSeconds: db.config.MemtablesMinActiveSeconds,
				MemtablesMaxActiveSeconds: db.config.MemtablesMaxActiveSeconds,
				MaxSegmentSize:            db.config.MaxSegmentSize,
				HNSWMaxLogSize:            db.config.HNSWMaxLogSize,
				HNSWWaitForCachePrefill:   db.config.HNSWWaitForCachePrefill,
				TrackVectorDimensions:     db.config.TrackVectorDimensions,
				AvoidMMap:                 db.config.AvoidMMap,
				DisableLazyLoadShards:     db.config.DisableLazyLoadShards,
				ReplicationFactor:         NewAtomicInt64(class.ReplicationConfig.Factor),
				AsyncReplicationEnabled:   class.ReplicationConfig.AsyncEnabled,
			}, db.schemaGetter.CopyShardingState(class.Class),
				inverted.ConfigFromModel(invertedConfig),
				convertToVectorIndexConfig(class.VectorIndexConfig),
				convertToVectorIndexConfigs(class.VectorConfig),
				db.schemaGetter, db, db.logger, db.nodeResolver, db.remoteIndex,
				db.replicaClient, db.promMetrics, class, db.jobQueueCh, db.indexCheckpoints,
				db.memMonitor)
			if err != nil {
				return errors.Wrap(err, "create index")
			}

			db.indexLock.Lock()
			db.indices[idx.ID()] = idx
			db.indexLock.Unlock()
		}
	}

	// If metrics aren't grouped, there is no need to observe node-wide metrics
	// asynchronously. In that case, each shard could track its own metrics with
	// a unique label. It is only when we conflate all collections/shards into
	// "n/a" that we need to actively aggregate node-wide metrics.
	//
	// See also https://github.com/weaviate/weaviate/issues/4396
	if db.promMetrics != nil && db.promMetrics.Group {
		db.metricsObserver = newNodeWideMetricsObserver(db)
		enterrors.GoWrapper(func() { db.metricsObserver.Start() }, db.logger)
	}

	return nil
}

func (db *DB) LocalTenantActivity() tenantactivity.ByCollection {
	return db.metricsObserver.Usage()
}

func (db *DB) migrateFileStructureIfNecessary() error {
	fsMigrationPath := path.Join(db.config.RootPath, "migration1.22.fs.hierarchy")
	exists, err := fileExists(fsMigrationPath)
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

	if err := migratefs.MigrateToHierarchicalFS(db.config.RootPath, db.schemaGetter); err != nil {
		return err
	}
	db.logger.WithField("action", "hierarchical_fs_migration").
		Debugf("fs migration took %s\n", time.Since(before))
	return nil
}

func fileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func NewAtomicInt64(val int64) *atomic.Int64 {
	aval := &atomic.Int64{}
	aval.Store(val)
	return aval
}
