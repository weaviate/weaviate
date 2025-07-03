//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/usage/types"
	backupent "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/schema"
)

type Service interface {
	Usage(ctx context.Context) (*types.Report, error)
}

type service struct {
	schemaManager schema.SchemaGetter
	db            db.IndexGetter
	backups       backup.BackupBackendProvider
	logger        logrus.FieldLogger
}

func NewService(schemaManager schema.SchemaGetter, db db.IndexGetter, backups backup.BackupBackendProvider, logger logrus.FieldLogger) Service {
	return &service{
		schemaManager: schemaManager,
		db:            db,
		backups:       backups,
		logger:        logger,
	}
}

// Usage service collects usage metrics for the node and shall return error in case of any error
// to avoid reporting partial data
func (m *service) Usage(ctx context.Context) (*Report, error) {
	collections := m.schemaManager.GetSchemaSkipAuth().Objects.Classes
	usage := &types.Report{
		Node:        m.schemaManager.NodeName(),
		Collections: make([]*types.CollectionUsage, 0, len(collections)),
		Backups:     make([]*types.BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, collection := range collections {
		shardingState := m.schemaManager.CopyShardingState(collection.Class)
		collectionUsage := &types.CollectionUsage{
			Name:              collection.Class,
			ReplicationFactor: int(collection.ReplicationConfig.Factor),
			UniqueShardCount:  int(len(shardingState.Physical)),
			Shards:            make([]*types.ShardUsage, 0),
		}
		// Get shard usage
		index := m.db.GetIndexLike(entschema.ClassName(collection.Class))
		if index != nil {
			// First, collect cold tenants from sharding state
			coldTenants := make(map[string]*types.ShardUsage)
			for tenantName, physical := range shardingState.Physical {
				// skip non-local shards
				if !shardingState.IsLocalShard(tenantName) {
					continue
				}

				// Only process COLD tenants here
				if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
					// Cold tenant: calculate from disk without loading
					objectUsage, err := index.CalculateUnloadedObjectsMetrics(ctx, tenantName)
					if err != nil {
						return nil, err
					}

					vectorStorageSize, err := index.CalculateUnloadedVectorsMetrics(ctx, tenantName)
					if err != nil {
						return nil, err
					}

					shardUsage := &types.ShardUsage{
						Name:                tenantName,
						ObjectsCount:        objectUsage.Count,
						ObjectsStorageBytes: uint64(objectUsage.StorageBytes),
						VectorStorageBytes:  uint64(vectorStorageSize),
						NamedVectors:        make([]*types.VectorUsage, 0), // Empty for cold tenants
					}
					coldTenants[tenantName] = shardUsage
				}
			}

			// Then, collect hot tenants from loaded shards
			index.ForEachShard(func(name string, shard db.ShardLike) error {
				// skip non-local shards
				if !shardingState.IsLocalShard(name) {
					return nil
				}

				objectStorageSize, err := shard.ObjectStorageSize(ctx)
				if err != nil {
					return err
				}
				objectCount, err := shard.ObjectCountAsync(ctx)
				if err != nil {
					return err
				}

				vectorStorageSize, err := shard.VectorStorageSize(ctx)
				if err != nil {
					return err
				}

				shardUsage := &types.ShardUsage{
					Name:                name,
					ObjectsCount:        objectCount,
					ObjectsStorageBytes: uint64(objectStorageSize),
					VectorStorageBytes:  uint64(vectorStorageSize),
					NamedVectors:        make([]*types.VectorUsage, 0),
				}

				// Get vector usage for each named vector
				_ = shard.ForEachVectorIndex(func(targetVector string, vectorIndex db.VectorIndex) error {
					category := db.DimensionCategoryStandard // Default category
					indexType := ""
					if vectorIndexConfig, ok := collection.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
						category, _ = db.GetDimensionCategory(vectorIndexConfig)
						indexType = vectorIndexConfig.IndexType()
					}

					count, dimensions := shard.DimensionsUsage(ctx, targetVector)

					// Get compression ratio from vector index stats
					compressionRatio := vectorIndex.CompressionStats().CompressionRatio(dimensions)

					vectorUsage := &types.VectorUsage{
						Name:                   targetVector,
						Compression:            category.String(),
						VectorIndexType:        indexType,
						VectorCompressionRatio: vectorIndex.CompressionStats().CompressionRatio(dimensionality.Dimensions),
					}

					vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &types.Dimensionality{
						Dimensions: dimensionality.Dimensions,
						Count:      dimensionality.Count,
					})

					shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
					return nil
				})

				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
				return nil
			})

			// Add cold tenants to the collection
			for _, coldShard := range coldTenants {
				collectionUsage.Shards = append(collectionUsage.Shards, coldShard)
			}
		}

		usage.Collections = append(usage.Collections, collectionUsage)
	}

	// Get backup usage from all enabled backup backends
	for _, backend := range m.backups.EnabledBackupBackends() {
		backups, err := backend.AllBackups(ctx)
		if err != nil {
			m.logger.WithError(err).WithFields(logrus.Fields{"backend": backend}).Error("failed to get backups from backend")
			return nil, err
		}

		for _, backup := range backups {
			if backup.Status != backupent.Success {
				continue
			}
			usage.Backups = append(usage.Backups, &BackupUsage{
				ID:             backup.ID,
				CompletionTime: backup.CompletedAt.Format(time.RFC3339),
				SizeInGib:      float64(backup.PreCompressionSizeBytes) / (1024 * 1024 * 1024), // Convert bytes to GiB
				Type:           string(backup.Status),
				Collections:    backup.Classes(),
			})
		}
	}
	return usage, nil
}
