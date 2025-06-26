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

	"github.com/weaviate/weaviate/adapters/repos/db"
	backupent "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/schema"
)

type Service interface {
	Usage(ctx context.Context) (*Report, error)
}

type service struct {
	schemaManager schema.SchemaGetter
	db            db.IndexGetter
	backups       backup.BackupBackendProvider
}

func NewService(schemaManager schema.SchemaGetter, db db.IndexGetter, backups backup.BackupBackendProvider) Service {
	return &service{
		schemaManager: schemaManager,
		db:            db,
		backups:       backups,
	}
}

func (m *service) Usage(ctx context.Context) (*Report, error) {
	collections := m.schemaManager.GetSchemaSkipAuth().Objects.Classes
	usage := &Report{
		Node:        m.schemaManager.NodeName(),
		Collections: make([]*CollectionUsage, 0, len(collections)),
		Backups:     make([]*BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, collection := range collections {
		shardingState := m.schemaManager.CopyShardingState(collection.Class)
		collectionUsage := &CollectionUsage{
			Name:              collection.Class,
			ReplicationFactor: int(collection.ReplicationConfig.Factor),
			UniqueShardCount:  int(len(shardingState.Physical)),
			Shards:            make([]*ShardUsage, 0),
		}
		// Get shard usage
		index := m.db.GetIndexLike(entschema.ClassName(collection.Class))
		if index != nil {
			// First, collect cold tenants from sharding state
			coldTenants := make(map[string]*ShardUsage)
			for tenantName, physical := range shardingState.Physical {
				// skip non-local shards
				if !shardingState.IsLocalShard(tenantName) {
					continue
				}

				// Only process COLD tenants here
				if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
					// Cold tenant: calculate from disk without loading
					objectCount, storageSize := index.CalculateUnloadedObjectsMetrics(ctx, tenantName)
					shardUsage := &ShardUsage{
						Name:                tenantName,
						ObjectsCount:        int(objectCount),
						ObjectsStorageBytes: uint64(storageSize),
						VectorStorageBytes:  uint64(index.CalculateColdTenantVectorStorageSize(ctx, tenantName)),
						NamedVectors:        make([]*VectorUsage, 0), // Empty for cold tenants
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

				shardUsage := &ShardUsage{
					Name:                name,
					ObjectsCount:        shard.ObjectCountAsync(),
					ObjectsStorageBytes: uint64(shard.ObjectStorageSize(ctx)),
					VectorStorageBytes:  uint64(shard.VectorStorageSize(ctx)),
					NamedVectors:        make([]*VectorUsage, 0),
				}

				// Get vector usage for each named vector
				_ = shard.ForEachVectorIndex(func(targetVector string, vectorIndex db.VectorIndex) error {
					category := db.DimensionCategoryStandard // Default category
					indexType := ""
					if vectorIndexConfig, ok := collection.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
						category, _ = db.GetDimensionCategory(vectorIndexConfig)
						indexType = vectorIndexConfig.IndexType()
					}

					dimensions, objects := shard.DimensionsUsage(ctx, targetVector)
					// Get compression ratio from vector index stats
					var compressionRatio float64
					if compressionStats, err := vectorIndex.CompressionStats(); err == nil {
						// TODO log error
						compressionRatio = compressionStats.CompressionRatio(dimensions)
					}

					vectorUsage := &VectorUsage{
						Name:                   targetVector,
						Compression:            category.String(),
						VectorIndexType:        indexType,
						VectorCompressionRatio: compressionRatio,
					}

					vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &DimensionalityUsage{
						Dimensionality: dimensions,
						Count:          objects,
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
		if err == nil {
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
	}
	return usage, nil
}
