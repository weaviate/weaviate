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
	entschema "github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/schema"
)

type Service interface {
	Usage(ctx context.Context) (*Report, error)
}

type service struct {
	schemaManager *schema.Manager
	db            *db.DB
	modules       *modules.Provider
}

func NewService(schemaManager *schema.Manager, db *db.DB, modules *modules.Provider) Service {
	return &service{
		schemaManager: schemaManager,
		db:            db,
		modules:       modules,
	}
}

func (m *service) Usage(ctx context.Context) (*Report, error) {
	collections := m.schemaManager.GetSchemaSkipAuth().Objects.Classes
	usage := &Report{
		Node:                    m.schemaManager.NodeName(),
		SingleTenantCollections: make([]*CollectionUsage, 0, len(collections)),
		Backups:                 make([]*BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, collection := range collections {
		vectorIndexConfig := collection.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		// vectorIndexConfig := collection.VectorConfig.
		shardingState := m.schemaManager.CopyShardingState(collection.Class)
		collectionUsage := &CollectionUsage{
			Name:              collection.Class,
			ReplicationFactor: int(collection.ReplicationConfig.Factor),
			UniqueShardCount:  int(len(shardingState.Physical)),
			Shards:            make([]*ShardUsage, 0),
		}
		// Get shard usage
		index := m.db.GetIndex(entschema.ClassName(collection.Class))
		if index != nil {
			// TODO: this will load all shards into memory, which is not efficient
			// we shall collect usage for each shard without loading them into memory
			index.ForEachShard(func(name string, shard db.ShardLike) error {
				// skip non-local shards
				if !shardingState.IsLocalShard(name) {
					return nil
				}

				shardUsage := &ShardUsage{
					Name:                name,
					ObjectsCount:        shard.ObjectCountAsync(),
					ObjectsStorageBytes: shard.ObjectStorageSize(ctx),
					NamedVectors:        make([]*VectorUsage, 0),
				}

				// Get vector usage for each named vector
				_ = shard.ForEachVectorIndex(func(targetVector string, vectorIndex db.VectorIndex) error {
					category, _ := db.GetDimensionCategory(vectorIndexConfig)
					dimensions, objects := shard.DimensionsUsage(ctx, targetVector)
					// Get compression ratio from vector index stats
					var compressionRatio float64
					if compressionStats, err := vectorIndex.CompressionStats(); err == nil {
						// TODO log error
						compressionRatio = compressionStats.CompressionRatio(dimensions)
					}

					vectorUsage := &VectorUsage{
						Name:                   targetVector,
						VectorIndexType:        vectorIndexConfig.IndexType(),
						Compression:            category.String(),
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
		}

		usage.SingleTenantCollections = append(usage.SingleTenantCollections, collectionUsage)
	}

	// Get backup usage from all enabled backup backends
	for _, backend := range m.modules.EnabledBackupBackends() {
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
