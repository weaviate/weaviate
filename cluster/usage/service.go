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
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	backupent "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/schema"
)

type Service interface {
	Usage(ctx context.Context) (*types.Report, error)
	SetJitterInterval(interval time.Duration)
}

type service struct {
	schemaManager  schema.SchemaGetter
	db             db.IndexGetter
	backups        backup.BackupBackendProvider
	logger         logrus.FieldLogger
	jitterInterval time.Duration
}

func NewService(schemaManager schema.SchemaGetter, db db.IndexGetter, backups backup.BackupBackendProvider, logger logrus.FieldLogger) Service {
	return &service{
		schemaManager:  schemaManager,
		db:             db,
		backups:        backups,
		logger:         logger,
		jitterInterval: 0, // Default to no jitter
	}
}

// SetJitterInterval sets the jitter interval for shard processing
func (s *service) SetJitterInterval(interval time.Duration) {
	s.jitterInterval = interval
	s.logger.WithFields(logrus.Fields{"jitter_interval": interval.String()}).Info("shard jitter interval updated")
}

// addJitter adds a small random delay if jitter interval is set
func (s *service) addJitter() {
	if s.jitterInterval <= 0 {
		return // No jitter if interval is 0 or negative
	}
	jitter := time.Duration(time.Now().UnixNano() % int64(s.jitterInterval))
	time.Sleep(jitter)
}

// Usage service collects usage metrics for the node and shall return error in case of any error
// to avoid reporting partial data
func (m *service) Usage(ctx context.Context) (*types.Report, error) {
	schema := m.schemaManager.GetSchemaSkipAuth().Objects
	collections := schema.Classes
	usage := &types.Report{
		Schema:      schema,
		Node:        m.schemaManager.NodeName(),
		Collections: make([]*types.CollectionUsage, 0, len(collections)),
		Backups:     make([]*types.BackupUsage, 0),
	}

	// Collect usage for each collection
	for _, collection := range collections {
		shardingState := m.schemaManager.CopyShardingState(collection.Class)
		if shardingState == nil {
			// this could happen in case the between getting the schema and getting the shard state the collection got deleted
			// in the meantime, usually in automated tests or scripts
			m.logger.WithFields(logrus.Fields{"class": collection.Class}).Debug("sharding state not found, could have been deleted in the meantime")
			continue
		}
		collectionUsage := &types.CollectionUsage{
			Name:              collection.Class,
			ReplicationFactor: int(collection.ReplicationConfig.Factor),
			UniqueShardCount:  int(len(shardingState.Physical)),
		}
		// Get shard usage
		index := m.db.GetIndexLike(entschema.ClassName(collection.Class))
		if index == nil {
			m.logger.WithFields(logrus.Fields{"class": collection.Class}).Debug("index not found, could have been deleted in the meantime")
			continue
		}

		// First, collect cold tenants from sharding state
		for shardName, physical := range shardingState.Physical {
			// skip non-local shards
			if !shardingState.IsLocalShard(shardName) {
				continue
			}

			// Only process COLD tenants here
			if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
				// Add jitter between cold tenant processing (except for the first one)
				if len(collectionUsage.Shards) > 0 {
					m.addJitter()
				}

				shardUsage, err := calculateUnloadedShardUsage(ctx, index, shardName, collection.VectorConfig)
				if err != nil {
					return nil, err
				}

				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
			}
		}

		if index == nil {
			// index could be deleted in the meantime
			m.logger.WithFields(logrus.Fields{"class": collection.Class}).Debug("index not found, could have been deleted in the meantime")
			continue
		}

		// Then, collect hot tenants from loaded shards
		if err := index.ForEachShard(func(shardName string, shard db.ShardLike) error {
			// skip non-local shards
			if !shardingState.IsLocalShard(shardName) {
				return nil
			}

			// Add jitter between hot shard processing (except for the first one)
			if len(collectionUsage.Shards) > 0 {
				m.addJitter()
			}

			// Check shard status without forcing load
			if shard.GetStatusNoLoad() == storagestate.StatusLoading {
				shardUsage, err := calculateUnloadedShardUsage(ctx, index, shardName, collection.VectorConfig)
				if err != nil {
					return err
				}
				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
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
				Name:                shardName,
				Status:              strings.ToLower(models.TenantActivityStatusACTIVE),
				ObjectsCount:        objectCount,
				ObjectsStorageBytes: uint64(objectStorageSize),
				VectorStorageBytes:  uint64(vectorStorageSize),
			}

			// Get vector usage for each named vector
			if err = shard.ForEachVectorIndex(func(targetVector string, vectorIndex db.VectorIndex) error {
				category := db.DimensionCategoryStandard // Default category
				indexType := ""
				var bits int16

				// Check if this is a named vector configuration
				if vectorConfig, exists := collection.VectorConfig[targetVector]; exists {
					// Use the named vector's configuration
					if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
						category, _ = db.GetDimensionCategory(vectorIndexConfig)
						indexType = vectorIndexConfig.IndexType()
						bits = enthnsw.GetRQBits(vectorIndexConfig)
					}
				} else if vectorIndexConfig, ok := collection.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
					// Fall back to legacy single vector configuration
					category, _ = db.GetDimensionCategory(vectorIndexConfig)
					indexType = vectorIndexConfig.IndexType()
					bits = enthnsw.GetRQBits(vectorIndexConfig)
				}

				dimensionality, err := shard.DimensionsUsage(ctx, targetVector)
				if err != nil {
					return err
				}

				// For dynamic indexes, get the actual underlying index type
				if dynamicIndex, ok := vectorIndex.(dynamic.Index); ok {
					indexType = dynamicIndex.UnderlyingIndex().String()
				}

				vectorUsage := &types.VectorUsage{
					Name:                   targetVector,
					Compression:            category.String(),
					VectorIndexType:        indexType,
					IsDynamic:              common.IsDynamic(common.IndexType(indexType)),
					VectorCompressionRatio: vectorIndex.CompressionStats().CompressionRatio(dimensionality.Dimensions),
					Bits:                   bits,
				}

				// Only add dimensionalities if there's valid data
				if dimensionality.Count > 0 || dimensionality.Dimensions > 0 {
					vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &types.Dimensionality{
						Dimensions: dimensionality.Dimensions,
						Count:      dimensionality.Count,
					})
				}

				shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
				return nil
			}); err != nil {
				return err
			}

			collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
			return nil
		}); err != nil {
			return nil, err
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
			usage.Backups = append(usage.Backups, &types.BackupUsage{
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

func calculateUnloadedShardUsage(ctx context.Context, index db.IndexLike, tenantName string, vectorConfigs map[string]models.VectorConfig) (*types.ShardUsage, error) {
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
		Status:              strings.ToLower(models.TenantActivityStatusINACTIVE),
		ObjectsStorageBytes: uint64(objectUsage.StorageBytes),
		VectorStorageBytes:  uint64(vectorStorageSize),
	}

	// Get named vector data for cold shards from schema configuration
	for targetVector, vectorConfig := range vectorConfigs {
		// For cold shards, we can't get actual dimensionality from disk without loading
		// So we'll use a placeholder or estimate based on the schema
		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			Compression:            db.DimensionCategoryStandard.String(),
			VectorCompressionRatio: 1.0, // Default ratio for cold shards
		}

		if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
			category, _ := db.GetDimensionCategory(vectorIndexConfig)
			vectorUsage.Compression = category.String()
			vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
			vectorUsage.Bits = enthnsw.GetRQBits(vectorIndexConfig)
			vectorUsage.IsDynamic = common.IsDynamic(common.IndexType(vectorUsage.VectorIndexType))
		}

		shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
	}
	return shardUsage, err
}
