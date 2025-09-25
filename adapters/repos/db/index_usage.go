package db

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (db *DB) UsageForIndex(ctx context.Context, className schema.ClassName) (*types.CollectionUsage, error) {
	var (
		index  *Index
		exists bool
	)
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		index, exists = db.indices[indexID(className)]
		if exists {
			index.dropIndex.RLock()
		}
	}()

	if !exists {
		return nil, nil
	}

	defer func() {
		index.dropIndex.RUnlock()
	}()

	return index.usageForCollection(ctx)
}

func (i *Index) usageForCollection(ctx context.Context) (*types.CollectionUsage, error) {
	schema := i.getSchema.ReadOnlyClass(i.Config.ClassName.String())

	var uniqueShardCount int

	collectionUsage := &types.CollectionUsage{
		Name:              i.Config.ClassName.String(),
		ReplicationFactor: int(i.Config.ReplicationFactor),
	}
	// iterate over local shards only
	shardingState := i.getSchema.CopyShardingState(i.Config.ClassName.String())
	for shardName, physical := range shardingState.Physical {

		isLocal := shardingState.IsLocalShard(shardName)
		if !isLocal {
			continue
		}
		uniqueShardCount++

		if err := func() error {
			i.shardCreateLocks.Lock(shardName)
			defer i.shardCreateLocks.Unlock(shardName)

			// there are 3 different states we need to handle here:
			// 1. cold tenant - handle without loading
			// 2. hot, unloaded lazy tenant - handle without loading
			// 3. hot, loaded lazy tenant or non-lazy tenant - handle normally

			// case 1: cold tenant
			if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
				//// Add jitter between cold tenant processing (except for the first one)
				//if len(collectionUsage.Shards) > 0 {
				//	jitter()
				//}

				shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName, schema.VectorConfig)
				if err != nil {
					return err
				}

				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
				return nil
			}

			shard := i.shards.Load(shardName)
			if shard == nil {
				return nil
			}

			// case 2: hot, unloaded lazy tenant
			if lazyShard, ok := shard.(*LazyLoadShard); ok && !lazyShard.isLoaded() {
				shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName, schema.VectorConfig)
				if err != nil {
					return err
				}

				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
				return nil
			}

			// case 3: hot, loaded lazy tenant or non-lazy tenant
			shardUsage, err := i.calculateLoadedShardUsage(ctx, shard, schema)
			if err != nil {
				return err
			}
			collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)

			return nil
		}(); err != nil {
			return nil, err
		}
	}

	collectionUsage.UniqueShardCount = uniqueShardCount

	return collectionUsage, nil
}

func (i *Index) calculateLoadedShardUsage(ctx context.Context, shard ShardLike, collectionConfig *models.Class) (*types.ShardUsage, error) {
	objectStorageSize, err := shard.ObjectStorageSize(ctx)
	if err != nil {
		return nil, err
	}
	objectCount, err := shard.ObjectCountAsync(ctx)
	if err != nil {
		return nil, err
	}

	vectorStorageSize, err := shard.VectorStorageSize(ctx)
	if err != nil {
		return nil, err
	}

	shardUsage := &types.ShardUsage{
		Name:                shard.Name(),
		Status:              strings.ToLower(models.TenantActivityStatusACTIVE),
		ObjectsCount:        objectCount,
		ObjectsStorageBytes: uint64(objectStorageSize),
		VectorStorageBytes:  uint64(vectorStorageSize),
	}
	// Get vector usage for each named vector
	if err = shard.ForEachVectorIndex(func(targetVector string, vectorIndex VectorIndex) error {
		category := DimensionCategoryStandard // Default category
		indexType := ""
		var bits int16

		// Check if this is a named vector configuration
		if vectorConfig, exists := collectionConfig.VectorConfig[targetVector]; exists {
			// Use the named vector's configuration
			if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
				category, _ = GetDimensionCategory(vectorIndexConfig)
				indexType = vectorIndexConfig.IndexType()
				bits = enthnsw.GetRQBits(vectorIndexConfig)
			}
		} else if vectorIndexConfig, ok := collectionConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
			// Fall back to legacy single vector configuration
			category, _ = GetDimensionCategory(vectorIndexConfig)
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
		return nil, err
	}
	return shardUsage, nil
}

func (i *Index) calculateUnloadedShardUsage(ctx context.Context, tenantName string, vectorConfigs map[string]models.VectorConfig) (*types.ShardUsage, error) {
	// Cold tenant: calculate from disk without loading
	objectUsage, err := i.CalculateUnloadedObjectsMetrics(ctx, tenantName)
	if err != nil {
		return nil, err
	}

	vectorStorageSize, err := i.CalculateUnloadedVectorsMetrics(ctx, tenantName)
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
			Compression:            DimensionCategoryStandard.String(),
			VectorCompressionRatio: 1.0, // Default ratio for cold shards
		}

		if vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig); ok {
			category, _ := GetDimensionCategory(vectorIndexConfig)
			vectorUsage.Compression = category.String()
			vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
			vectorUsage.Bits = enthnsw.GetRQBits(vectorIndexConfig)
			vectorUsage.IsDynamic = common.IsDynamic(common.IndexType(vectorUsage.VectorIndexType))
		}

		shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
	}
	return shardUsage, err
}
