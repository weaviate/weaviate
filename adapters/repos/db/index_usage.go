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

package db

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/usage"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
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
	var uniqueShardCount int

	collectionUsage := &types.CollectionUsage{
		Name:              i.Config.ClassName.String(),
		ReplicationFactor: int(i.Config.ReplicationFactor),
	}

	// iterate over local shards only
	err := i.schemaReader.Read(i.Config.ClassName.String(), func(_ *models.Class, ss *sharding.State) error {
		for shardName, physical := range ss.Physical {
			isLocal := ss.IsLocalShard(shardName)
			if !isLocal {
				continue
			}

			if err := func() error {
				i.shardCreateLocks.Lock(shardName)
				defer i.shardCreateLocks.Unlock(shardName)

				// there are 5 different states we need to handle here:
				// 1. newly created tenant (empty)- no files on disk yet but present in sharding state. Just return 0 usage
				// 2. cold tenant - handle without loading
				// 3. offloaded tenants - handle without loading, return 0 usage
				// 3. hot, unloaded lazy tenant - handle without loading
				// 4. hot, loaded lazy tenant or non-lazy tenant - handle normally

				uniqueShardCount++

				// case1: newly created empty tenant - status does not matter as there is no data yet
				exists, err := i.tenantDirExists(shardName)
				if err != nil {
					return err
				}
				if !exists {
					collectionUsage.Shards = append(collectionUsage.Shards, emptyShardUsageWithNameAndActivity(shardName, physical.ActivityStatus()))
					return nil
				}

				// case 2: cold tenant
				if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
					//// Add jitter between cold tenant processing (except for the first one)
					//if len(collectionUsage.Shards) > 0 {
					//	jitter()
					//}

					shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName)
					if err != nil {
						return err
					}

					collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
					return nil
				} else if physical.ActivityStatus() != models.TenantActivityStatusHOT {
					// case 3: non-hot tenants - OFFLOADED, OFFLOADING, ONLOADING. We return 0 usage for these tenants
					collectionUsage.Shards = append(collectionUsage.Shards, emptyShardUsageWithNameAndActivity(shardName, physical.ActivityStatus()))
					return nil
				}

				shard := i.shards.Load(shardName)
				if shard == nil {
					// not totally sure what this means, but just skip it for now
					i.logger.WithField("shard", shardName).Warn("shard not found in memory, skipping usage calculation")
					return nil
				}

				// case 2: hot, unloaded lazy tenant
				if lazyShard, ok := shard.(*LazyLoadShard); ok && !lazyShard.isLoaded() {
					shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName)
					if err != nil {
						return err
					}

					collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
					return nil
				}

				// case 3: hot, loaded lazy tenant or non-lazy tenant
				shardUsage, err := i.calculateLoadedShardUsage(ctx, shard)
				if err != nil {
					return err
				}
				collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)

				return nil
			}(); err != nil {
				return err
			}
		}
		collectionUsage.UniqueShardCount = uniqueShardCount

		return nil
	})
	if err != nil {
		return nil, err
	}

	return collectionUsage, nil
}

func (i *Index) calculateLoadedShardUsage(ctx context.Context, shard ShardLike) (*types.ShardUsage, error) {
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

	vectorconfigs := i.GetVectorIndexConfigs()

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
		if vectorIndexConfig, exists := vectorconfigs[targetVector]; exists {
			// Use the named vector's configuration
			category, _ = GetDimensionCategory(vectorIndexConfig)
			indexType = vectorIndexConfig.IndexType()
			bits = enthnsw.GetRQBits(vectorIndexConfig)
		} else if vectorIndexConfig, exists := vectorconfigs[""]; exists {
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

func (i *Index) calculateUnloadedShardUsage(ctx context.Context, tenantName string) (*types.ShardUsage, error) {
	// Cold tenant: calculate from disk without loading
	objectUsage, err := shardusage.CalculateUnloadedObjectsMetrics(i.logger, i.path(), tenantName)
	if err != nil {
		return nil, err
	}

	vectorIndexConfigs := i.GetVectorIndexConfigs()

	vectorStorageSize, err := shardusage.CalculateUnloadedVectorsMetrics(ctx, i.logger, i.path(), tenantName, vectorIndexConfigs)
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
	for targetVector, vectorIndexConfig := range vectorIndexConfigs {
		// For cold shards, we can't get actual dimensionality from disk without loading
		// So we'll use a placeholder or estimate based on the schema
		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			Compression:            DimensionCategoryStandard.String(),
			VectorCompressionRatio: 1.0, // Default ratio for cold shards
		}

		category, _ := GetDimensionCategory(vectorIndexConfig)
		vectorUsage.Compression = category.String()
		vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
		vectorUsage.Bits = enthnsw.GetRQBits(vectorIndexConfig)
		vectorUsage.IsDynamic = common.IsDynamic(common.IndexType(vectorUsage.VectorIndexType))
		// Why is this a list? There should be one dimensionality per named vector
		dimensionalities, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, i.logger, i.path(), tenantName, targetVector)
		if err != nil {
			return nil, err
		}
		vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &dimensionalities)

		shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
	}
	return shardUsage, err
}

func emptyShardUsageWithNameAndActivity(shardName, activity string) *types.ShardUsage {
	return &types.ShardUsage{
		Name:                shardName,
		Status:              activity,
		ObjectsCount:        0,
		ObjectsStorageBytes: 0,
		VectorStorageBytes:  0,
	}
}
