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
	"fmt"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (db *DB) UsageForIndex(ctx context.Context, className schema.ClassName, jitterInterval time.Duration, exactObjectCount bool) (*types.CollectionUsage, error) {
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

	return index.usageForCollection(ctx, jitterInterval, exactObjectCount)
}

func (i *Index) usageForCollection(ctx context.Context, jitterInterval time.Duration, exactObjectCount bool) (*types.CollectionUsage, error) {
	var uniqueShardCount int

	collectionUsage := &types.CollectionUsage{
		Name:              i.Config.ClassName.String(),
		ReplicationFactor: int(i.Config.ReplicationFactor),
	}

	// Iterate over local shards:
	// 1) lock underlying map using schemaReader.Read
	// 2) for each local shard, lock using shardCreateLocks to avoid concurrent creation/deletion
	// 3) calculate usage depending on shard state
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
				// 1. newly created shard (empty)- no files on disk yet but present in sharding state. Just return 0 usage
				// 2. cold shard (only MT) - handle without loading
				// 3. offloaded shards - handle without loading, return 0 usage
				// 4. hot, unloaded lazy shard - handle without loading
				// 5. hot, loaded lazy shard or non-lazy shard - handle normally

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

				// Add jitter between tenant processing to not overload the system if there are many shards
				if len(collectionUsage.Shards) > 0 {
					addJitter(jitterInterval)
				}

				// case 2: cold tenant
				if physical.ActivityStatus() == models.TenantActivityStatusCOLD {
					shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName)
					if err != nil {
						return err
					}

					collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
					return nil
				} else if physical.ActivityStatus() != models.TenantActivityStatusHOT {
					// case 3: other non-hot tenants - OFFLOADED, OFFLOADING, ONLOADING. We return 0 usage for these tenants
					collectionUsage.Shards = append(collectionUsage.Shards, emptyShardUsageWithNameAndActivity(shardName, physical.ActivityStatus()))
					return nil
				}

				shard := i.shards.Load(shardName)
				if shard == nil {
					// not totally sure what this means, but just skip it for now
					i.logger.WithField("action", "usage").WithFields(map[string]interface{}{
						"shard":      shardName,
						"collection": i.Config.ClassName,
						"status":     physical.ActivityStatus(),
					}).Warn("shard not found in memory, skipping usage calculation")
					return nil
				}

				// case 4: hot, unloaded lazy tenant. Handle without loading
				if lazyShard, ok := shard.(*LazyLoadShard); ok && !lazyShard.isLoaded() {
					shardUsage, err := i.calculateUnloadedShardUsage(ctx, shardName)
					if err != nil {
						return err
					}

					collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
					return nil
				}

				// case 3: hot, loaded lazy tenant or non-lazy tenant
				shardUsage, err := i.calculateLoadedShardUsage(ctx, shard, exactObjectCount)
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

func (i *Index) calculateLoadedShardUsage(ctx context.Context, shard ShardLike, exactCount bool) (*types.ShardUsage, error) {
	objectStorageSize, err := shard.ObjectStorageSize(ctx)
	if err != nil {
		return nil, err
	}
	var objectCount int64
	if exactCount {
		objectCountInt, err := shard.ObjectCount(ctx)
		if err != nil {
			return nil, err
		}
		objectCount = int64(objectCountInt)
	} else {
		objectCount, err = shard.ObjectCountAsync(ctx)
		if err != nil {
			return nil, err
		}
	}

	vectorStorageSize, err := shard.VectorStorageSize(ctx)
	if err != nil {
		return nil, err
	}

	vectorConfigs := i.GetVectorIndexConfigs()

	shardUsage := &types.ShardUsage{
		Name:                shard.Name(),
		Status:              strings.ToLower(models.TenantActivityStatusACTIVE),
		ObjectsCount:        objectCount,
		ObjectsStorageBytes: uint64(objectStorageSize),
		VectorStorageBytes:  uint64(vectorStorageSize),
	}
	// Get vector usage for each named vector
	if err = shard.ForEachVectorIndex(func(targetVector string, vectorIndex VectorIndex) error {
		var vectorIndexConfig schemaConfig.VectorIndexConfig
		if vecCfg, exists := vectorConfigs[targetVector]; exists {
			vectorIndexConfig = vecCfg
		} else if vecCfg, exists := vectorConfigs[""]; exists {
			vectorIndexConfig = vecCfg
		} else {
			return fmt.Errorf("vector index %s not found in config", targetVector)
		}
		category, _ := GetDimensionCategory(vectorIndexConfig)
		indexType := vectorIndexConfig.IndexType()
		bits := enthnsw.GetRQBits(vectorIndexConfig)

		// For dynamic indexes, get the actual underlying index type
		if dynamicIndex, ok := vectorIndex.(dynamic.Index); ok {
			indexType = dynamicIndex.UnderlyingIndex().String()
		}
		dimensionality, err := shard.DimensionsUsage(ctx, targetVector)
		if err != nil {
			return err
		}

		compressionRatio := vectorIndex.CompressionStats().CompressionRatio(dimensionality.Dimensions)

		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			Compression:            category.String(),
			VectorIndexType:        indexType,
			IsDynamic:              common.IsDynamic(common.IndexType(indexType)),
			VectorCompressionRatio: compressionRatio,
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
		Status:              strings.ToLower(models.TenantActivityStatusINACTIVE), //
		ObjectsStorageBytes: uint64(objectUsage.StorageBytes),
		VectorStorageBytes:  uint64(vectorStorageSize),
	}

	// Get named vector data for cold shards from schema configuration
	for targetVector, vectorIndexConfig := range vectorIndexConfigs {
		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			VectorCompressionRatio: 1.0, // Default ratio for cold shards
		}

		category, _ := GetDimensionCategory(vectorIndexConfig)
		vectorUsage.Compression = category.String()
		vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
		vectorUsage.Bits = enthnsw.GetRQBits(vectorIndexConfig)
		vectorUsage.IsDynamic = common.IsDynamic(common.IndexType(vectorUsage.VectorIndexType))
		dimensionalities, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, i.logger, i.path(), tenantName, targetVector)
		if err != nil {
			return nil, err
		}
		vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &dimensionalities)

		shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
	}
	return shardUsage, err
}

// addJitter adds a small random delay if jitter interval is set
func addJitter(jitterInterval time.Duration) {
	if jitterInterval <= 0 {
		return // No jitter if interval is 0 or negative
	}
	jitter := time.Duration(time.Now().UnixNano() % int64(jitterInterval))
	time.Sleep(jitter)
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
