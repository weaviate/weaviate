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
	"sort"
	"strings"
	"time"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (db *DB) UsageForIndex(ctx context.Context, className schema.ClassName, jitterInterval time.Duration, exactObjectCount bool, vectorsConfig map[string]models.VectorConfig) (*types.CollectionUsage, error) {
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

	return index.usageForCollection(ctx, jitterInterval, exactObjectCount, vectorsConfig)
}

func (i *Index) usageForCollection(ctx context.Context, jitterInterval time.Duration, exactObjectCount bool, vectorConfig map[string]models.VectorConfig) (*types.CollectionUsage, error) {
	collectionUsage := &types.CollectionUsage{
		Name:              i.Config.ClassName.String(),
		ReplicationFactor: int(i.Config.ReplicationFactor),
	}

	localShards := map[string]struct{}{}

	// We need a consistent view of the sharding state and the locals shards. At the same time, we do not want to lock
	// the entire state for the duration of the potentially long-running usage calculation. Therefore, we do this in steps:
	// 1) lock sharding state using schemaReader.Read while we collect all local shards and their status
	// 2) calculate usage depending on shard state below where only the given shard is locked (below)
	err := i.schemaReader.Read(i.Config.ClassName.String(), false, func(_ *models.Class, ss *sharding.State) error {
		for shardName := range ss.Physical {
			isLocal := ss.IsLocalShard(shardName)
			if !isLocal {
				continue
			}

			localShards[shardName] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("schemareader: %w", err)
	}

	var uniqueShardCount int

	// There is an important distinction between the state of the shard in the schema (in schemaReader) and the local
	// state, which corresponds to which shard is loaded in memory and both can be out of sync.
	//
	// After collection all local shards from the sharding state, we now need to iterate through these shards, lock them
	// individually against changes in the _local_ state (i.e. loading/unloading) and then collect their usage based
	// on this local state.
	for shardName := range localShards {
		if err := func() error {
			// Add jitter between tenant processing to not overload the system if there are many shards
			if len(collectionUsage.Shards) > 0 {
				addJitter(jitterInterval)
			}

			i.shardCreateLocks.Lock(shardName)
			defer i.shardCreateLocks.Unlock(shardName)

			uniqueShardCount++

			shard := i.shards.Load(shardName)
			var localStatus string
			if shard != nil {
				localStatus = models.TenantActivityStatusACTIVE
			} else {
				// this might also be offloaded, we need to check this later
				localStatus = models.TenantActivityStatusINACTIVE
			}

			// case1: newly created empty tenant - status does not matter as there is no data yet. This also includes
			// tenants that were deleted in the meantime, but as we record zero usage, it doesn't matter
			exists, err := i.tenantDirExists(shardName)
			if err != nil {
				return fmt.Errorf("tenant exists: %w", err)
			}
			if !exists {
				collectionUsage.Shards = append(collectionUsage.Shards, emptyShardUsageWithNameAndActivity(shardName, localStatus))
				return nil
			}

			var err2 error
			var shardUsage *types.ShardUsage
			switch localStatus {
			case models.TenantActivityStatusACTIVE, models.TenantActivityStatusHOT:
				// active tenants can be either fully loaded or lazy loaded. Lazy shards should _not_ be loaded just for
				// usage calculation and are treated like inactive shards
				lazyShard, isLazy := shard.(*LazyLoadShard)
				if isLazy {
					// distinguish between loaded and unloaded lazy shards - make sure that the shard is not loaded
					// while we calculate usage for the unloaded case by blocking loading
					func() {
						release := lazyShard.blockLoading()
						defer release()

						if lazyShard.loaded {
							shardUsage, err2 = i.calculateLoadedShardUsage(ctx, lazyShard.shard, exactObjectCount)
							if err2 != nil {
								err2 = fmt.Errorf("loaded lazy shard %s: %w", shardName, err2)
							}
						} else {
							shardUsage, err2 = i.calculateUnloadedShardUsage(ctx, shardName, vectorConfig)
							if err2 != nil {
								err2 = fmt.Errorf("unloaded lazy shard %s: %w", shardName, err2)
							}
						}
					}()
				} else {
					loadedShard, ok := shard.(*Shard)
					if !ok {
						return fmt.Errorf("expected loaded shard, got %T", shard)
					}
					shardUsage, err2 = i.calculateLoadedShardUsage(ctx, loadedShard, exactObjectCount)
					if err2 != nil {
						err2 = fmt.Errorf("non-lazy shard %s: %w", shardName, err2)
					}
				}
			case models.TenantActivityStatusINACTIVE, models.TenantActivityStatusCOLD:
				shardUsage, err2 = i.calculateUnloadedShardUsage(ctx, shardName, vectorConfig)
				if err2 != nil {
					err2 = fmt.Errorf("inactive shard %s: %w", shardName, err2)
				}
			case models.TenantActivityStatusFROZEN, models.TenantActivityStatusOFFLOADING, models.TenantActivityStatusOFFLOADED, models.TenantActivityStatusONLOADING, models.TenantActivityStatusUNFREEZING, models.TenantActivityStatusFREEZING:
				// skip for now and handle after we stabilized them
			default:
				// should not happen as we only collected local shards from the sharding state
				return fmt.Errorf("shard %s has unknown local status %s", shardName, localStatus)
			}
			if err2 != nil {
				return err2
			}
			collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)

			return nil
		}(); err != nil {
			return nil, err
		}
	}

	collectionUsage.UniqueShardCount = uniqueShardCount
	sort.Sort(collectionUsage.Shards)
	return collectionUsage, nil
}

func (i *Index) calculateLoadedShardUsage(ctx context.Context, shard *Shard, exactCount bool) (*types.ShardUsage, error) {
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
		indexType := vectorIndexConfig.IndexType()
		bits := enthnsw.GetRQBits(vectorIndexConfig)

		// For dynamic indexes, get the actual underlying index type
		isDynamic := false
		isDynamicUpgraded := false // only matters for dynamic
		if dynamicIndex, ok := vectorIndex.(dynamic.Index); ok {
			indexType = dynamicIndex.UnderlyingIndex().String()
			isDynamic = true
			isDynamicUpgraded = dynamicIndex.IsUpgraded()
		}
		category, _ := GetDimensionCategory(vectorIndexConfig, isDynamicUpgraded)

		dimensionality, err := shard.DimensionsUsage(ctx, targetVector)
		if err != nil {
			return err
		}

		compressionRatio := vectorIndex.CompressionStats().CompressionRatio(dimensionality.Dimensions)

		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			Compression:            category.String(),
			VectorIndexType:        indexType,
			IsDynamic:              isDynamic,
			VectorCompressionRatio: compressionRatio,
			Bits:                   bits,
			MultiVectorConfig:      multiVectorConfigFromConfig(vectorIndexConfig),
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
	sort.Sort(shardUsage.NamedVectors)
	return shardUsage, nil
}

func (i *Index) calculateUnloadedShardUsage(ctx context.Context, tenantName string, vectorConfigs map[string]models.VectorConfig) (*types.ShardUsage, error) {
	// Cold tenant: calculate from disk without loading
	objectUsage, err := shardusage.CalculateUnloadedObjectsMetrics(i.logger, i.path(), tenantName)
	if err != nil {
		return nil, err
	}

	vectorStorageSize, err := shardusage.CalculateUnloadedVectorsMetrics(ctx, i.logger, i.path(), tenantName, i.GetVectorIndexConfigs())
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
		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			VectorCompressionRatio: 1.0, // Default ratio for cold shards

		}

		vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("vector index config for %q is not of expected type", targetVector)
		}

		vectorUsage.IsDynamic = vectorConfig.VectorIndexType == common.IndexTypeDynamic
		if !vectorUsage.IsDynamic {
			// for cold tenants we cannot distinguish know if dynamic has been upgraded or not. Do not include wrong data
			category, _ := GetDimensionCategory(vectorIndexConfig, false)
			vectorUsage.Compression = category.String()
			vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
		}

		dimensionalities, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, i.logger, i.path(), tenantName, targetVector)
		if err != nil {
			return nil, err
		}
		vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &dimensionalities)
		vectorUsage.MultiVectorConfig = multiVectorConfigFromConfig(vectorIndexConfig)
		shardUsage.NamedVectors = append(shardUsage.NamedVectors, vectorUsage)
	}
	sort.Sort(shardUsage.NamedVectors)
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

func multiVectorConfigFromConfig(vectorConfig schemaConfig.VectorIndexConfig) *types.MultiVectorConfig {
	hnswConfig, ok := vectorConfig.(enthnsw.UserConfig)
	if !ok || !hnswConfig.Multivector.Enabled {
		return &types.MultiVectorConfig{Enabled: false}
	}
	return &types.MultiVectorConfig{
		Enabled: true,
		MuveraConfig: &types.MuveraConfig{
			Enabled:      hnswConfig.Multivector.MuveraConfig.Enabled,
			KSim:         hnswConfig.Multivector.MuveraConfig.KSim,
			DProjections: hnswConfig.Multivector.MuveraConfig.DProjections,
			Repetitions:  hnswConfig.Multivector.MuveraConfig.Repetitions,
		},
	}
}
