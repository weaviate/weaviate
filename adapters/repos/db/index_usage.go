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
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ShardReadLimiter bounds the number of concurrent shard readers within a single usage
// report — parallel UsageForIndex calls share the same limiter. Acquisition is FIFO.
type ShardReadLimiter struct {
	sem   *semaphore.Weighted
	limit int
}

func NewShardReadLimiter(limit int) *ShardReadLimiter {
	if limit < 1 {
		limit = 1
	}
	return &ShardReadLimiter{sem: semaphore.NewWeighted(int64(limit)), limit: limit}
}

func (l *ShardReadLimiter) Acquire(ctx context.Context) error {
	return l.sem.Acquire(ctx, 1)
}

func (l *ShardReadLimiter) Release() {
	l.sem.Release(1)
}

func (l *ShardReadLimiter) Limit() int {
	return l.limit
}

// UsageForIndex computes usage for a single collection.
func (db *DB) UsageForIndex(ctx context.Context, className schema.ClassName, shardReadLimiter *ShardReadLimiter, exactObjectCount bool, vectorsConfig map[string]models.VectorConfig) (*types.CollectionUsage, error) {
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

	return index.usageForCollection(ctx, shardReadLimiter, exactObjectCount, vectorsConfig)
}

func (i *Index) usageForCollection(ctx context.Context, shardReadLimiter *ShardReadLimiter, exactObjectCount bool, vectorConfig map[string]models.VectorConfig) (*types.CollectionUsage, error) {
	collectionUsage := &types.CollectionUsage{
		Name:              i.Config.ClassName.String(),
		ReplicationFactor: int(i.Config.ReplicationFactor),
	}

	if shardReadLimiter == nil {
		shardReadLimiter = NewShardReadLimiter(1)
	}
	shardConcurrency := shardReadLimiter.Limit()

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

	i.logger.WithFields(
		logrus.Fields{
			"class":             i.Config.ClassName.String(),
			"shard_concurrency": shardConcurrency,
		}).Infof("creating usage report with %d shards", len(localShards))

	shardNames := make([]string, 0, len(localShards))
	for shardName := range localShards {
		shardNames = append(shardNames, shardName)
	}

	// process the local shards with a bounded number of concurrent readers
	var (
		mu        sync.Mutex
		processed atomic.Int64
	)
	start := time.Now()

	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(i.logger, ctx)
	eg.SetLimit(shardConcurrency)
	for _, shardName := range shardNames {
		eg.Go(func() error {
			if err := shardReadLimiter.Acquire(egCtx); err != nil {
				return err
			}
			defer shardReadLimiter.Release()

			shardUsage, err := i.usageForShard(egCtx, shardName, exactObjectCount, vectorConfig)
			if err != nil {
				return err
			}

			count := processed.Add(1)
			if shardUsage != nil {
				func() {
					mu.Lock()
					defer mu.Unlock()
					collectionUsage.Shards = append(collectionUsage.Shards, shardUsage)
				}()
			}

			if count%1000 == 0 {
				i.logger.WithFields(
					logrus.Fields{
						"class":             i.Config.ClassName.String(),
						"shard_concurrency": shardConcurrency,
						"processed_shards":  count,
						"took":              time.Since(start).Seconds(),
					}).Debugf("processed %d/%d shards for usage report", count, len(shardNames))
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	uniqueShardCount := int(processed.Load())
	i.logger.WithFields(
		logrus.Fields{
			"class":             i.Config.ClassName.String(),
			"shard_concurrency": shardConcurrency,
			"processed_shards":  uniqueShardCount,
			"took":              time.Since(start).Seconds(),
		}).Infof("finished processing %d/%d shards for usage report", uniqueShardCount, len(shardNames))

	collectionUsage.UniqueShardCount = uniqueShardCount
	sort.Sort(collectionUsage.Shards)
	return collectionUsage, nil
}

// usageForShard computes usage for a single local shard. There is an important distinction between
// the state of the shard in the schema (in schemaReader) and the local state, which corresponds to
// which shard is loaded in memory — both can be out of sync, so the shard is locked against changes
// in the _local_ state (i.e. loading/unloading) for the duration of the calculation. A nil
// *ShardUsage with a nil error means the shard should be skipped (transitional states like
// FREEZING/OFFLOADING). Safe to call concurrently for distinct shards.
func (i *Index) usageForShard(ctx context.Context, shardName string, exactObjectCount bool, vectorConfig map[string]models.VectorConfig) (*types.ShardUsage, error) {
	i.shardCreateLocks.RLock(shardName)
	defer i.shardCreateLocks.RUnlock(shardName)

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
		return nil, fmt.Errorf("tenant exists: %w", err)
	}
	if !exists {
		return emptyShardUsageWithNameAndActivity(shardName, localStatus), nil
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
				return nil, fmt.Errorf("expected loaded shard, got %T", shard)
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
		return nil, nil
	default:
		// should not happen as we only collected local shards from the sharding state
		return nil, fmt.Errorf("shard %s has unknown local status %s", shardName, localStatus)
	}
	if err2 != nil {
		// Files vanished mid-deletion: record zero usage instead of failing
		// the whole report (same as the tenantDirExists check above).
		if errors.Is(err2, fs.ErrNotExist) {
			i.logger.WithFields(logrus.Fields{
				"class": i.Config.ClassName.String(),
				"shard": shardName,
			}).Warnf("shard files missing during usage calculation, likely deleted concurrently; recording empty usage: %v", err2)
			return emptyShardUsageWithNameAndActivity(shardName, localStatus), nil
		}
		return nil, err2
	}
	return shardUsage, nil
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

	lsmPath := shardPathLSM(i.path(), shard.Name())

	_, directories, err := diskio.GetFileWithSizes(lsmPath)
	if err != nil {
		return nil, err
	}

	vectorStorageSize, uncompressedVectorSize, err := shard.VectorStorageSize(ctx, lsmPath, directories)
	if err != nil {
		return nil, err
	}

	indexUsage, err := shardusage.CalculateUnloadedIndicesSize(lsmPath, directories)
	if err != nil {
		return nil, err
	}

	vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := shardusage.CalculateNonLSMStorage(i.path(), shard.Name())
	if err != nil {
		return nil, err
	}

	shardUsage := &types.ShardUsage{
		Name:                  shard.Name(),
		Status:                strings.ToLower(models.TenantActivityStatusACTIVE),
		ObjectsCount:          objectCount,
		ObjectsStorageBytes:   uint64(objectStorageSize) - uint64(uncompressedVectorSize),                               // objects without vectors
		VectorStorageBytes:    uint64(vectorStorageSize) + uint64(uncompressedVectorSize) + vectorCommitLogsStorageSize, // lsm/vectors + objects vectors + commit.log folders
		IndexStorageBytes:     indexUsage,                                                                               // lsm property folders and dimensions folder
		FullShardStorageBytes: vectorCommitLogsStorageSize + otherNonLSMFoldersStorageSize + indexUsage + uint64(objectStorageSize) + uint64(vectorStorageSize),
	}
	// Get vector usage for each named vector
	vectorConfigs := i.GetVectorIndexConfigs()
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

		// For dynamic indexes, get the actual underlying index type
		isDynamic := false
		isDynamicUpgraded := false // only matters for dynamic
		if dynamicIndex, ok := vectorIndex.(dynamic.Index); ok {
			indexType = dynamicIndex.UnderlyingIndex().String()
			isDynamic = true
			isDynamicUpgraded = dynamicIndex.IsUpgraded()
		}
		dimInfo := GetDimensionCategory(vectorIndexConfig, isDynamicUpgraded)

		dimensionality, err := shard.DimensionsUsage(ctx, targetVector)
		if err != nil {
			return err
		}

		compressionRatio := vectorIndex.CompressionStats().CompressionRatio(dimensionality.Dimensions)

		vectorUsage := &types.VectorUsage{
			Name:                   targetVector,
			Compression:            dimInfo.category.String(),
			VectorIndexType:        indexType,
			IsDynamic:              isDynamic,
			VectorCompressionRatio: compressionRatio,
			Bits:                   dimInfo.bits,
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

func (i *Index) calculateUnloadedShardUsage(ctx context.Context, shardName string, vectorConfigs map[string]models.VectorConfig) (*types.ShardUsage, error) {
	if shardusage.ComputedUsageDataExists(i.path(), shardName) {
		// usage has been pre-calculated and can be read from disk
		shardUsage, err := shardusage.LoadComputedUsageData(i.path(), shardName)
		if err != nil {
			// in case of error just log an information and proceed with computation
			i.logger.Errorf("failed to load pre-calculated usage data for shard %s: %v", shardName, err)
		} else {
			return shardUsage, nil
		}
	}
	lsmPath := shardPathLSM(i.path(), shardName)

	_, directories, err := diskio.GetFileWithSizes(lsmPath)
	if err != nil {
		return nil, err
	}

	// Cold tenant: calculate from disk without loading
	objectUsage, err := shardusage.CalculateUnloadedObjectsMetrics(i.logger, i.path(), shardName, true)
	if err != nil {
		return nil, err
	}

	vectorStorageSize, err := shardusage.CalculateUnloadedVectorsMetrics(lsmPath, directories)
	if err != nil {
		return nil, err
	}

	indexUsage, err := shardusage.CalculateUnloadedIndicesSize(lsmPath, directories)
	if err != nil {
		return nil, err
	}

	vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := shardusage.CalculateNonLSMStorage(i.path(), shardName)
	if err != nil {
		return nil, err
	}

	// Get named vector data for cold shards from schema configuration
	var namedVectors types.VectorsUsage
	uncompressedVectorSize := uint64(0) // calculate total uncompressed vector size for all vectors
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
			dimInfo := GetDimensionCategory(vectorIndexConfig, false)
			vectorUsage.Compression = dimInfo.category.String()
			vectorUsage.VectorIndexType = vectorIndexConfig.IndexType()
		}

		dimensionalities, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, i.logger, i.path(), shardName, targetVector)
		if err != nil {
			return nil, err
		}
		uncompressedVectorSize += uint64(dimensionalities.Count) * uint64(dimensionalities.Dimensions) * 4
		vectorUsage.Dimensionalities = append(vectorUsage.Dimensionalities, &dimensionalities)
		vectorUsage.MultiVectorConfig = multiVectorConfigFromConfig(vectorIndexConfig)
		namedVectors = append(namedVectors, vectorUsage)
	}

	sort.Sort(namedVectors)

	shardUsage := &types.ShardUsage{
		Name:                  shardName,
		ObjectsCount:          objectUsage.Count,
		Status:                strings.ToLower(models.TenantActivityStatusINACTIVE),
		ObjectsStorageBytes:   uint64(objectUsage.StorageBytes) - uncompressedVectorSize,
		VectorStorageBytes:    uint64(vectorStorageSize) + uncompressedVectorSize + vectorCommitLogsStorageSize,
		IndexStorageBytes:     indexUsage,
		FullShardStorageBytes: vectorCommitLogsStorageSize + otherNonLSMFoldersStorageSize + indexUsage + uint64(objectUsage.StorageBytes) + uint64(vectorStorageSize),
		NamedVectors:          namedVectors,
	}
	if err := shardusage.SaveComputedUsageData(i.path(), shardName, shardUsage); err != nil {
		return nil, fmt.Errorf("save usage to disk: %w", err)
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
