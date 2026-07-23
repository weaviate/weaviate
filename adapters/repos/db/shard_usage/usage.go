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

package shardusage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
	entsync "github.com/weaviate/weaviate/entities/sync"
)

func shardPathLSM(indexPath, shardName string) string {
	return path.Join(indexPath, shardName, "lsm")
}

func shardPathObjectsLSM(indexPath, shardName string) string {
	return path.Join(shardPathLSM(indexPath, shardName), helpers.ObjectsBucketLSM)
}

func shardPathDimensionsLSM(indexPath, shardName string) string {
	return path.Join(shardPathLSM(indexPath, shardName), helpers.DimensionsBucketLSM)
}

func usageTmpFilePath(indexPath, shardName string) string {
	return path.Join(indexPath, shardName, "usage.json.tmp")
}

// ComputedUsageDataExists checks if pre-calculated shard usage data file exists
func ComputedUsageDataExists(indexPath, shardName string) bool {
	_, err := os.Stat(usageTmpFilePath(indexPath, shardName))
	return !os.IsNotExist(err)
}

// RemoveComputedUsageDataForUnloadedShard removes pre-calculated shard usage data from disk
func RemoveComputedUsageDataForUnloadedShard(indexPath, shardName string) error {
	usageFilePath := usageTmpFilePath(indexPath, shardName)
	if _, err := os.Stat(usageFilePath); !os.IsNotExist(err) {
		if err := os.RemoveAll(usageFilePath); err != nil {
			return err
		}
	}
	return nil
}

// SaveComputedUsageData saves pre-calculated shard usage data to disk
func SaveComputedUsageData(indexPath, shardName string, shardUsage *types.ShardUsage) error {
	data, err := json.Marshal(usageDisk(shardUsage))
	if err != nil {
		return fmt.Errorf("marshal pre-calculated usage for disk: %w", err)
	}
	if err := os.WriteFile(usageTmpFilePath(indexPath, shardName), data, os.FileMode(0o600)); err != nil {
		return fmt.Errorf("write pre-calculated usage to disk: %w", err)
	}
	return nil
}

// LoadComputedUsageData loads pre-calculated shard usage data, checks version of saved data before returning
func LoadComputedUsageData(indexPath, shardName string) (*types.ShardUsage, error) {
	// usage has been pre-calculated and can be read from disk
	usage, err := os.ReadFile(usageTmpFilePath(indexPath, shardName))
	if err != nil {
		return nil, fmt.Errorf("read pre-calculated usage from disk: %w", err)
	}
	usageDisk := &types.UsageDisk{}
	if err := json.Unmarshal(usage, usageDisk); err != nil {
		return nil, fmt.Errorf("unmarshal pre-calculated usage from disk: %w", err)
	}
	if usageDisk.Version != types.UsageDiskVersion {
		return nil, fmt.Errorf("usage data saved to disk version mismatch, currently supported version is %d but got %d",
			types.UsageDiskVersion, usageDisk.Version)
	}
	return usageDisk.ShardUsage, nil
}

func usageDisk(shardUsage *types.ShardUsage) *types.UsageDisk {
	return &types.UsageDisk{Version: types.UsageDiskVersion, ShardUsage: shardUsage}
}

// unloadedDimensionsBucketLocks serializes access to the same unloaded dimensions bucket.
// Concurrent usage reports (overlapping periodic collections, /debug/usage, both usage modules
// enabled) and the node-wide metrics observer may otherwise open the same bucket at once,
// which lsmkv's GlobalBucketRegistry rejects with "bucket already registered".
var unloadedDimensionsBucketLocks = entsync.NewKeyLockerContext()

// openUnloadedDimensionsBucket opens the dimensions bucket of an unloaded shard without
// loading the shard into memory. The bucket is opened with a sequential-access hint, as the
// dimension calculations scan it with cursors.
// Callers must hold the unloadedDimensionsBucketLocks lock for bucketPath until the returned
// bucket is shut down.
func openUnloadedDimensionsBucket(ctx context.Context, logger logrus.FieldLogger, path, bucketPath string) (*lsmkv.Bucket, error) {
	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	if err != nil {
		return nil, fmt.Errorf("determine dimensions bucket strategy: %w", err)
	}

	return lsmkv.NewBucketCreator().NewBucket(ctx,
		bucketPath,
		path,
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(strategy),
		lsmkv.WithSequentialAccess(true),
	)
}

// CalculateUnloadedDimensionsUsage calculates dimensions and object count for an unloaded shard without loading it into memory
func CalculateUnloadedDimensionsUsage(ctx context.Context, logger logrus.FieldLogger, path, tenantName, targetVector string) (types.Dimensionality, error) {
	bucketPath := shardPathDimensionsLSM(path, tenantName)
	if err := unloadedDimensionsBucketLocks.LockWithContext(bucketPath, ctx); err != nil {
		return types.Dimensionality{}, fmt.Errorf("lock dimensions bucket: %w", err)
	}
	defer unloadedDimensionsBucketLocks.Unlock(bucketPath)

	bucket, err := openUnloadedDimensionsBucket(ctx, logger, path, bucketPath)
	if err != nil {
		return types.Dimensionality{}, err
	}
	defer bucket.Shutdown(ctx)

	return CalculateTargetVectorDimensionsFromBucket(ctx, bucket, targetVector)
}

// DimensionalityUsage pairs the raw bucket dimensionality (drives disk accounting) with the
// reported RAM-oriented one, which differs only for MUVERA-encoded multi-vectors.
type DimensionalityUsage struct {
	Raw      types.Dimensionality
	Reported types.Dimensionality
}

// CalculateUnloadedDimensionsUsageAll calculates dimensions and object count for all target
// vectors of an unloaded shard without loading it into memory. The dimensions bucket is opened
// once and shared by all target vector calculations, instead of once per target vector.
func CalculateUnloadedDimensionsUsageAll(ctx context.Context,
	logger logrus.FieldLogger, path, tenantName string, targetVectors []string,
	muveraDimensions map[string]int,
) (map[string]DimensionalityUsage, error) {
	if len(targetVectors) == 0 {
		return nil, nil
	}

	bucketPath := shardPathDimensionsLSM(path, tenantName)
	if err := unloadedDimensionsBucketLocks.LockWithContext(bucketPath, ctx); err != nil {
		return nil, fmt.Errorf("lock dimensions bucket: %w", err)
	}
	defer unloadedDimensionsBucketLocks.Unlock(bucketPath)

	bucket, err := openUnloadedDimensionsBucket(ctx, logger, path, bucketPath)
	if err != nil {
		return nil, err
	}
	defer bucket.Shutdown(ctx)

	dimensionalities := make(map[string]DimensionalityUsage, len(targetVectors))
	for _, targetVector := range targetVectors {
		raw, err := CalculateTargetVectorDimensionsFromBucket(ctx, bucket, targetVector)
		if err != nil {
			return nil, err
		}
		usage := DimensionalityUsage{Raw: raw, Reported: raw}
		if encodedDimensions := muveraDimensions[targetVector]; encodedDimensions > 0 {
			if usage.Reported, err = CalculateMuveraDimensionsUsageFromBucket(ctx, bucket, targetVector, encodedDimensions); err != nil {
				return nil, err
			}
		}
		dimensionalities[targetVector] = usage
	}
	return dimensionalities, nil
}

// CalculateUnloadedVectorsMetrics calculates vector storage size from disk
func CalculateUnloadedVectorsMetrics(lsmPath string, directories []string) (int64, error) {
	totalSize := int64(0)

	// vector storage consists of:
	// 1) size of vector folder - these are:
	//     - the compressed vectors stored in their own folder each
	//     - the flat index extra copy of the uncompressed vectors (if flat index is used)
	// 2) size of uncompressed vectors stored in dimensions bucket. The size of these is calculated based on the number
	// of objects and their dimensionality. They need to be subtracted from the object bucket size to not count them twice.
	for _, directory := range directories {
		if !strings.HasPrefix(directory, "vector") {
			continue
		}
		fullPath := filepath.Join(lsmPath, directory)

		files, _, err := diskio.GetFileWithSizes(fullPath)
		if err != nil {
			return 0, err
		}
		for _, size := range files {
			totalSize += size
		}
	}
	return totalSize, nil
}

// CalculateUnloadedObjectsMetrics calculates both object count and storage size from disk
func CalculateUnloadedObjectsMetrics(logger logrus.FieldLogger, path, shardName string, includeCount bool) (types.ObjectUsage, error) {
	// Parse all .cna files in the object store and sum them up
	totalObjectCount := int64(0)
	totalDiskSize := int64(0)

	// Use a single walk to avoid multiple filepath.Walk calls and reduce file descriptors
	objectStore := shardPathObjectsLSM(path, shardName)
	files, _, err := diskio.GetFileWithSizes(objectStore)
	if err != nil {
		return types.ObjectUsage{}, err
	}
	for file, size := range files {
		totalDiskSize += size

		if includeCount {
			filePath := filepath.Join(objectStore, file)
			// Look for .cna files (net count additions)
			if strings.HasSuffix(file, lsmkv.CountNetAdditionsFileSuffix) {
				count, err := lsmkv.ReadCountNetAdditionsFile(filePath)
				if err != nil {
					logger.WithField("path", filePath).WithField("shard", shardName).WithError(err).Warn("failed to read .cna file")
					return types.ObjectUsage{}, err
				}
				totalObjectCount += count
			}

			// Look for .metadata files (bloom filters + count net additions)
			if strings.HasSuffix(file, lsmkv.MetadataFileSuffix) {
				count, err := lsmkv.ReadObjectCountFromMetadataFile(filePath)
				if err != nil {
					logger.WithField("path", filePath).WithField("shard", shardName).WithError(err).Warn("failed to read .metadata file")
					return types.ObjectUsage{}, err
				}
				totalObjectCount += count
			}
		}
	}

	// If we can't determine object count, return the disk size as fallback
	return types.ObjectUsage{
		Count:        totalObjectCount,
		StorageBytes: totalDiskSize,
	}, nil
}

// CalculateUnloadedIndicesSize calculates both object count and storage size for a cold tenant without loading it into memory
func CalculateUnloadedIndicesSize(lsmPath string, directories []string) (uint64, error) {
	totalSize := uint64(0)

	// get the storage of all lsm properties that are not objects or vector
	includedPrefixes := []string{helpers.DimensionsBucketLSM, helpers.BucketFromPropNameLSM("")}

	// check all folders and add their sizes
	for _, directory := range directories {
		included := slices.ContainsFunc(includedPrefixes, func(prefix string) bool {
			return strings.HasPrefix(directory, prefix)
		})
		if !included {
			continue
		}

		fullPath := filepath.Join(lsmPath, directory)
		files, _, err := diskio.GetFileWithSizes(fullPath)
		if err != nil {
			return 0, err
		}
		for _, size := range files {
			totalSize += uint64(size)
		}
	}
	return totalSize, nil
}

// CalculateNonLSMStorage calculates the full storage used by a shard, including objects, vectors, and indices
func CalculateNonLSMStorage(path, shardName string) (uint64, uint64, error) {
	var vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize uint64
	shardPath := filepath.Join(path, shardName)

	files, dirs, err := diskio.GetFileWithSizes(shardPath)
	if err != nil {
		return 0, 0, err
	}

	// Add sizes of all files in the shard root directory
	for _, size := range files {
		otherNonLSMFoldersStorageSize += uint64(size)
	}
	for _, dir := range dirs {
		if dir == "lsm" {
			// lsm folder is already calculated, no need to read two times
			continue
		}

		fullPath := filepath.Join(shardPath, dir)
		filesSubFolder, subDirs, err := diskio.GetFileWithSizes(fullPath)
		if err != nil {
			return 0, 0, err
		}

		totalSize := uint64(0)
		for _, size := range filesSubFolder {
			totalSize += uint64(size)
		}

		if strings.HasSuffix(dir, ".hfresh.d") {
			for _, subDir := range subDirs {
				subDirPath := filepath.Join(fullPath, subDir)
				subFiles, _, err := diskio.GetFileWithSizes(subDirPath)
				if err != nil {
					return 0, 0, err
				}

				subDirSize := uint64(0)
				for _, size := range subFiles {
					subDirSize += uint64(size)
				}

				if strings.HasSuffix(subDir, "commitlog.d") ||
					strings.HasSuffix(subDir, "snapshot.d") ||
					strings.HasSuffix(subDir, "queue.d") {
					vectorCommitLogsStorageSize += subDirSize
				} else {
					otherNonLSMFoldersStorageSize += subDirSize
				}
			}
			otherNonLSMFoldersStorageSize += totalSize
		} else if strings.HasSuffix(dir, "commitlog.d") || strings.HasSuffix(dir, "snapshot.d") {
			vectorCommitLogsStorageSize += totalSize
		} else {
			otherNonLSMFoldersStorageSize += totalSize
		}
	}

	return vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, nil
}

// forEachTargetVectorDimensionality visits every (dimensionality, object count) entry recorded
// for a target vector; multi-vector objects are recorded under their per-object total dimensions.
func forEachTargetVectorDimensionality(ctx context.Context, b *lsmkv.Bucket, targetVector string,
	visit func(dimensions, count int),
) error {
	if err := lsmkv.CheckExpectedStrategy(b.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyRoaringSet); err != nil {
		return fmt.Errorf("forEachTargetVectorDimensionality: %w", err)
	}

	prefix := []byte(targetVector)
	nameLen := len(targetVector)
	expectedKeyLen := nameLen + 4 // vector name + uint32
	var k []byte

	switch b.Strategy() {
	case lsmkv.StrategyMapCollection:
		// Since weaviate 1.34 default dimension bucket strategy is StrategyRoaringSet.
		// For backward compatibility StrategyMapCollection is still supported.

		c, err := b.MapCursor()
		if err != nil {
			return fmt.Errorf("create cursor: %w", err)
		}
		defer c.Close()

		var v []lsmkv.MapPair
		if nameLen == 0 {
			k, v = c.First(ctx)
		} else {
			k, v = c.Seek(ctx, []byte(targetVector))
		}
		for ; k != nil; k, v = c.Next(ctx) {
			if !bytes.HasPrefix(k, prefix) {
				break // prefixed keys are contiguous, no later key can match
			}
			// skip interleaved keys of longer vector names extending this one ("texts…" sorts before "text\x80…")
			if len(k) != expectedKeyLen {
				continue
			}

			visit(int(binary.LittleEndian.Uint32(k[nameLen:])), len(v))
		}
	default:
		c := b.CursorRoaringSet()
		defer c.Close()

		var v *sroar.Bitmap
		if nameLen == 0 {
			k, v = c.First()
		} else {
			k, v = c.Seek([]byte(targetVector))
		}
		for ; k != nil; k, v = c.Next() {
			// same termination rules as the map-cursor branch above
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if len(k) != expectedKeyLen {
				continue
			}

			visit(int(binary.LittleEndian.Uint32(k[nameLen:])), v.GetCardinality())
		}
	}

	return nil
}

// CalculateTargetVectorDimensionsFromBucket calculates dimensions and object count for a target vector from an LSMKV bucket
func CalculateTargetVectorDimensionsFromBucket(ctx context.Context, b *lsmkv.Bucket, targetVector string,
) (types.Dimensionality, error) {
	dimensionality := types.Dimensionality{}
	err := forEachTargetVectorDimensionality(ctx, b, targetVector, func(dimensions, count int) {
		if dimensions > 0 && (dimensionality.Dimensions == 0 || dimensionality.Count == 0) {
			dimensionality.Dimensions = dimensions
			dimensionality.Count = count
		}
	})
	if err != nil {
		return types.Dimensionality{}, err
	}
	return dimensionality, nil
}

// CalculateMuveraDimensionsUsageFromBucket reports the fixed MUVERA-encoded dimensionality
// (what is held in memory per object) with the object count summed across all raw entries.
func CalculateMuveraDimensionsUsageFromBucket(ctx context.Context, b *lsmkv.Bucket, targetVector string,
	encodedDimensions int,
) (types.Dimensionality, error) {
	totalCount := 0
	err := forEachTargetVectorDimensionality(ctx, b, targetVector, func(dimensions, count int) {
		if dimensions > 0 {
			totalCount += count
		}
	})
	if err != nil || totalCount == 0 {
		return types.Dimensionality{}, err
	}
	return types.Dimensionality{Dimensions: encodedDimensions, Count: totalCount}, nil
}
