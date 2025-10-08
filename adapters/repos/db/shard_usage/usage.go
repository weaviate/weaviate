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

package shardusage

import (
	"context"
	"encoding/binary"
	"fmt"
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
)

func UsageTmpFilePath(indexPath, shardName string) string {
	return path.Join(indexPath, shardName, "USAGE_TMP.tmp")
}

func shardPathLSM(indexPath, shardName string) string {
	return path.Join(indexPath, shardName, "lsm")
}

func shardPathObjectsLSM(indexPath, shardName string) string {
	return path.Join(shardPathLSM(indexPath, shardName), helpers.ObjectsBucketLSM)
}

func shardPathDimensionsLSM(indexPath, shardName string) string {
	return path.Join(shardPathLSM(indexPath, shardName), helpers.DimensionsBucketLSM)
}

// CalculateUnloadedDimensionsUsage calculates dimensions and object count for an unloaded shard without loading it into memory
func CalculateUnloadedDimensionsUsage(ctx context.Context, logger logrus.FieldLogger, path, tenantName, targetVector string) (types.Dimensionality, error) {
	bucketPath := shardPathDimensionsLSM(path, tenantName)
	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	if err != nil {
		return types.Dimensionality{}, fmt.Errorf("determine dimensions bucket strategy: %w", err)
	}

	bucket, err := lsmkv.NewBucketCreator().NewBucket(ctx,
		bucketPath,
		path,
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(strategy),
	)
	if err != nil {
		return types.Dimensionality{}, err
	}
	defer bucket.Shutdown(ctx)

	return CalculateTargetVectorDimensionsFromBucket(ctx, bucket, targetVector)
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
func CalculateNonLSMStorage(path, shardName string) (uint64, error) {
	totalSize := uint64(0)
	shardPath := filepath.Join(path, shardName)

	files, dirs, err := diskio.GetFileWithSizes(shardPath)
	if err != nil {
		return 0, err
	}

	// Add sizes of all files in the shard root directory
	for _, size := range files {
		totalSize += uint64(size)
	}
	for _, dir := range dirs {
		if strings.Contains(dir, "lsm") {
			// lsm folder is already calculated, no need to read two times
			continue
		}

		fullPath := filepath.Join(shardPath, dir)
		filesSubFolder, _, err := diskio.GetFileWithSizes(fullPath)
		if err != nil {
			return 0, err
		}

		for _, size := range filesSubFolder {
			totalSize += uint64(size)
		}

	}

	return totalSize, nil
}

// CalculateTargetVectorDimensionsFromBucket calculates dimensions and object count for a target vector from an LSMKV bucket
func CalculateTargetVectorDimensionsFromBucket(ctx context.Context, b *lsmkv.Bucket, targetVector string,
) (types.Dimensionality, error) {
	dimensionality := types.Dimensionality{}

	if err := lsmkv.CheckExpectedStrategy(b.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyRoaringSet); err != nil {
		return dimensionality, fmt.Errorf("calcTargetVectorDimensionsFromBucket: %w", err)
	}

	nameLen := len(targetVector)
	expectedKeyLen := nameLen + 4 // vector name + uint32
	var k []byte

	switch b.Strategy() {
	case lsmkv.StrategyMapCollection:
		// Since weaviate 1.34 default dimension bucket strategy is StrategyRoaringSet.
		// For backward compatibility StrategyMapCollection is still supported.

		c, err := b.MapCursor()
		if err != nil {
			return dimensionality, fmt.Errorf("create cursor: %w", err)
		}
		defer c.Close()

		var v []lsmkv.MapPair
		if nameLen == 0 {
			k, v = c.First(ctx)
		} else {
			k, v = c.Seek(ctx, []byte(targetVector))
		}
		for ; k != nil; k, v = c.Next(ctx) {
			// for named vectors we have to additionally check if the key is prefixed with the vector name
			if len(k) != expectedKeyLen || !strings.HasPrefix(string(k), targetVector) {
				break
			}

			dimLength := binary.LittleEndian.Uint32(k[nameLen:])
			if dimLength > 0 && (dimensionality.Dimensions == 0 || dimensionality.Count == 0) {
				dimensionality.Dimensions = int(dimLength)
				dimensionality.Count = len(v)
			}
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
			// for named vectors we have to additionally check if the key is prefixed with the vector name
			if len(k) != expectedKeyLen || !strings.HasPrefix(string(k), targetVector) {
				break
			}

			dimLength := binary.LittleEndian.Uint32(k[nameLen:])
			if dimLength > 0 && (dimensionality.Dimensions == 0 || dimensionality.Count == 0) {
				dimensionality.Dimensions = int(dimLength)
				dimensionality.Count = v.GetCardinality()
			}
		}
	}

	return dimensionality, nil
}
