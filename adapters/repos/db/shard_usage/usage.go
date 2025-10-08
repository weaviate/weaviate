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
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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
func CalculateUnloadedVectorsMetrics(path, shard string) (int64, error) {
	totalSize := int64(0)

	// vector storage consists of:
	// 1) size of vector folder - these are:
	//     - the compressed vectors stored in their own folder each
	//     - the flat index extra copy of the uncompressed vectors (if flat index is used)
	// 2) size of uncompressed vectors stored in dimensions bucket. The size of these is calculated based on the number
	// of objects and their dimensionality. They need to be subtracted from the object bucket size to not count them twice.

	lsmPath := shardPathLSM(path, shard)
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "vector") {
			continue
		}
		if entry.IsDir() {
			fullPath := filepath.Join(lsmPath, entry.Name())
			dirSize, err := sumDir(fullPath)
			if err != nil {
				return 0, err
			}
			totalSize += dirSize
		}
	}
	return totalSize, nil
}

func CalculateUnloadedUncompressedVectorSize(ctx context.Context, logger logrus.FieldLogger, path, tenantName string, vectorConfig map[string]schemaConfig.VectorIndexConfig) (int64, error) {
	uncompressedSize := int64(0)
	// For each target vector, calculate storage size using dimensions bucket and config-based compression
	for targetVector := range vectorConfig {
		err := func() error {
			bucketPath := shardPathDimensionsLSM(path, tenantName)
			strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
			if err != nil {
				return fmt.Errorf("determine dimensions bucket strategy: %w", err)
			}

			// Get dimensions and object count from the dimensions bucket
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
				return err
			}
			defer bucket.Shutdown(ctx)

			dimensionality, err := CalculateTargetVectorDimensionsFromBucket(ctx, bucket, targetVector)
			if err != nil {
				return err
			}

			if dimensionality.Count != 0 && dimensionality.Dimensions != 0 {
				uncompressedSize += int64(dimensionality.Count) * int64(dimensionality.Dimensions) * 4
			}
			return nil
		}()
		if err != nil {
			return 0, err
		}
	}
	return uncompressedSize, nil
}

// CalculateUnloadedObjectsMetrics calculates both object count and storage size from disk
func CalculateUnloadedObjectsMetrics(logger logrus.FieldLogger, path, shardName string) (types.ObjectUsage, error) {
	// Parse all .cna files in the object store and sum them up
	totalObjectCount := int64(0)
	totalDiskSize := int64(0)

	// Use a single walk to avoid multiple filepath.Walk calls and reduce file descriptors
	objectStore := shardPathObjectsLSM(path, shardName)
	if err := filepath.Walk(objectStore, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only count files, not directories
		if !info.IsDir() {
			totalDiskSize += info.Size()

			// Look for .cna files (net count additions)
			if strings.HasSuffix(info.Name(), lsmkv.CountNetAdditionsFileSuffix) {
				count, err := lsmkv.ReadCountNetAdditionsFile(path)
				if err != nil {
					logger.WithField("path", path).WithField("shard", shardName).WithError(err).Warn("failed to read .cna file")
					return err
				}
				totalObjectCount += count
			}

			// Look for .metadata files (bloom filters + count net additions)
			if strings.HasSuffix(info.Name(), lsmkv.MetadataFileSuffix) {
				count, err := lsmkv.ReadObjectCountFromMetadataFile(path)
				if err != nil {
					logger.WithField("path", path).WithField("shard", shardName).WithError(err).Warn("failed to read .metadata file")
					return err
				}
				totalObjectCount += count
			}
		}

		return nil
	}); err != nil {
		return types.ObjectUsage{}, err
	}

	// If we can't determine object count, return the disk size as fallback
	return types.ObjectUsage{
		Count:        totalObjectCount,
		StorageBytes: totalDiskSize,
	}, nil
}

// CalculateUnloadedIndicesSize calculates both object count and storage size for a cold tenant without loading it into memory
func CalculateUnloadedIndicesSize(path, shardName string) (uint64, error) {
	totalSize := uint64(0)

	// get the storage of all lsm properties that are not objects or vector
	includedPrefixes := []string{helpers.DimensionsBucketLSM, helpers.BucketFromPropNameLSM("")}

	// check all vector folders and add their sizes
	lsmPath := shardPathLSM(path, shardName)
	entries, err := os.ReadDir(lsmPath)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		included := false
		for _, prefix := range includedPrefixes {
			if strings.HasPrefix(entry.Name(), prefix) {
				included = true
				break
			}
		}
		if !included {
			continue
		}

		if entry.IsDir() {
			fullPath := filepath.Join(lsmPath, entry.Name())
			dirSize, err := sumDir(fullPath)
			if err != nil {
				return 0, err
			}
			totalSize += uint64(dirSize)
		}
	}
	return totalSize, nil
}

// CalculateShardStorage calculates the full storage used by a shard, including objects, vectors, and indices
func CalculateShardStorage(path, shardName string) (uint64, error) {
	totalSize := uint64(0)

	// check all vector folders and add their sizes
	shardPath := filepath.Join(path, shardName)
	if err := filepath.Walk(shardPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only count files, not directories
		if info.IsDir() {
			return nil
		}
		totalSize += uint64(info.Size())

		return nil
	}); err != nil {
		return 0, err
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

		c := b.MapCursor()
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

// sumDir calculates the total size of all files in a directory recursively
// Note that we sum up the logical file size, not the actual disk usage which might be slightly higher. This is only
// relevant for very small files where the filesystem block size matters. In practice this is not relevant for us.
func sumDir(dirPath string) (int64, error) {
	size := int64(0)
	err := filepath.Walk(dirPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
