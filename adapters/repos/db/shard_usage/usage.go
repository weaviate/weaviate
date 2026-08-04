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

	scan, err := ScanTargetVectorDimensions(ctx, bucket, targetVector, false)
	if err != nil {
		return types.Dimensionality{}, err
	}
	return scan.Dimensionality, nil
}

// CalculateUnloadedDimensionsUsageAll calculates dimensions and object count for all target
// vectors of an unloaded shard without loading it into memory. The dimensions bucket is opened
// once and shared by all target vector calculations, instead of once per target vector.
// Target vectors present in needTotal additionally get the object count summed across all rows
// (the MUVERA reading); the others keep the cheaper first-complete-row scan.
func CalculateUnloadedDimensionsUsageAll(ctx context.Context,
	logger logrus.FieldLogger, path, tenantName string, targetVectors []string,
	needTotal map[string]int,
) (map[string]DimensionsScan, error) {
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

	scans := make(map[string]DimensionsScan, len(targetVectors))
	for _, targetVector := range targetVectors {
		_, withTotal := needTotal[targetVector]
		scan, err := ScanTargetVectorDimensions(ctx, bucket, targetVector, withTotal)
		if err != nil {
			return nil, err
		}
		scans[targetVector] = scan
	}
	return scans, nil
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

// DimensionsScan holds both readings the usage report needs from a single pass over the
// dimensions bucket.
type DimensionsScan struct {
	// Dimensionality is the first row with non-zero dimensions and a non-empty doc set —
	// the classic single-vector reading.
	types.Dimensionality
	// TotalCount is the number of objects with a vector, summed across every row.
	// Multi-vector objects are recorded under their varying per-object total dimensions,
	// so no single row counts them all. Only populated when the scan runs with needTotal.
	TotalCount int
}

// MuveraDimensionality reports the fixed MUVERA-encoded dimensionality (what is held in
// memory per object) with the object count summed across all rows. The scan must have run
// with needTotal.
func (s DimensionsScan) MuveraDimensionality(encodedDimensions int) types.Dimensionality {
	if s.TotalCount == 0 {
		return types.Dimensionality{}
	}
	return types.Dimensionality{Dimensions: encodedDimensions, Count: s.TotalCount}
}

// ScanTargetVectorDimensions calculates dimensions and object count for a target vector from
// an LSMKV bucket. With needTotal it additionally sums the object count across all rows (the
// MUVERA reading); without it the scan stops at the first complete row.
func ScanTargetVectorDimensions(ctx context.Context, b *lsmkv.Bucket, targetVector string,
	needTotal bool,
) (DimensionsScan, error) {
	scan := DimensionsScan{}

	if err := lsmkv.CheckExpectedStrategy(b.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyRoaringSet); err != nil {
		return scan, fmt.Errorf("scanTargetVectorDimensions: %w", err)
	}

	prefix := []byte(targetVector)
	nameLen := len(targetVector)
	expectedKeyLen := nameLen + 4 // vector name + uint32
	// addRow feeds one bucket row into both readings and reports whether the scan may stop.
	addRow := func(k []byte, count int) (done bool) {
		// a longer name sharing this prefix can sort before the target's own keys
		// ("texts…" before "text\x80…"); rows with dims=0 are objects without a vector
		if len(k) != expectedKeyLen {
			return false
		}
		dimLength := binary.LittleEndian.Uint32(k[nameLen:])
		if dimLength == 0 {
			return false
		}
		if scan.Dimensions == 0 || scan.Count == 0 {
			scan.Dimensions = int(dimLength)
			scan.Count = count
		}
		if needTotal {
			scan.TotalCount += count
		}
		// remaining rows cannot change a complete result unless the total is needed, and an
		// empty name matches every key so the prefix check in the loop condition never fires
		return !needTotal && scan.Dimensions != 0 && scan.Count != 0
	}
	var k []byte

	switch b.Strategy() {
	case lsmkv.StrategyMapCollection:
		// Since weaviate 1.34 default dimension bucket strategy is StrategyRoaringSet.
		// For backward compatibility StrategyMapCollection is still supported.

		c, err := b.MapCursor()
		if err != nil {
			return scan, fmt.Errorf("create cursor: %w", err)
		}
		defer c.Close()

		var v []lsmkv.MapPair
		if nameLen == 0 {
			k, v = c.First(ctx)
		} else {
			k, v = c.Seek(ctx, prefix)
		}
		for ; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next(ctx) {
			if addRow(k, len(v)) {
				break
			}
		}
	default:
		c := b.CursorRoaringSet()
		defer c.Close()

		var v *sroar.Bitmap
		if nameLen == 0 {
			k, v = c.First()
		} else {
			k, v = c.Seek(prefix)
		}
		for ; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if addRow(k, v.GetCardinality()) {
				break
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return scan, err
	}
	return scan, nil
}
