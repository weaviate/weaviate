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
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestShardPathDimensionsLSM(t *testing.T) {
	tests := []struct {
		name      string
		indexPath string
		shardName string
		expected  string
	}{
		{
			name:      "basic path",
			indexPath: "/data/index",
			shardName: "shard1",
			expected:  "/data/index/shard1/lsm/dimensions",
		},
		{
			name:      "empty shard name",
			indexPath: "/data/index",
			shardName: "",
			expected:  "/data/index/lsm/dimensions",
		},
		{
			name:      "empty index path",
			indexPath: "",
			shardName: "shard1",
			expected:  "shard1/lsm/dimensions",
		},
		{
			name:      "both empty",
			indexPath: "",
			shardName: "",
			expected:  "lsm/dimensions",
		},
		{
			name:      "relative paths",
			indexPath: "data/index",
			shardName: "shard1",
			expected:  "data/index/shard1/lsm/dimensions",
		},
		{
			name:      "with special characters in shard name",
			indexPath: "/data/index",
			shardName: "shard-1_test",
			expected:  "/data/index/shard-1_test/lsm/dimensions",
		},
		{
			name:      "with spaces in shard name",
			indexPath: "/data/index",
			shardName: "shard 1",
			expected:  "/data/index/shard 1/lsm/dimensions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shardPathDimensionsLSM(tt.indexPath, tt.shardName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShardPathObjectsLSM(t *testing.T) {
	tests := []struct {
		name      string
		indexPath string
		shardName string
		expected  string
	}{
		{
			name:      "basic path",
			indexPath: "/data/index",
			shardName: "shard1",
			expected:  "/data/index/shard1/lsm/objects",
		},
		{
			name:      "empty shard name",
			indexPath: "/data/index",
			shardName: "",
			expected:  "/data/index/lsm/objects",
		},
		{
			name:      "empty index path",
			indexPath: "",
			shardName: "shard1",
			expected:  "shard1/lsm/objects",
		},
		{
			name:      "both empty",
			indexPath: "",
			shardName: "",
			expected:  "lsm/objects",
		},
		{
			name:      "relative paths",
			indexPath: "data/index",
			shardName: "shard1",
			expected:  "data/index/shard1/lsm/objects",
		},
		{
			name:      "with special characters in shard name",
			indexPath: "/data/index",
			shardName: "shard-1_test",
			expected:  "/data/index/shard-1_test/lsm/objects",
		},
		{
			name:      "with spaces in shard name",
			indexPath: "/data/index",
			shardName: "shard 1",
			expected:  "/data/index/shard 1/lsm/objects",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shardPathObjectsLSM(tt.indexPath, tt.shardName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCalculateUnloadedObjectsMetrics(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	tenantName := "tenant"

	for _, metadata := range []bool{true, false} {
		t.Run(fmt.Sprintf("metadata=%v", metadata), func(t *testing.T) {
			dirName := t.TempDir()
			bucketFolder := shardPathObjectsLSM(dirName, tenantName)

			b, err := lsmkv.NewBucketCreator().NewBucket(ctx, bucketFolder, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithCalcCountNetAdditions(true), lsmkv.WithWriteMetadata(metadata))
			require.NoError(t, err)
			defer b.Shutdown(ctx)

			require.NoError(t, b.Put([]byte("hello1"), []byte("world1")))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.Put([]byte("hello2"), []byte("world2")))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.Put([]byte("hello3"), []byte("world3")))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.Put([]byte("hello4"), []byte("world4")))
			require.NoError(t, b.FlushMemtable())

			fileTypes := getFileTypeCount(t, bucketFolder)
			require.Equal(t, 4, fileTypes[".db"])
			require.Equal(t, 0, fileTypes[".wal"])
			if metadata {
				require.Equal(t, 4, fileTypes[".metadata"])
			} else {
				require.Equal(t, 4, fileTypes[".cna"])
			}

			metrics, err := CalculateUnloadedObjectsMetrics(logger, dirName, "tenant", true)
			require.NoError(t, err)
			require.Equal(t, metrics.Count, int64(4))

			// add another key but dont flush => will not be included in count
			require.NoError(t, b.Put([]byte("hello5"), []byte("world5")))

			fileTypes = getFileTypeCount(t, bucketFolder)
			require.Equal(t, 4, fileTypes[".db"])
			require.Equal(t, 1, fileTypes[".wal"])
			if metadata {
				require.Equal(t, 4, fileTypes[".metadata"])
			} else {
				require.Equal(t, 4, fileTypes[".cna"])
			}

			metrics, err = CalculateUnloadedObjectsMetrics(logger, dirName, "tenant", true)
			require.NoError(t, err)
			require.Equal(t, metrics.Count, int64(4))
		})
	}
}

func getFileTypeCount(t *testing.T, path string) map[string]int {
	t.Helper()
	fileTypes := map[string]int{}
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		fileTypes[filepath.Ext(entry.Name())] += 1
	}
	return fileTypes
}

func TestStorageCalculation(t *testing.T) {
	logger, _ := test.NewNullLogger()

	dirName := t.TempDir()

	// create a LSM path for a shard
	lsmFolder := shardPathLSM(dirName, "shard1")
	require.NoError(t, os.MkdirAll(lsmFolder, 0o777))

	buckets := []string{helpers.ObjectsBucketLSM, helpers.DimensionsBucketLSM, "vectors", "vectors_compressed", "vectors_compressed_named_vector", "property_someProp_searchable", "property_someProp", "property__id"}
	sizeTracker := make(map[string]uint64, len(buckets))

	// create different buckets with dummy files with varying sizes
	for _, bucket := range buckets {
		bucketPath := filepath.Join(lsmFolder, bucket)
		require.NoError(t, os.MkdirAll(bucketPath, 0o777))
		sizeTracker[bucket] = 0

		// create some dummy files
		for i := 0; i < rand.Intn(10); i++ {
			filePath := filepath.Join(bucketPath, fmt.Sprintf("file%d.db", i))
			f, err := os.Create(filePath)
			require.NoError(t, err)

			size := rand.Intn(10000)
			sizeTracker[bucket] += uint64(size)
			data := make([]byte, size)
			_, err = f.Write(data)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
	}

	// calculate storage and compare
	expectedTotal := uint64(0)
	for _, size := range sizeTracker {
		expectedTotal += size
	}

	objectsBytes, err := CalculateUnloadedObjectsMetrics(logger, dirName, "shard1", false)
	require.NoError(t, err)
	require.Equal(t, sizeTracker[helpers.ObjectsBucketLSM], uint64(objectsBytes.StorageBytes))

	vectorBytes, err := CalculateUnloadedVectorsMetrics(lsmFolder, buckets)
	require.NoError(t, err)
	require.Equal(t, sizeTracker["vectors"]+sizeTracker["vectors_compressed"]+sizeTracker["vectors_compressed_named_vector"], uint64(vectorBytes))

	indexBytes, err := CalculateUnloadedIndicesSize(lsmFolder, buckets)
	require.NoError(t, err)
	require.Equal(t, sizeTracker["property_someProp_searchable"]+sizeTracker["property_someProp"]+sizeTracker["property__id"]+sizeTracker[helpers.DimensionsBucketLSM], indexBytes)

	vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := CalculateNonLSMStorage(dirName, "shard1")
	require.NoError(t, err)
	require.Equal(t, expectedTotal, vectorCommitLogsStorageSize+otherNonLSMFoldersStorageSize+indexBytes+uint64(objectsBytes.StorageBytes)+uint64(vectorBytes))
}

func BenchmarkStorageCalculation(b *testing.B) {
	for n := 0; n < b.N; n++ {
		logger, _ := test.NewNullLogger()

		dirName := b.TempDir()

		// create a LSM path for a shard
		lsmFolder := shardPathLSM(dirName, "shard1")
		require.NoError(b, os.MkdirAll(lsmFolder, 0o777))

		buckets := []string{helpers.ObjectsBucketLSM, helpers.DimensionsBucketLSM, "vectors", "vectors_compressed", "vectors_compressed_named_vector", "property_someProp_searchable", "property_someProp", "property__id"}
		sizeTracker := make(map[string]uint64, len(buckets))

		// create different buckets with dummy files with varying sizes
		for _, bucket := range buckets {
			bucketPath := filepath.Join(lsmFolder, bucket)
			require.NoError(b, os.MkdirAll(bucketPath, 0o777))
			sizeTracker[bucket] = 0

			// create some dummy files
			for i := 0; i < 10; i++ {
				filePath := filepath.Join(bucketPath, fmt.Sprintf("file%d.db", i))
				f, err := os.Create(filePath)
				require.NoError(b, err)

				size := 8000
				sizeTracker[bucket] += uint64(size)
				data := make([]byte, size)
				_, err = f.Write(data)
				require.NoError(b, err)
				require.NoError(b, f.Close())
			}
		}

		// calculate storage and compare
		expectedTotal := uint64(0)
		for _, size := range sizeTracker {
			expectedTotal += size
		}

		objectsBytes, err := CalculateUnloadedObjectsMetrics(logger, dirName, "shard1", false)
		require.NoError(b, err)
		require.Equal(b, sizeTracker[helpers.ObjectsBucketLSM], uint64(objectsBytes.StorageBytes))

		vectorBytes, err := CalculateUnloadedVectorsMetrics(lsmFolder, buckets)
		require.NoError(b, err)
		require.Equal(b, sizeTracker["vectors"]+sizeTracker["vectors_compressed"]+sizeTracker["vectors_compressed_named_vector"], uint64(vectorBytes))

		indexBytes, err := CalculateUnloadedIndicesSize(lsmFolder, buckets)
		require.NoError(b, err)
		require.Equal(b, sizeTracker["property_someProp_searchable"]+sizeTracker["property_someProp"]+sizeTracker["property__id"]+sizeTracker[helpers.DimensionsBucketLSM], indexBytes)

		vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := CalculateNonLSMStorage(dirName, "shard1")
		require.NoError(b, err)
		require.Equal(b, expectedTotal, vectorCommitLogsStorageSize+otherNonLSMFoldersStorageSize+indexBytes+uint64(objectsBytes.StorageBytes)+uint64(vectorBytes))

	}
}

func TestCalculateNonLSMStorage_NestedHFreshDirectories(t *testing.T) {
	dirName := t.TempDir()
	shardPath := filepath.Join(dirName, "shard1")
	require.NoError(t, os.MkdirAll(shardPath, 0o777))

	topLevelCommitLog := filepath.Join(shardPath, "main.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(topLevelCommitLog, 0o777))
	topLevelCommitLogFile := filepath.Join(topLevelCommitLog, "file1")
	require.NoError(t, os.WriteFile(topLevelCommitLogFile, make([]byte, 1000), 0o644))

	topLevelSnapshot := filepath.Join(shardPath, "main.hnsw.snapshot.d")
	require.NoError(t, os.MkdirAll(topLevelSnapshot, 0o777))
	topLevelSnapshotFile := filepath.Join(topLevelSnapshot, "file2")
	require.NoError(t, os.WriteFile(topLevelSnapshotFile, make([]byte, 2000), 0o644))

	hfreshDir := filepath.Join(shardPath, "main.hfresh.d")
	require.NoError(t, os.MkdirAll(hfreshDir, 0o777))

	nestedCommitLog := filepath.Join(hfreshDir, "main_centroids.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(nestedCommitLog, 0o777))
	nestedCommitLogFile := filepath.Join(nestedCommitLog, "file3")
	require.NoError(t, os.WriteFile(nestedCommitLogFile, make([]byte, 3000), 0o644))

	nestedSnapshot := filepath.Join(hfreshDir, "main_centroids.hnsw.snapshot.d")
	require.NoError(t, os.MkdirAll(nestedSnapshot, 0o777))
	nestedSnapshotFile := filepath.Join(nestedSnapshot, "file4")
	require.NoError(t, os.WriteFile(nestedSnapshotFile, make([]byte, 4000), 0o644))

	nestedQueue := filepath.Join(hfreshDir, "analyze.queue.d")
	require.NoError(t, os.MkdirAll(nestedQueue, 0o777))
	nestedQueueFile := filepath.Join(nestedQueue, "file5")
	require.NoError(t, os.WriteFile(nestedQueueFile, make([]byte, 5000), 0o644))

	otherDir := filepath.Join(hfreshDir, "other_dir")
	require.NoError(t, os.MkdirAll(otherDir, 0o777))
	otherDirFile := filepath.Join(otherDir, "file6")
	require.NoError(t, os.WriteFile(otherDirFile, make([]byte, 6000), 0o644))

	hfreshFile := filepath.Join(hfreshDir, "file7")
	require.NoError(t, os.WriteFile(hfreshFile, make([]byte, 7000), 0o644))

	otherTopLevel := filepath.Join(shardPath, "other_top_level")
	require.NoError(t, os.MkdirAll(otherTopLevel, 0o777))
	otherTopLevelFile := filepath.Join(otherTopLevel, "file8")
	require.NoError(t, os.WriteFile(otherTopLevelFile, make([]byte, 8000), 0o644))

	vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := CalculateNonLSMStorage(dirName, "shard1")
	require.NoError(t, err)

	// Expected commitlog storage:
	// - topLevelCommitLog: 1000
	// - topLevelSnapshot: 2000
	// - nestedCommitLog: 3000
	// - nestedSnapshot: 4000
	// - nestedQueue: 5000
	expectedCommitLogSize := uint64(1000 + 2000 + 3000 + 4000 + 5000)

	// Expected other storage:
	// - otherDir: 6000
	// - hfreshFile: 7000
	// - otherTopLevel: 8000
	expectedOtherSize := uint64(6000 + 7000 + 8000)

	assert.Equal(t, expectedCommitLogSize, vectorCommitLogsStorageSize, "commitlog/snapshot/queue storage should match")
	assert.Equal(t, expectedOtherSize, otherNonLSMFoldersStorageSize, "other storage should match")
}

func TestCalculateNonLSMStorage_HFreshOneLevelDeep(t *testing.T) {
	dirName := t.TempDir()
	shardPath := filepath.Join(dirName, "shard1")
	require.NoError(t, os.MkdirAll(shardPath, 0o777))

	// Create hfresh.d directory
	hfreshDir := filepath.Join(shardPath, "main.hfresh.d")
	require.NoError(t, os.MkdirAll(hfreshDir, 0o777))

	// Create nested commitlog.d (one level deep)
	nestedCommitLog := filepath.Join(hfreshDir, "main_centroids.hnsw.commitlog.d")
	require.NoError(t, os.MkdirAll(nestedCommitLog, 0o777))
	nestedCommitLogFile := filepath.Join(nestedCommitLog, "file1")
	require.NoError(t, os.WriteFile(nestedCommitLogFile, make([]byte, 1000), 0o644))

	// Create nested snapshot.d (one level deep)
	nestedSnapshot := filepath.Join(hfreshDir, "main_centroids.hnsw.snapshot.d")
	require.NoError(t, os.MkdirAll(nestedSnapshot, 0o777))
	nestedSnapshotFile := filepath.Join(nestedSnapshot, "file2")
	require.NoError(t, os.WriteFile(nestedSnapshotFile, make([]byte, 2000), 0o644))

	// Create nested queue.d (one level deep)
	nestedQueue := filepath.Join(hfreshDir, "analyze.queue.d")
	require.NoError(t, os.MkdirAll(nestedQueue, 0o777))
	nestedQueueFile := filepath.Join(nestedQueue, "file3")
	require.NoError(t, os.WriteFile(nestedQueueFile, make([]byte, 3000), 0o644))

	// Calculate storage - nested directories should be counted
	vectorCommitLogsStorageSize, otherNonLSMFoldersStorageSize, err := CalculateNonLSMStorage(dirName, "shard1")
	require.NoError(t, err)

	// The nested commitlog.d, snapshot.d, and queue.d should be found and counted
	expectedCommitLogSize := uint64(1000 + 2000 + 3000)
	assert.Equal(t, expectedCommitLogSize, vectorCommitLogsStorageSize, "nested commitlog/snapshot/queue.d should be counted")
	assert.Equal(t, uint64(0), otherNonLSMFoldersStorageSize, "no other storage expected")
}
