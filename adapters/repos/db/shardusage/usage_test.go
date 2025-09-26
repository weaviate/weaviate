package shardusage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			metrics, err := CalculateUnloadedObjectsMetrics(logger, dirName, "tenant")
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

			metrics, err = CalculateUnloadedObjectsMetrics(logger, dirName, "tenant")
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
