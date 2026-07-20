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
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestApplyLazyShardAutoDetection(t *testing.T) {
	tests := []struct {
		name               string
		mtEnabled          bool
		localShardCount    int
		totalShardSizeGib  float64
		countThreshold     int
		sizeThresholdGib   float64
		expectedEnableLazy bool
	}{
		{
			name:               "non-multi-tenant always disabled",
			mtEnabled:          false,
			localShardCount:    10,
			totalShardSizeGib:  500,
			countThreshold:     1,
			sizeThresholdGib:   1,
			expectedEnableLazy: false,
		},
		{
			name:               "multi-tenant, below thresholds",
			mtEnabled:          true,
			localShardCount:    10,
			totalShardSizeGib:  10,
			countThreshold:     1000,
			sizeThresholdGib:   100,
			expectedEnableLazy: false,
		},
		{
			name:               "multi-tenant, shard count above threshold",
			mtEnabled:          true,
			localShardCount:    2000,
			totalShardSizeGib:  10,
			countThreshold:     1000,
			sizeThresholdGib:   100,
			expectedEnableLazy: true,
		},
		{
			name:               "multi-tenant, size above threshold",
			mtEnabled:          true,
			localShardCount:    10,
			totalShardSizeGib:  200,
			countThreshold:     1000,
			sizeThresholdGib:   100,
			expectedEnableLazy: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalBytes := uint64(tt.totalShardSizeGib * 1024 * 1024 * 1024)
			got := shouldAutoLazyLoadShards(
				tt.mtEnabled,
				tt.localShardCount,
				totalBytes,
				tt.countThreshold,
				tt.sizeThresholdGib,
			)
			require.Equal(t, tt.expectedEnableLazy, got)
		})
	}
}

// TestNewShard_AbortsWhenUsageFileRemovalFails pins that NewShard propagates a
// failure to remove the stale precomputed usage file, rather than silently
// ignoring it. Otherwise the outdated usage.json.tmp survives and later gets
// served as the shard's usage once it is treated as unloaded (deactivated/COLD
// tenant, unloaded lazy shard), reporting wrong object counts/storage bytes.
func TestNewShard_AbortsWhenUsageFileRemovalFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test cannot run as root")
	}

	ctx := context.Background()
	className := "UsageFileCleanup"
	shard, index := testShard(t, ctx, className)
	shardName := shard.Name()
	// close the loaded shard so NewShard can re-init the same on-disk shard
	require.NoError(t, shard.Shutdown(ctx))

	// Make the usage-file removal fail in isolation: create usage.json.tmp as a
	// non-empty directory and drop write permission on it, so os.RemoveAll fails
	// on its child while the (writable) shard dir leaves the rest of NewShard
	// unaffected.
	usageTmp := path.Join(index.path(), shardName, "usage.json.tmp")
	require.NoError(t, os.MkdirAll(usageTmp, 0o700))
	require.NoError(t, os.WriteFile(path.Join(usageTmp, "child"), []byte("x"), 0o600))
	require.NoError(t, os.Chmod(usageTmp, 0o500))
	t.Cleanup(func() { _ = os.Chmod(usageTmp, 0o700) })

	_, err := NewShard(ctx, nil, shardName, index, &models.Class{Class: className},
		index.centralJobQueue, index.scheduler, index.indexCheckpoints,
		index.shardReindexer, false, index.bitmapBufPool)
	require.Error(t, err)
	require.ErrorContains(t, err, "remove computed usage file")
}

func TestTotalShardSizeBytes_FallsBackToDirSizeWhenNoMeta(t *testing.T) {
	tmpDir := t.TempDir()

	db := &DB{
		logger: logrus.New(),
		config: Config{
			RootPath: tmpDir,
		},
	}

	className := schema.ClassName("MyClass")
	indexPath := path.Join(tmpDir, indexID(className))
	shardName := "shard1"
	shardPath := path.Join(indexPath, shardName)

	require.NoError(t, os.MkdirAll(shardPath, 0o777))

	data := []byte("0123456789") // 10 bytes
	require.NoError(t, os.WriteFile(path.Join(shardPath, "data.bin"), data, 0o644))

	got := db.totalShardSizeBytes(className, []string{shardName}, 0)
	require.Equal(t, uint64(len(data)), got)
}

func TestTotalShardSizeBytes_PrefersMetaFileWhenPresent(t *testing.T) {
	tmpDir := t.TempDir()

	db := &DB{
		logger: logrus.New(),
		config: Config{
			RootPath: tmpDir,
		},
	}

	className := schema.ClassName("MyClass")
	indexPath := path.Join(tmpDir, indexID(className))
	shardName := "shard1"
	shardPath := path.Join(indexPath, shardName)

	require.NoError(t, os.MkdirAll(shardPath, 0o777))

	// Write a meta file with a known size
	const fullShardBytes = uint64(1234)
	err := shardusage.SaveComputedUsageData(indexPath, shardName, &types.ShardUsage{
		Name:                  shardName,
		FullShardStorageBytes: fullShardBytes,
	})
	require.NoError(t, err)

	// Also create some on-disk data to ensure we really prefer the meta value
	data := []byte("0123456789") // 10 bytes
	require.NoError(t, os.WriteFile(path.Join(shardPath, "data.bin"), data, 0o644))

	got := db.totalShardSizeBytes(className, []string{shardName}, 0)
	require.Equal(t, fullShardBytes, got)
}

// TestWarnUnmatchedRoaringSetInMemoryEntries pins the one-time startup warning
// for INDEX_ROARINGSET_IN_MEMORY entries that match no live
// <Collection>.<property>: a well-formed but unmatched entry (typo, wrong
// case) must be loud, a matched one silent.
func TestWarnUnmatchedRoaringSetInMemoryEntries(t *testing.T) {
	mkSchema := func() schema.Schema {
		return schema.Schema{Objects: &models.Schema{Classes: []*models.Class{{
			Class: "Article",
			Properties: []*models.Property{
				{Name: "title", DataType: schema.DataTypeText.PropString()},
			},
		}}}}
	}

	warnMessages := func(hook *logrustest.Hook) []string {
		var msgs []string
		for _, entry := range hook.AllEntries() {
			msgs = append(msgs, entry.Message)
		}
		return msgs
	}

	t.Run("unmatched entries warn", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		warnUnmatchedRoaringSetInMemoryEntries(logger, mkSchema(), entcfg.StringSet{
			"Missing.title":   {}, // no such collection
			"Article.summary": {}, // no such property
		})

		msgs := warnMessages(hook)
		require.Len(t, msgs, 2)
		require.Contains(t, msgs, `INDEX_ROARINGSET_IN_MEMORY entry "Missing.title" matches no collection`)
		require.Contains(t, msgs, `INDEX_ROARINGSET_IN_MEMORY entry "Article.summary" matches no property on collection "Article"`)
	})

	t.Run("matched entry stays silent", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		warnUnmatchedRoaringSetInMemoryEntries(logger, mkSchema(), entcfg.StringSet{"Article.title": {}})
		require.Empty(t, hook.AllEntries())
	})

	t.Run("entry warns when schema is empty", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		warnUnmatchedRoaringSetInMemoryEntries(logger, schema.Schema{}, entcfg.StringSet{"Article.title": {}})
		require.Len(t, warnMessages(hook), 1)
	})
}
