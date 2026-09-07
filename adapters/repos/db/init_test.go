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
	"math"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
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

// TestShouldComputeShardSizes pins that the startup shard-size sweep is skipped
// whenever its result cannot change the lazy-loading decision. The guard used to
// check only the count and size thresholds, so an explicit EnableLazyLoadShards
// setting still paid a walk over every shard directory before the decision
// short-circuited on it.
func TestShouldComputeShardSizes(t *testing.T) {
	enabled, disabled := true, false

	tests := []struct {
		name             string
		explicitLazyLoad *bool
		localShardCount  int
		countThreshold   int
		sizeThresholdGB  float64
		want             bool
	}{
		{
			name:            "auto-detection below the count threshold measures",
			localShardCount: 10,
			countThreshold:  1000,
			sizeThresholdGB: 100,
			want:            true,
		},
		{
			name:            "auto-detection at the count threshold measures",
			localShardCount: 1000,
			countThreshold:  1000,
			sizeThresholdGB: 100,
			want:            true,
		},
		{
			name:            "auto-detection above the count threshold skips",
			localShardCount: 1001,
			countThreshold:  1000,
			sizeThresholdGB: 100,
			want:            false,
		},
		{
			name:            "a zero size threshold skips",
			localShardCount: 10,
			countThreshold:  1000,
			sizeThresholdGB: 0,
			want:            false,
		},
		{
			// LAZY_LOAD_SHARD_SIZE_THRESHOLD_GB=NaN parses and passes validation,
			// and uint64(NaN*1<<30) is not even the same on every arch, so the
			// threshold it yields can never be meaningfully met
			name:            "a NaN size threshold skips",
			localShardCount: 10,
			countThreshold:  1000,
			sizeThresholdGB: math.NaN(),
			want:            false,
		},
		{
			name:             "an explicit enable skips",
			explicitLazyLoad: &enabled,
			localShardCount:  10,
			countThreshold:   1000,
			sizeThresholdGB:  100,
			want:             false,
		},
		{
			name:             "an explicit disable skips",
			explicitLazyLoad: &disabled,
			localShardCount:  10,
			countThreshold:   1000,
			sizeThresholdGB:  100,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldComputeShardSizes(
				tt.explicitLazyLoad,
				tt.localShardCount,
				tt.countThreshold,
				tt.sizeThresholdGB,
			))
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
	usageTmp := savedShardUsagePath(index.path(), shardName)
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
	const fullShardBytes = uint64(1234)
	// on-disk data, so a fallback to the directory size is distinguishable
	onDisk := []byte("0123456789")

	tests := []struct {
		name  string
		usage *types.ShardUsage
		// wantFromDir expects the shard's files to be summed instead of the saved
		// FullShardStorageBytes.
		wantFromDir bool
	}{
		{
			name: "saved usage is preferred",
			usage: &types.ShardUsage{
				Name:                  "shard1",
				FullShardStorageBytes: fullShardBytes,
			},
		},
		{
			// nothing to prefer, so the shard is sized from disk like an unsaved one
			name:        "a saved record holding no usage falls back to the directory size",
			wantFromDir: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			require.NoError(t, shardusage.SaveComputedUsageData(indexPath, shardName, tt.usage, ""))
			require.NoError(t, os.WriteFile(path.Join(shardPath, "data.bin"), onDisk, 0o644))

			want := fullShardBytes
			if tt.wantFromDir {
				saved, err := os.Stat(savedShardUsagePath(indexPath, shardName))
				require.NoError(t, err)
				want = uint64(len(onDisk)) + uint64(saved.Size())
			}
			require.Equal(t, want, db.totalShardSizeBytes(className, []string{shardName}, 0))
		})
	}
}
