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
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
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
