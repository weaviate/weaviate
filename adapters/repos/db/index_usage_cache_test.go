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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// populatedObjectsCount is how many objects setupPopulatedLazyIndex writes.
const populatedObjectsCount = int64(20)

// savedObjectsCount marks a shard usage as coming from the saved file: a report
// serving it read the file instead of the shard.
const savedObjectsCount = int64(7)

// TestUnloadedShardUsageSavedToDisk pins which saved usage a cold shard serves.
// Nothing deletes the file of a shard that stays cold, so a UsageDiskVersion bump
// or a new field only reaches those shards if what they hold makes them recompute.
func TestUnloadedShardUsageSavedToDisk(t *testing.T) {
	currentFingerprint, err := shardusage.VectorConfigsFingerprint(nil)
	require.NoError(t, err)

	tests := []struct {
		name             string
		savedVersion     int
		savedFingerprint string
		wantObjectsCount int64
	}{
		{
			name:             "usage of the current version is served",
			savedVersion:     types.UsageDiskVersion,
			savedFingerprint: currentFingerprint,
			wantObjectsCount: savedObjectsCount,
		},
		{
			name:             "usage of an older version is recomputed",
			savedVersion:     types.UsageDiskVersion - 1,
			savedFingerprint: currentFingerprint,
			wantObjectsCount: populatedObjectsCount,
		},
		{
			name:             "usage of a newer version is recomputed",
			savedVersion:     types.UsageDiskVersion + 1,
			savedFingerprint: currentFingerprint,
			wantObjectsCount: populatedObjectsCount,
		},
		{
			// what a build that saved no fingerprint left behind
			name:             "usage without a fingerprint is recomputed",
			savedVersion:     types.UsageDiskVersion,
			savedFingerprint: "",
			wantObjectsCount: populatedObjectsCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index, _ := setupPopulatedLazyIndex(ctx, t, usageIndexParams{})
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			writeSavedShardUsage(t, index.path(), tenantName, &types.UsageDisk{
				Version:                  tt.savedVersion,
				VectorConfigsFingerprint: tt.savedFingerprint,
				ShardUsage:               &types.ShardUsage{Name: tenantName, ObjectsCount: savedObjectsCount},
			})

			usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(1), true, nil)
			require.NoError(t, err)
			require.Len(t, usage.Shards, 1)
			assert.Equal(t, tt.wantObjectsCount, usage.Shards[0].ObjectsCount)
		})
	}
}

// TestUnloadedShardUsageAfterVectorConfigChange pins that a cold shard stops
// serving usage computed from vector configs the collection no longer has. Only
// loading the shard deletes the saved file, so a tenant left cold would otherwise
// keep reporting a dropped vector index — and the object/vector byte split that
// went with it — for good. [TestUnloadedShardUsageSavedToDisk] covers what the
// saved record itself has to hold to be served.
func TestUnloadedShardUsageAfterVectorConfigChange(t *testing.T) {
	hnswConfig := func(mutate func(*enthnsw.UserConfig)) models.VectorConfig {
		cfg := enthnsw.NewDefaultUserConfig()
		mutate(&cfg)
		return models.VectorConfig{VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg}
	}
	live := hnswConfig(func(*enthnsw.UserConfig) {})
	dropped := models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}

	tests := []struct {
		name string
		// change is applied to the configs the second report runs with. The first
		// report always runs with the legacy vector plus "first" and "second".
		change func(map[string]models.VectorConfig)
		// servedFromFile says whether the second report may still answer from the
		// file the first one saved.
		servedFromFile bool
		// wantCompressions maps every reported vector name to its compression.
		wantCompressions map[string]string
	}{
		{
			name:             "configs unchanged, saved usage is served",
			change:           func(map[string]models.VectorConfig) {},
			servedFromFile:   true,
			wantCompressions: map[string]string{"": "standard", "first": "standard", "second": "standard"},
		},
		{
			name:             "one vector dropped",
			change:           func(c map[string]models.VectorConfig) { c["second"] = dropped },
			wantCompressions: map[string]string{"": "standard", "first": "standard"},
		},
		{
			name: "every vector dropped",
			change: func(c map[string]models.VectorConfig) {
				for targetVector := range c {
					c[targetVector] = dropped
				}
			},
			wantCompressions: map[string]string{},
		},
		{
			name:             "a vector added",
			change:           func(c map[string]models.VectorConfig) { c["third"] = live },
			wantCompressions: map[string]string{"": "standard", "first": "standard", "second": "standard", "third": "standard"},
		},
		{
			name: "a vector reconfigured",
			change: func(c map[string]models.VectorConfig) {
				c["second"] = hnswConfig(func(cfg *enthnsw.UserConfig) { cfg.BQ.Enabled = true })
			},
			wantCompressions: map[string]string{"": "standard", "first": "standard", "second": "bq"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index, vectorConfigs := setupPopulatedLazyIndex(ctx, t, usageIndexParams{
				namedVectors:     map[string]models.VectorConfig{"first": live, "second": live},
				vectorDimensions: 32,
			})
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			// the first report is what saves the file the second one may serve
			_, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			markSavedShardUsage(t, index.path(), tenantName)

			tt.change(vectorConfigs)
			usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			require.Len(t, usage.Shards, 1)

			wantObjectsCount := populatedObjectsCount
			if tt.servedFromFile {
				wantObjectsCount = savedObjectsCount
			}
			assert.Equal(t, wantObjectsCount, usage.Shards[0].ObjectsCount)
			gotCompressions := map[string]string{}
			for _, namedVector := range usage.Shards[0].NamedVectors {
				gotCompressions[namedVector.Name] = namedVector.Compression
			}
			assert.Equal(t, tt.wantCompressions, gotCompressions)

			// the recompute must have saved usage the changed configs can serve again,
			// or every later report pays for the shard anew
			markSavedShardUsage(t, index.path(), tenantName)
			again, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			require.Len(t, again.Shards, 1)
			assert.Equal(t, savedObjectsCount, again.Shards[0].ObjectsCount)

			if tt.servedFromFile {
				return
			}
			// a recompute must land on what a shard with no saved file reports
			require.NoError(t, os.Remove(savedShardUsagePath(index.path(), tenantName)))
			fresh, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			require.Len(t, fresh.Shards, 1)
			assert.Equal(t, fresh.Shards[0], usage.Shards[0])
		})
	}
}

// savedShardUsagePath repeats the name of the file [shardusage.SaveComputedUsageData]
// writes, because the package keeps it private.
func savedShardUsagePath(indexPath, shardName string) string {
	return filepath.Join(indexPath, shardName, "usage.json.tmp")
}

// writeSavedShardUsage writes the file a cold shard's usage is served from,
// at an arbitrary version and fingerprint.
func writeSavedShardUsage(t *testing.T, indexPath, shardName string, saved *types.UsageDisk) {
	t.Helper()

	data, err := json.Marshal(saved)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(savedShardUsagePath(indexPath, shardName), data, 0o600))
}

// markSavedShardUsage stamps savedObjectsCount on the usage a report saved, leaving
// everything the cache validates untouched.
func markSavedShardUsage(t *testing.T, indexPath, shardName string) {
	t.Helper()

	data, err := os.ReadFile(savedShardUsagePath(indexPath, shardName))
	require.NoError(t, err)
	saved := &types.UsageDisk{}
	require.NoError(t, json.Unmarshal(data, saved))
	require.NotNil(t, saved.ShardUsage, "the first report must have saved usage")
	saved.ShardUsage.ObjectsCount = savedObjectsCount
	writeSavedShardUsage(t, indexPath, shardName, saved)
}
