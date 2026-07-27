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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestIndex_UsageForCollection_IsIdempotent pins an observer effect: calculating
// a cold shard's usage wrote files into the very directories it measures, so a
// second calculation of unchanged data reported a bigger shard than the first.
// Two sources, both counted:
//
//   - the usage cache the calculation itself saves into the shard root
//   - the bloom filter materialised when the dimensions bucket is opened
//
// The inflated figure is then saved back into the cache and served from there
// on every later report, so a single recalculation permanently overstated the
// shard. This surfaced as `acceptance large (python)` failures in
// test_usage.py, where hot and cold reports differed by exactly the size of the
// cache file.
func TestIndex_UsageForCollection_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	tenantName := "test-tenant"

	index := setupPopulatedLazyIndex(ctx, t)
	t.Cleanup(func() { _ = index.Shutdown(ctx) })

	// drop the shard from the in-memory map so usage takes the cold path
	index.shards.LoadAndDelete(tenantName)

	vectorConfigs := map[string]models.VectorConfig{
		"": {
			VectorIndexType:   "hnsw",
			VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		},
	}

	first, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
	require.NoError(t, err)
	require.Len(t, first.Shards, 1)

	// A cache hit would trivially return the same numbers and hide the effect.
	// Make the second call recompute, which is what a concurrent report or a
	// cache left behind by an older UsageDiskVersion does.
	cachePath := filepath.Join(index.path(), tenantName, "usage.json.tmp")
	stale, err := os.ReadFile(cachePath)
	require.NoError(t, err, "cold usage calculation must have written its cache")
	require.NoError(t, os.WriteFile(cachePath, make([]byte, len(stale)), 0o600))

	second, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
	require.NoError(t, err)
	require.Len(t, second.Shards, 1)

	assert.Equal(t, first.Shards[0].ObjectsStorageBytes, second.Shards[0].ObjectsStorageBytes)
	assert.Equal(t, first.Shards[0].VectorStorageBytes, second.Shards[0].VectorStorageBytes)
	assert.Equal(t, first.Shards[0].IndexStorageBytes, second.Shards[0].IndexStorageBytes)
	assert.Equal(t, first.Shards[0].FullShardStorageBytes, second.Shards[0].FullShardStorageBytes,
		"recalculating an unchanged shard must report the same size")
}
