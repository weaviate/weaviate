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

// TestIndex_UsageForCollection_IsIdempotent pins an observer effect: the cold usage
// calculation wrote files into the directories it measures (its own cache, and the
// bloom filter from opening the dimensions bucket), inflating every recalculation.
func TestIndex_UsageForCollection_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	tenantName := "test-tenant"

	index := setupPopulatedLazyIndex(ctx, t)
	t.Cleanup(func() { _ = index.Shutdown(ctx) })

	// force the cold path
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

	// A cache hit would hide the effect; recomputing is what a concurrent report or
	// a cache from an older UsageDiskVersion hits in production.
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
