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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestIndex_UsageForCollection_LoadedAndUnloadedAgree pins that one shard reports
// the same named vectors and the same full storage size whether it is loaded or
// not. The loaded path asks the shard's index, the unloaded path reads the shard's
// files. A bill must not change because a tenant happened to be active when the
// report ran.
func TestIndex_UsageForCollection_LoadedAndUnloadedAgree(t *testing.T) {
	hnswConfig := func(mutate func(*enthnsw.UserConfig)) models.VectorConfig {
		cfg := enthnsw.NewDefaultUserConfig()
		mutate(&cfg)
		return models.VectorConfig{VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg}
	}
	flatConfig := func(mutate func(*entflat.UserConfig)) models.VectorConfig {
		cfg := entflat.NewDefaultUserConfig()
		mutate(&cfg)
		return models.VectorConfig{VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg}
	}

	namedVectors := map[string]models.VectorConfig{
		"hnsw-standard": hnswConfig(func(cfg *enthnsw.UserConfig) {}),
		"hnsw-bq":       hnswConfig(func(cfg *enthnsw.UserConfig) { cfg.BQ.Enabled = true }),
		"hnsw-sq":       hnswConfig(func(cfg *enthnsw.UserConfig) { cfg.SQ.Enabled = true }),
		"hnsw-pq": hnswConfig(func(cfg *enthnsw.UserConfig) {
			cfg.PQ.Enabled = true
			cfg.PQ.Segments = 4
		}),
		"hnsw-rq": hnswConfig(func(cfg *enthnsw.UserConfig) { cfg.RQ.Enabled = true }),
		"flat-bq": flatConfig(func(cfg *entflat.UserConfig) { cfg.BQ.Enabled = true }),
		"flat-rq": flatConfig(func(cfg *entflat.UserConfig) { cfg.RQ.Enabled = true }),
	}
	// a multivector index rejects the single vectors this test writes, so it only
	// takes part in the case that writes none
	withMultiVector := maps.Clone(namedVectors)
	withMultiVector["hnsw-multivector"] = hnswConfig(func(cfg *enthnsw.UserConfig) {
		cfg.Multivector.Enabled = true
	})

	tests := []struct {
		name         string
		namedVectors map[string]models.VectorConfig
		// vectorDimensions is the size of the vector stored for every object under
		// every named vector. 0 leaves every configured vector without data.
		vectorDimensions  int
		dimensionTracking dimensionTracking
	}{
		{name: "configured vectors without data", namedVectors: withMultiVector},
		{name: "configured vectors with data", namedVectors: namedVectors, vectorDimensions: 32},
		{
			// a node reporting usage without TRACK_VECTOR_DIMENSIONS has no dimensions
			// bucket for the loaded path to read, and the report must survive that
			name:              "vectors whose dimensions were never tracked",
			namedVectors:      namedVectors,
			vectorDimensions:  32,
			dimensionTracking: trackingOff,
		},
		{
			// dimensions tracked earlier stay on disk, where the unloaded path reads
			// them; the loaded path holds no bucket covering them
			name:              "dimensions tracked before tracking was turned off",
			namedVectors:      namedVectors,
			vectorDimensions:  32,
			dimensionTracking: trackingStoppedAfterImport,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index, vectorConfigs := setupPopulatedLazyIndex(ctx, t, usageIndexParams{
				namedVectors:      tt.namedVectors,
				vectorDimensions:  tt.vectorDimensions,
				dimensionTracking: tt.dimensionTracking,
			})
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			_, release, err := index.GetShard(ctx, tenantName)
			require.NoError(t, err)
			loaded, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			release()
			require.NoError(t, err)
			require.Len(t, loaded.Shards, 1)

			// the unloaded path opens the same buckets, so the loaded shard has to
			// let go of them first
			loadedShard, ok := index.shards.LoadAndDelete(tenantName)
			require.True(t, ok)
			require.NoError(t, loadedShard.Shutdown(ctx))

			unloaded, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			require.Len(t, unloaded.Shards, 1)

			require.Len(t, loaded.Shards[0].NamedVectors, len(vectorConfigs))
			assert.Equal(t, loaded.Shards[0].NamedVectors, unloaded.Shards[0].NamedVectors)
			assert.Equal(t, loaded.Shards[0].FullShardStorageBytes, unloaded.Shards[0].FullShardStorageBytes)

			// the first cold report leaves its saved usage in the shard directory, so
			// a second one walks a directory the loaded shard never has
			writeSavedShardUsage(t, index.path(), tenantName, &types.UsageDisk{
				Version:    types.UsageDiskVersion,
				ShardUsage: unloaded.Shards[0],
			})
			recomputed, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err)
			require.Len(t, recomputed.Shards, 1)
			assert.Equal(t, loaded.Shards[0].FullShardStorageBytes, recomputed.Shards[0].FullShardStorageBytes)
		})
	}
}
