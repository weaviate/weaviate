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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestIndex_UsageForCollection_DroppedNamedVector is a regression test for a
// dropped named vector aborting the entire node's usage report:
//
//	collection X: inactive shard <id>:
//	vector index config for "to-be-dropped" is not of expected type
//
// Dropping a named vector keeps its schema entry with VectorIndexType "none"
// and drops its VectorIndexConfig, and nothing ever repopulates that config —
// the schema parser skips dropped entries. The cold path asserted every entry
// to a concrete config type, so the nil left behind failed the assertion, and
// the error is not fs.ErrNotExist so usageForShard's recovery arm did not catch
// it. It propagated all the way out of service.Usage, costing the node every
// collection's usage, not just this one.
//
// The loaded path never saw this: dropVectorIndex removes the entry from
// vectorIndexUserConfigs, so ForEachVectorIndex has no such vector to
// enumerate. The cold path must skip it the same way.
func TestIndex_UsageForCollection_DroppedNamedVector(t *testing.T) {
	liveConfig := models.VectorConfig{
		VectorIndexType:   "hnsw",
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
	}
	droppedConfig := models.VectorConfig{
		VectorIndexType: modelsext.VectorIndexTypeNone,
	}

	tests := []struct {
		name         string
		vectorConfig map[string]models.VectorConfig
		wantVectors  []string
	}{
		{
			name:         "no dropped vectors",
			vectorConfig: map[string]models.VectorConfig{"one": liveConfig},
			wantVectors:  []string{"one"},
		},
		{
			name:         "dropped vector alongside a live one",
			vectorConfig: map[string]models.VectorConfig{"one": liveConfig, "to-be-dropped": droppedConfig},
			wantVectors:  []string{"one"},
		},
		{
			name:         "every vector dropped",
			vectorConfig: map[string]models.VectorConfig{"one": droppedConfig, "to-be-dropped": droppedConfig},
			wantVectors:  nil,
		},
	}

	// shardStates covers the two journeys onto the cold path: a tenant that went
	// INACTIVE, and one still registered but never lazy-loaded (e.g. after a restart).
	shardStates := []struct {
		name string
		// loadAndDelete drops the shard from the in-memory map so it is processed
		// as INACTIVE; otherwise it stays a registered-but-unloaded lazy shard.
		loadAndDelete bool
	}{
		{name: "unloaded lazy shard", loadAndDelete: false},
		{name: "inactive shard", loadAndDelete: true},
	}

	for _, tt := range tests {
		for _, state := range shardStates {
			t.Run(tt.name+", "+state.name, func(t *testing.T) {
				ctx := context.Background()
				tenantName := "test-tenant"

				index, _ := setupPopulatedLazyIndex(ctx, t, usageIndexParams{namedVectors: tt.vectorConfig})
				t.Cleanup(func() { _ = index.Shutdown(ctx) })

				if state.loadAndDelete {
					index.shards.LoadAndDelete(tenantName)
				}

				usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, tt.vectorConfig)
				require.NoError(t, err, "a dropped named vector must not fail the report")
				require.NotNil(t, usage)
				require.Len(t, usage.Shards, 1)

				var gotVectors []string
				for _, namedVector := range usage.Shards[0].NamedVectors {
					gotVectors = append(gotVectors, namedVector.Name)
				}
				assert.Equal(t, tt.wantVectors, gotVectors)
			})
		}
	}
}

// TestIndex_UsageForCollection_VectorIndexWithoutConfig is the loaded-shard half:
// an index the schema no longer configures is left out of the report instead of
// failing it. A drop that failed halfway leaves this shape behind for good.
func TestIndex_UsageForCollection_VectorIndexWithoutConfig(t *testing.T) {
	liveConfig := models.VectorConfig{
		VectorIndexType:   "hnsw",
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
	}

	tests := []struct {
		name string
		// dropped is the named vector whose config is removed while the shard keeps
		// its index.
		dropped     string
		wantVectors []string
	}{
		{name: "one of two vectors dropped", dropped: "two", wantVectors: []string{"", "one"}},
		{name: "the legacy vector dropped", dropped: "", wantVectors: []string{"one", "two"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index, vectorConfigs := setupPopulatedLazyIndex(ctx, t, usageIndexParams{
				namedVectors: map[string]models.VectorConfig{"one": liveConfig, "two": liveConfig},
			})
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			_, release, err := index.GetShard(ctx, tenantName)
			require.NoError(t, err)
			defer release()

			index.vectorIndexUserConfigLock.Lock()
			if tt.dropped == "" {
				index.vectorIndexUserConfig = nil
			} else {
				delete(index.vectorIndexUserConfigs, tt.dropped)
			}
			index.vectorIndexUserConfigLock.Unlock()
			delete(vectorConfigs, tt.dropped)

			usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, vectorConfigs)
			require.NoError(t, err, "an index without a config must not fail the report")
			require.Len(t, usage.Shards, 1)

			var gotVectors []string
			for _, namedVector := range usage.Shards[0].NamedVectors {
				gotVectors = append(gotVectors, namedVector.Name)
			}
			assert.Equal(t, tt.wantVectors, gotVectors)
		})
	}
}
