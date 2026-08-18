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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A record is neither of the two things the unloaded-shard gate hydrates a
// tenant for: it names nothing a sweep would remove or a load would reclaim.
// Otherwise every sweep would hydrate every tenant carrying one until the
// flag flips.
func TestHasStalePartialReindexStateSettlesOnARecord(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
	)

	tests := []struct {
		name            string
		trackers        map[string][]string
		sidecars        []string
		wantStale       bool
		wantFinalizable bool
	}{
		{
			name:     "a record on its own is settled",
			trackers: map[string][]string{"enable_filterable_category_1": recordedSentinels},
		},
		{
			// The promotion is still ahead of this tenant: only a load runs it,
			// and until it does the data sits under the ingest sidecar name.
			name:            "a completed migration with no record yet still needs a load",
			trackers:        map[string][]string{"enable_filterable_category_1": completedSentinels},
			sidecars:        []string{"property_category__enable_filterable_ingest_1"},
			wantFinalizable: true,
		},
		{
			name: "a record next to a newer generation that has not been promoted",
			trackers: map[string][]string{
				"enable_filterable_category_1": recordedSentinels,
				"enable_filterable_category_2": completedSentinels,
			},
			sidecars:        []string{"property_category__enable_filterable_ingest_2"},
			wantFinalizable: true,
		},
		{
			name: "a record next to a cancelled run's leftovers",
			trackers: map[string][]string{
				"enable_filterable_category_1": recordedSentinels,
				"enable_filterable_category_2": {"started.mig"},
			},
			wantStale: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SettledRecord_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			for name, sentinels := range tc.trackers {
				mkTrackerDir(t, lsm, name, sentinels...)
				mkRecoveryPayload(t, lsm, name, propName)
			}
			for _, name := range tc.sidecars {
				mkSidecarDir(t, lsm, name)
			}

			stale, finalizable := hasStalePartialReindexState(lsm, propName, indexType, nil, nil)
			assert.Equal(t, tc.wantStale, stale, "stale")
			assert.Equal(t, tc.wantFinalizable, finalizable, "finalizable")
		})
	}
}

// The gate's answer decides whether a cold tenant is hydrated, and a hydration
// storm is what the whole gate exists to avoid. Run over the index-level sweep
// rather than the gate alone, on a tenant whose only state is a record.
func TestSweepLeavesAColdTenantWithARecordAlone(t *testing.T) {
	const (
		propName   = "category"
		indexType  = "filterable"
		tracker    = "enable_filterable_category_1"
		coldTenant = "cold-tenant"
	)

	ctx := testCtx()
	className := "ColdRecordSweep_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	coldLSM := shardPathLSM(idx.path(), coldTenant)
	require.NoError(t, os.MkdirAll(coldLSM, 0o755))
	mkTrackerDir(t, coldLSM, tracker, recordedSentinels...)
	mkRecoveryPayload(t, coldLSM, tracker, propName)
	mkSidecarDir(t, coldLSM, helpers.BucketFromPropNameLSM(propName))

	cold := NewLazyLoadShard(ctx, nil, coldTenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(coldTenant, cold)
	defer func() {
		if cold.isLoaded() {
			require.NoError(t, cold.Shutdown(context.Background()))
		}
	}()

	// Submit, cancel and terminal cleanup all reach the shard through this one
	// sweep, so running it twice covers the cycle the record has to survive.
	for range 2 {
		require.NoError(t, idx.cleanStalePartialReindexState(context.Background(), propName, indexType, nil))
	}

	assert.False(t, cold.isLoaded(),
		"a record is settled state; hydrating every tenant carrying one is the storm the gate prevents")
	assert.True(t, dirExistsAt(t, coldLSM, ".migrations/"+tracker),
		"and the sweep must leave the record itself intact")
	assert.True(t, dirExistsAt(t, coldLSM, helpers.BucketFromPropNameLSM(propName)),
		"along with the bucket it names")
}
