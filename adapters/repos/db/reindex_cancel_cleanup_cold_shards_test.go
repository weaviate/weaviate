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
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Every caller of this sweep blocks something for its whole duration: the
// auto-cleanup after a terminal task registers the task's shards as
// cleanup-in-progress, which the backup gate refuses backups on, and the REST
// submit and cancel handlers hold the (collection, property) submit mutex.
// Loading every cold tenant of a multi-tenant collection to look for state that
// is not there stretches that hold across thousands of hydrations, and an
// expired context did not stop the walk — it only turned the remaining shards
// into failed loads that were still attempted.
func TestIndexCleanStalePartialReindexStateLeavesColdShardsAlone(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
		coldShard = "cold-tenant"
	)

	tests := []struct {
		name string
		// staleOnColdShard puts a cancelled run's leftovers on the cold
		// shard's disk, which is the only reason to pay for loading it.
		staleOnColdShard bool
		cancelBeforeWalk bool
		wantColdLoaded   bool
		wantColdTracker  bool
		// wantHotTracker is what proves the walk stopped: the loaded shard's
		// tracker dir is removed by a sweep that reaches it, and steps 2 and 3
		// of the per-shard sweep never consult the context themselves.
		wantHotTracker bool
		wantErr        bool
	}{
		{
			name: "a cold shard with nothing to clean is not loaded",
		},
		{
			name:             "a cold shard with stale state is loaded and cleaned",
			staleOnColdShard: true,
			wantColdLoaded:   true,
		},
		{
			name:             "a cancelled context stops the walk at the first shard",
			staleOnColdShard: true,
			cancelBeforeWalk: true,
			wantColdTracker:  true,
			wantHotTracker:   true,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupCtx := testCtx()
			className := "ColdSweep_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, setupCtx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			hot := shd.(*Shard)
			defer hot.Shutdown(context.Background())

			mkTrackerDir(t, hot.pathLSM(), tracker, "started.mig")

			coldLSM := shardPathLSM(idx.path(), coldShard)
			if tc.staleOnColdShard {
				mkTrackerDir(t, coldLSM, tracker, "started.mig")
			}
			cold := NewLazyLoadShard(setupCtx, nil, coldShard, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(coldShard, cold)
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			sweepCtx := context.Background()
			if tc.cancelBeforeWalk {
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				sweepCtx = cancelled
			}

			err := idx.CleanStalePartialReindexState(sweepCtx, propName, indexType)

			assert.Equalf(t, tc.wantColdLoaded, cold.isLoaded(),
				"cold shard loaded=%v, want %v: the sweep blocks its caller for its whole "+
					"duration, so it may only pay for a shard that has something to clean",
				cold.isLoaded(), tc.wantColdLoaded)
			assert.Equal(t, tc.wantColdTracker, dirExistsAt(t, coldLSM, ".migrations/"+tracker),
				"cold shard tracker dir")
			assert.Equal(t, tc.wantHotTracker, dirExistsAt(t, hot.pathLSM(), ".migrations/"+tracker),
				"loaded shard tracker dir")

			if tc.wantErr {
				assert.ErrorIs(t, err, context.Canceled,
					"abandoning the walk must be reported as the cancellation it is")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// lsmDirNames lists the directories the sweep can remove: the sidecar dirs at
// the LSM root and the migration tracker dirs under .migrations.
func lsmDirNames(t *testing.T, lsmPath string) []string {
	t.Helper()
	var out []string
	collect := func(dir, prefix string) {
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			return
		}
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.IsDir() {
				out = append(out, prefix+entry.Name())
			}
		}
	}
	collect(lsmPath, "")
	collect(filepath.Join(lsmPath, ".migrations"), ".migrations/")
	sort.Strings(out)
	return out
}

// hasStalePartialReindexState is a second implementation of the rules
// Shard.CleanStalePartialReindexState removes by, living in another file: it
// re-derives the preserve sets rather than sharing them. A shard it answers
// false for is never loaded, so anything the hydrated sweep would still have
// removed there stays on disk until someone else finds it. This runs both over
// the same fixtures and pins them together.
func TestHasStalePartialReindexStateMatchesTheHydratedSweep(t *testing.T) {
	const propName = "category"

	// A completed-but-deferred migration: the tracker carries tidied.mig and
	// its ingest sidecar is the live bucket, which the sweep preserves.
	deferredFinalize := map[string][]string{
		"enable_filterable_category_1": {"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
	}

	tests := []struct {
		name      string
		indexType string
		// trackers are .migrations dirs, mapped to the sentinels inside them.
		trackers map[string][]string
		// sidecars are dirs at the LSM root.
		sidecars  []string
		wantStale bool
	}{
		{
			name:      "a shard with no reindex state at all",
			indexType: "filterable",
		},
		{
			name:      "the main bucket dir is not a sidecar of itself",
			indexType: "filterable",
			sidecars:  []string{"property_category"},
		},
		{
			name:      "a sidecar a cancelled run left behind",
			indexType: "filterable",
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
			wantStale: true,
		},
		{
			name:      "a tracker a cancelled run left behind",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_category_1": {"started.mig"}},
			wantStale: true,
		},
		{
			name:      "deferred-finalize state the sweep preserves",
			indexType: "filterable",
			trackers:  deferredFinalize,
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
		},
		{
			name:      "deferred-finalize state plus one stale sidecar",
			indexType: "filterable",
			trackers:  deferredFinalize,
			sidecars: []string{
				"property_category__enable_filterable_ingest_1",
				"property_category__enable_filterable_ingest_2",
			},
			wantStale: true,
		},
		{
			name:      "another property's stale state is not this property's",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_other_1": {"started.mig"}},
			sidecars:  []string{"property_other__enable_filterable_ingest_1"},
		},
		{
			name:      "an index type this build cannot map to a bucket",
			indexType: "an-index-type-this-build-does-not-know",
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
			wantStale: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "ColdSweepEquiv_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			for name, sentinels := range tc.trackers {
				mkTrackerDir(t, lsm, name, sentinels...)
			}
			for _, name := range tc.sidecars {
				mkSidecarDir(t, lsm, name)
			}

			require.Equal(t, tc.wantStale, hasStalePartialReindexState(lsm, propName, tc.indexType))
			if tc.wantStale {
				// The shard is hydrated, and whatever the sweep then makes of
				// it is the sweep's own business — the other tests here cover
				// what it removes.
				return
			}

			before := lsmDirNames(t, lsm)
			require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, tc.indexType))
			require.Equal(t, before, lsmDirNames(t, lsm),
				"the predicate said this shard has nothing to clean, so the sweep it gates "+
					"must not find anything either — a shard it skips is never looked at again")
		})
	}
}
