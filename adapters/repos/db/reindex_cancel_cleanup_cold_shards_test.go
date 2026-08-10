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

// See [Index.CleanStalePartialReindexState] for why hydrating every cold
// tenant to check it is too expensive to do unconditionally.
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

// hasStalePartialReindexState re-derives, independently, the same rules
// Shard.CleanStalePartialReindexState removes by. This pins the two against
// the same fixtures so they can't drift apart.
func TestHasStalePartialReindexStateMatchesTheHydratedSweep(t *testing.T) {
	// A completed-but-deferred migration: the tracker carries tidied.mig and
	// its ingest sidecar is the live bucket, which the sweep preserves.
	deferredFinalize := map[string][]string{
		"enable_filterable_category_1": {"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
	}
	completed := []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}

	tests := []struct {
		name      string
		propName  string
		indexType string
		// trackers are .migrations dirs, mapped to the sentinels inside them.
		trackers map[string][]string
		// sidecars are dirs at the LSM root.
		sidecars []string
		// unreadable is a dir the gate is denied access to, relative to the
		// shard's LSM path ("." is the LSM path itself). Empty denies nothing.
		unreadable string
		wantStale  bool
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
		// Property names may contain underscores, so "category"'s tracker
		// prefix is a prefix of "category_x"'s tracker dir. The gate would
		// hydrate every tenant of the collection for state that is not this
		// property's, and the sweep it gates would then delete that property's
		// live deferred-finalize tracker.
		{
			name:      "a property whose name extends this one, awaiting finalize",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_category_x_1": completed},
			sidecars:  []string{"property_category_x__enable_filterable_ingest_1"},
		},
		{
			name:      "a property whose name extends this one, left mid-run",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_category_x_1": {"started.mig"}},
			sidecars:  []string{"property_category_x__enable_filterable_ingest_1"},
		},
		// A class-level migration in deferred-finalize state leaves a live
		// sidecar on EVERY tenant of the collection. A gate that does not
		// preserve those hydrates the whole collection, which is the one thing
		// it exists to stop.
		{
			name:      "filterable: a class-level roaringset refresh awaiting finalize",
			indexType: "filterable",
			trackers:  map[string][]string{"filterable_roaringset_refresh_2": completed},
			sidecars:  []string{"property_category__roaringset_ingest_2"},
		},
		{
			name:      "searchable: a class-level map_to_blockmax awaiting finalize",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"searchable_map_to_blockmax_2": completed},
			sidecars:  []string{"property_descr_searchable__blockmax_ingest_2"},
		},
		{
			name:      "filterable: a cancelled class-level attempt is still stale",
			indexType: "filterable",
			trackers:  map[string][]string{"filterable_roaringset_refresh_3": {"started.mig"}},
			sidecars:  []string{"property_category__roaringset_ingest_3"},
			wantStale: true,
		},
		// rangeable has no class-level strategy, so the preserve set is the
		// per-property one on its own.
		{
			name:      "rangeable: a per-property migration awaiting finalize",
			indexType: "rangeable",
			trackers:  map[string][]string{"filterable_to_rangeable_category_1": completed},
			sidecars:  []string{"property_category_rangeable__rangeable_ingest_1"},
		},
		{
			name:      "rangeable: a sidecar a cancelled run left behind",
			indexType: "rangeable",
			sidecars:  []string{"property_category_rangeable__rangeable_ingest_1"},
			wantStale: true,
		},
		{
			name:      "an index type this build cannot map to a bucket",
			indexType: "an-index-type-this-build-does-not-know",
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
			wantStale: true,
		},
		// A question the gate could not ask is not an answer of "nothing to
		// clean": that would leave a stale started.mig for the next task to
		// resume against, the short-circuit this whole sweep guards against.
		{
			name:       "an LSM dir the gate cannot enumerate",
			indexType:  "filterable",
			unreadable: ".",
			wantStale:  true,
		},
		{
			name:       "a .migrations dir the gate cannot enumerate",
			indexType:  "filterable",
			unreadable: ".migrations",
			trackers:   map[string][]string{"enable_filterable_category_1": completed},
			wantStale:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.unreadable != "" && os.Geteuid() == 0 {
				t.Skip("root reads a directory whatever its mode says")
			}
			propName := tc.propName
			if propName == "" {
				propName = "category"
			}
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
			if tc.unreadable != "" {
				denied := filepath.Join(lsm, tc.unreadable)
				// Restored before the shard shuts down, which needs the dir
				// back: defers run in reverse order of registration.
				defer func() { require.NoError(t, os.Chmod(denied, 0o755)) }()
				require.NoError(t, os.Chmod(denied, 0o000))
			}

			require.Equal(t, tc.wantStale, hasStalePartialReindexState(lsm, propName, tc.indexType, nil))
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

// A sweep of one property must leave a property whose name extends it alone.
// The tidied.mig it would delete is what promotes that property's ingest dir to
// canonical on the next restart, and losing it empties the canonical bucket.
func TestShardCleanStalePartialReindexStateLeavesALongerPropertyNameAlone(t *testing.T) {
	const (
		mine   = "enable_filterable_category_1"
		theirs = "enable_filterable_category_x_1"
		// Their sidecar is out of reach of this sweep's bucket prefix
		// already ("property_category__" is not a prefix of it), and stays
		// here so the whole of their state is in the fixture.
		theirSidecar = "property_category_x__enable_filterable_ingest_1"
	)
	ctx := testCtx()
	class := newTestClassWithProps("ColdSweepPrefix_"+uuid.NewString()[:8], []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())
	lsm := shard.pathLSM()

	mkTrackerDir(t, lsm, mine, "started.mig")
	mkTrackerDir(t, lsm, theirs, "started.mig", "merged.mig", "swapped.mig", "tidied.mig")
	mkSidecarDir(t, lsm, theirSidecar)

	require.NoError(t, shard.CleanStalePartialReindexState(ctx, "category", "filterable"))

	assert.False(t, dirExistsAt(t, lsm, ".migrations/"+mine),
		"this property's cancelled run is what the sweep is for")
	assert.True(t, dirExistsAt(t, lsm, ".migrations/"+theirs),
		"another property's completed migration is live state, not this sweep's to remove")
	assert.True(t, dirExistsAt(t, lsm, theirSidecar),
		"the bucket that tracker still points at")
}

// The sweeps after a terminal task ask the same unhydrated shards the same
// question about a different tuple, so they share one listing per directory.
func TestDirNamesCache(t *testing.T) {
	newDir := func(t *testing.T, entries ...string) string {
		t.Helper()
		root := t.TempDir()
		for _, name := range entries {
			require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o755))
		}
		require.NoError(t, os.WriteFile(filepath.Join(root, "a-file"), nil, 0o644))
		return root
	}

	t.Run("files are not listed", func(t *testing.T) {
		root := newDir(t, "bucket-a", "bucket-b")
		names, err := (&dirNamesCache{}).list(root)
		require.NoError(t, err)
		require.Equal(t, []string{"bucket-a", "bucket-b"}, names)
	})

	t.Run("a missing dir keeps its error", func(t *testing.T) {
		cache := &dirNamesCache{}
		missing := filepath.Join(t.TempDir(), "never-written-to")
		for range 2 {
			_, err := cache.list(missing)
			require.True(t, os.IsNotExist(err),
				"a shard nothing has written to yet is not a shard the gate cannot read")
		}
	})

	t.Run("a second look does not touch the filesystem", func(t *testing.T) {
		root := newDir(t, "bucket-a")
		cache := &dirNamesCache{}
		_, err := cache.list(root)
		require.NoError(t, err)
		require.NoError(t, os.RemoveAll(filepath.Join(root, "bucket-a")))

		names, err := cache.list(root)
		require.NoError(t, err)
		require.Equal(t, []string{"bucket-a"}, names)
	})

	t.Run("nil caches nothing", func(t *testing.T) {
		root := newDir(t, "bucket-a")
		var cache *dirNamesCache
		_, err := cache.list(root)
		require.NoError(t, err)
		require.NoError(t, os.RemoveAll(filepath.Join(root, "bucket-a")))

		names, err := cache.list(root)
		require.NoError(t, err)
		require.Empty(t, names)
	})

	t.Run("a full cache stops holding listings", func(t *testing.T) {
		root := newDir(t, "bucket-a")
		cache := &dirNamesCache{names: maxCachedDirNames}
		_, err := cache.list(root)
		require.NoError(t, err)
		require.Empty(t, cache.listings,
			"a node runs tens of thousands of tenants, and the gate exists to not "+
				"spend that memory")
	})
}

// Pins a known collision in the on-disk bucket names, so it is executable
// rather than folklore: a property whose name ends in an index-type suffix
// derives the same bucket name as another property's bucket of that index type,
// and both the cold-shard gate and the sweep match on it. Closing it means
// renaming buckets on disk, which is why this test asserts the collision
// instead of its absence — invert it when the naming is fixed.
func TestMainBucketForPropertyIndexHasAKnownNameCollision(t *testing.T) {
	searchableOfCat, ok := mainBucketForPropertyIndex("cat", "searchable")
	require.True(t, ok)
	filterableOfCatSearchable, ok := mainBucketForPropertyIndex("cat_searchable", "filterable")
	require.True(t, ok)

	require.Equal(t, searchableOfCat, filterableOfCatSearchable,
		"two properties share one bucket name; a sweep of either reaches the other's sidecars")
	require.True(t,
		isSidecarDirOf(searchableOfCat+"__enable_filterable_ingest_1", filterableOfCatSearchable),
		"and the sidecar rule cannot tell them apart either")
}
