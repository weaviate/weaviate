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
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// See [Index.cleanStalePartialReindexState] for why hydrating every unloaded
// tenant to check it is too expensive to do unconditionally.
func TestIndexCleanStalePartialReindexStateLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		propName      = "category"
		indexType     = "filterable"
		tracker       = "enable_filterable_category_1"
		unloadedShard = "unloaded-tenant"
	)

	tests := []struct {
		name string
		// staleOnUnloadedShard puts a cancelled run's leftovers on the unloaded
		// shard's disk, which is the only reason to pay for loading it.
		staleOnUnloadedShard bool
		cancelBeforeWalk     bool
		wantUnloadedLoaded   bool
		wantUnloadedTracker  bool
		// wantHotTracker proves the walk stopped: a reached shard's tracker dir
		// is removed.
		wantHotTracker bool
		wantErr        bool
	}{
		{
			name: "an unloaded shard with nothing to clean is not loaded",
		},
		{
			name:                 "an unloaded shard with stale state is loaded and cleaned",
			staleOnUnloadedShard: true,
			wantUnloadedLoaded:   true,
		},
		{
			name:                 "a cancelled context stops the walk at the first shard",
			staleOnUnloadedShard: true,
			cancelBeforeWalk:     true,
			wantUnloadedTracker:  true,
			wantHotTracker:       true,
			wantErr:              true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupCtx := testCtx()
			className := "UnloadedSweep_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, setupCtx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			hot := shd.(*Shard)
			defer hot.Shutdown(context.Background())

			mkTrackerDir(t, hot.pathLSM(), tracker, "started.mig")

			unloadedLSM := shardPathLSM(idx.path(), unloadedShard)
			if tc.staleOnUnloadedShard {
				mkTrackerDir(t, unloadedLSM, tracker, "started.mig")
			}
			unloaded := NewLazyLoadShard(setupCtx, nil, unloadedShard, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(unloadedShard, unloaded)
			defer func() {
				if unloaded.isLoaded() {
					require.NoError(t, unloaded.Shutdown(context.Background()))
				}
			}()

			sweepCtx := context.Background()
			if tc.cancelBeforeWalk {
				cancelled, cancel := context.WithCancel(context.Background())
				cancel()
				sweepCtx = cancelled
			}

			err := idx.cleanStalePartialReindexState(sweepCtx, propName, indexType, nil)

			assert.Equalf(t, tc.wantUnloadedLoaded, unloaded.isLoaded(),
				"unloaded shard loaded=%v, want %v: the sweep blocks its caller for its whole "+
					"duration, so it may only pay for a shard that has something to clean",
				unloaded.isLoaded(), tc.wantUnloadedLoaded)
			assert.Equal(t, tc.wantUnloadedTracker, dirExistsAt(t, unloadedLSM, ".migrations/"+tracker),
				"unloaded shard tracker dir")
			assert.Equal(t, tc.wantHotTracker, dirExistsAt(t, hot.pathLSM(), ".migrations/"+tracker),
				"loaded shard tracker dir")

			if tc.wantErr {
				assert.ErrorIs(t, err, context.Canceled,
					"abandoning the walk must be reported as the cancellation it is")
				assert.ErrorIs(t, err, ErrCleanupSweepTruncated,
					"and tagged truncated, which is what decides the caller's severity")
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
		// payloads is the property list a tracker's task recorded, distinguishing
		// a two-property task from a property whose name contains the join char.
		payloads map[string][]string
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
			name:      "the main bucket dir on its own is not stale state",
			indexType: "filterable",
			sidecars:  []string{"property_category"},
		},
		{
			name:      "a sidecar a cancelled run left behind",
			indexType: "filterable",
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
			wantStale: true,
		},
		// "__" is allowed in a property name, so this is property
		// "category__extra"'s own main bucket, not a sidecar of "category".
		{
			name:      "another property whose name carries the sidecar separator",
			indexType: "filterable",
			sidecars:  []string{"property_category__extra"},
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
		// Pins that "category"'s prefix matching "category_x"'s tracker does
		// not falsely hydrate/delete the latter's state.
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
		// A class-level migration awaiting finalize leaves a live sidecar on
		// every tenant of the collection.
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
		// Fails open: not reachable in production (the sweep refuses this
		// input earlier), but "nothing here" would read as a clean sweep.
		{
			name:      "an index type this build cannot map to a bucket",
			indexType: "an-index-type-this-build-does-not-know",
			sidecars:  []string{"property_category__enable_filterable_ingest_1"},
			wantStale: true,
		},
		// A two-property task writes one tracker for both properties.
		{
			name:      "a two-property task this property is part of",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_category_other_1": {"started.mig"}},
			payloads: map[string][]string{
				"enable_filterable_category_other_1": {"category", "other"},
			},
			wantStale: true,
		},
		{
			name:      "a two-property task this property is not part of",
			indexType: "filterable",
			trackers:  map[string][]string{"enable_filterable_other_third_1": {"started.mig"}},
			payloads: map[string][]string{
				"enable_filterable_other_third_1": {"other", "third"},
			},
		},
		// An unreadable dir fails open, not "nothing to clean".
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
			className := "UnloadedSweepEquiv_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			for name, sentinels := range tc.trackers {
				mkTrackerDir(t, lsm, name, sentinels...)
				if props, ok := tc.payloads[name]; ok {
					mkRecoveryPayload(t, lsm, name, props...)
				}
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

// Pins that a sweep of "category" leaves "category_x"'s completed migration
// tracker alone.
func TestShardCleanStalePartialReindexStateLeavesALongerPropertyNameAlone(t *testing.T) {
	const (
		mine   = "enable_filterable_category_1"
		theirs = "enable_filterable_category_x_1"
		// Out of reach of this sweep's bucket prefix already; included so the
		// whole of their state is in the fixture.
		theirSidecar = "property_category_x__enable_filterable_ingest_1"
	)
	ctx := testCtx()
	class := newTestClassWithProps("UnloadedSweepPrefix_"+uuid.NewString()[:8], []string{"category"})
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

// A cancelled two-property task leaves one tracker dir for both properties,
// and cleanup runs once per property.
func TestShardCleanStalePartialReindexStateSweepsAMultiPropertyTracker(t *testing.T) {
	const tracker = "enable_filterable_a_b_1"

	tests := []struct {
		name     string
		propName string
		want     bool
	}{
		{name: "swept by its first property", propName: "a"},
		{name: "swept by its second property", propName: "b"},
		{name: "left alone by a property it does not name", propName: "c", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("UnloadedSweepMultiProp_"+uuid.NewString()[:8],
				[]string{"a", "b", "c"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			mkTrackerDir(t, lsm, tracker, "started.mig")
			mkRecoveryPayload(t, lsm, tracker, "a", "b")

			require.Equal(t, !tc.want,
				hasStalePartialReindexState(lsm, tc.propName, "filterable", nil),
				"the gate has to load the shard for exactly the sweeps that would clean it")
			require.NoError(t, shard.CleanStalePartialReindexState(ctx, tc.propName, "filterable"))

			require.Equal(t, tc.want, dirExistsAt(t, lsm, ".migrations/"+tracker))
		})
	}
}

// A completed two-property migration owns a live ingest sidecar per property.
// The sweep of one of those properties must leave both the tracker and that
// sidecar alone, whether or not the tracker still carries its payload — a
// tracker written before payload.mig existed points at live data just the same,
// and sidecar deletion never consulted the payload.
func TestShardCleanStalePartialReindexStatePreservesACompletedMultiPropertyTracker(t *testing.T) {
	const sidecar = "property_a__enable_filterable_ingest_1"
	completed := []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}

	tests := []struct {
		name    string
		tracker string
		// payload is what the task recorded; empty writes no payload.mig.
		payload      []string
		wantTracker  bool
		wantSidecar  bool
		wantGateHold bool
	}{
		{
			name:    "the completed tracker names this property",
			tracker: "enable_filterable_a_b_1", payload: []string{"a", "b"},
			wantTracker: true, wantSidecar: true,
		},
		{
			name:        "the completed tracker names this property, payload gone",
			tracker:     "enable_filterable_a_b_1",
			wantTracker: true, wantSidecar: true,
		},
		// The widened preserve match stays inside this property: a completed
		// tracker of another property must not shield this one's stale sidecar.
		{
			name:        "a completed tracker of another property, payload gone",
			tracker:     "enable_filterable_other_1",
			wantTracker: true, wantGateHold: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("UnloadedSweepMultiPropDone_"+uuid.NewString()[:8],
				[]string{"a", "b", "other"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			mkTrackerDir(t, lsm, tc.tracker, completed...)
			if len(tc.payload) > 0 {
				mkRecoveryPayload(t, lsm, tc.tracker, tc.payload...)
			}
			mkSidecarDir(t, lsm, sidecar)

			require.Equal(t, tc.wantGateHold,
				hasStalePartialReindexState(lsm, "a", "filterable", nil),
				"the gate has to load the shard for exactly the sweeps that would clean it")
			require.NoError(t, shard.CleanStalePartialReindexState(ctx, "a", "filterable"))

			require.Equal(t, tc.wantTracker, dirExistsAt(t, lsm, ".migrations/"+tc.tracker))
			require.Equal(t, tc.wantSidecar, dirExistsAt(t, lsm, sidecar),
				"the bucket the in-memory pointer is on")
		})
	}
}

// A loaded shard is swept unconditionally, without consulting the gate, even
// with a stale directory listing on hand.
func TestIndexCleanStalePartialReindexStateSweepsALoadedShardUnconditionally(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
		tenant    = "loaded-tenant"
	)
	ctx := testCtx()
	class := newTestClassWithProps("UnloadedSweepLoaded_"+uuid.NewString()[:8], []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	lazy := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, lazy)
	_, err := lazy.Unwrap(ctx)
	require.NoError(t, err)
	defer lazy.Shutdown(context.Background())
	lsm := shardPathLSM(idx.path(), tenant)

	// Read the shard's directories before its state exists, which is what a
	// sweep earlier in the same run would have cached.
	dirs := &dirNamesCache{}
	_, err = dirs.listSidecarCandidates(lsm)
	require.NoError(t, err)
	_, err = dirs.list(filepath.Join(lsm, ".migrations"))
	require.True(t, err == nil || os.IsNotExist(err))
	mkTrackerDir(t, lsm, tracker, "started.mig")
	require.False(t, hasStalePartialReindexState(lsm, propName, indexType, dirs),
		"the stale listing is the point: the gate cannot see what arrived after it")

	require.NoError(t, idx.cleanStalePartialReindexState(ctx, propName, indexType, dirs))

	require.False(t, dirExistsAt(t, lsm, ".migrations/"+tracker),
		"a loaded shard is swept whatever the gate would have said about it")
}

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
		cache := &dirNamesCache{cost: maxCachedDirNames}
		_, err := cache.list(root)
		require.NoError(t, err)
		require.Empty(t, cache.listings,
			"a node runs tens of thousands of tenants, and the gate exists to not "+
				"spend that memory")
	})

	// The workload the bound is for: tenants whose LSM dirs hold no sidecar at
	// all. Charging only the names kept would let every one of them in for free.
	t.Run("listings that keep no names are still bounded", func(t *testing.T) {
		first, second := newDir(t, "property_a"), newDir(t, "property_b")
		cache := &dirNamesCache{cost: maxCachedDirNames - 1}
		for _, root := range []string{first, second} {
			names, err := cache.listSidecarCandidates(root)
			require.NoError(t, err)
			require.Empty(t, names)
		}
		require.Len(t, cache.listings, 1,
			"an empty listing is still a map entry, and a node whose tenants are "+
				"untouched produces nothing but those")
	})

	// Pins the fix for a cached listing aliasing listDirNames's full-directory
	// backing array instead of owning its own copy.
	t.Run("a cached listing does not retain the whole directory", func(t *testing.T) {
		root := t.TempDir()
		for i := range 100 {
			require.NoError(t, os.Mkdir(filepath.Join(root, fmt.Sprintf("property_%d", i)), 0o755))
		}
		require.NoError(t, os.Mkdir(filepath.Join(root, "property_a__blockmax_ingest_1"), 0o755))

		cache := &dirNamesCache{}
		names, err := cache.listSidecarCandidates(root)
		require.NoError(t, err)
		require.Equal(t, []string{"property_a__blockmax_ingest_1"}, names)
		require.Greater(t, cap(names), len(names),
			"the returned slice is sized for the whole directory, which is what "+
				"the cache must not hold on to")

		cached := cache.listings[dirNamesKey{path: root, filter: "sidecar"}]
		require.Equal(t, len(cached.names), cap(cached.names))

		names[0] = "overwritten"
		require.Equal(t, []string{"property_a__blockmax_ingest_1"}, cached.names,
			"the cached listing shares a backing array with the full-directory slice")
	})

	// The full and the filtered listing are different answers about one path;
	// handing the filtered one back for the full question hides every bucket dir.
	t.Run("a filtered listing does not answer an unfiltered question", func(t *testing.T) {
		root := newDir(t, "property_a", "property_a__blockmax_ingest_1")
		cache := &dirNamesCache{}
		sidecars, err := cache.listSidecarCandidates(root)
		require.NoError(t, err)
		require.Equal(t, []string{"property_a__blockmax_ingest_1"}, sidecars)

		all, err := cache.list(root)
		require.NoError(t, err)
		require.Equal(t, []string{"property_a", "property_a__blockmax_ingest_1"}, all)
	})
}

// Pins the known bucket-name collision as passing; invert this assertion
// once the on-disk naming is fixed.
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

// An unmappable index type is refused before the walk starts, without
// hydrating any shard.
func TestIndexCleanStalePartialReindexStateRefusesAnUnknownIndexType(t *testing.T) {
	const (
		propName = "category"
		tenant   = "unloaded-tenant"
	)
	ctx := testCtx()
	class := newTestClassWithProps("UnloadedSweepUnknownType_"+uuid.NewString()[:8], []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	unloaded := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, unloaded)

	err := idx.cleanStalePartialReindexState(ctx, propName, "an-index-type-this-build-does-not-know", nil)

	require.ErrorIs(t, err, ErrCleanupSweepTruncated)
	require.False(t, unloaded.isLoaded(),
		"refusing the input must not cost a hydration of the whole collection")
	outcome, _ := classifyTerminalSweep(err)
	require.Equal(t, terminalSweepUnknown, outcome,
		"an input the node cannot process is not a swept collection")
}

// strategiesByMigrationDir builds one instance of every migration strategy,
// keyed by the tracker dir prefix it declares.
func strategiesByMigrationDir(generation int) map[string]MigrationStrategy {
	return map[string]MigrationStrategy{
		MigrationDirSearchableMapToBlockmax:     &MapToBlockmaxStrategy{generation: generation},
		MigrationDirFilterableRoaringsetRefresh: &RoaringSetRefreshStrategy{generation: generation},
		MigrationDirPrefixFilterableToRangeable: &FilterableToRangeableStrategy{generation: generation},
		MigrationDirPrefixSearchableRetokenize:  &SearchableRetokenizeStrategy{generation: generation},
		MigrationDirPrefixFilterableRetokenize:  &FilterableRetokenizeStrategy{generation: generation},
		MigrationDirPrefixEnableFilterable:      &EnableFilterableStrategy{generation: generation},
		MigrationDirPrefixEnableSearchable:      &EnableSearchableStrategy{generation: generation},
		MigrationDirPrefixRebuildSearchable:     &RebuildSearchableStrategy{generation: generation},
	}
}

// sweptMigrationDirPrefixes is every tracker dir prefix the cleanup knows, taken
// from the production tables a new strategy has to extend to be swept at all.
func sweptMigrationDirPrefixes() []string {
	var prefixes []string
	for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
		prefixes = append(prefixes, migrationDirPrefixesForIndexType(indexType)...)
		if classDir, ok := classLevelMigrationDirForIndexType(indexType); ok {
			prefixes = append(prefixes, classDir)
		}
	}
	slices.Sort(prefixes)
	return slices.Compact(prefixes)
}

// Every migration strategy's sidecar suffix must be recognized by
// [isSidecarDirOf]; extend [sidecarRoleWords] if a new one is added.
func TestEverySidecarSuffixIsASidecar(t *testing.T) {
	const main = "property_category"

	require.ElementsMatch(t, sweptMigrationDirPrefixes(),
		slices.Collect(maps.Keys(strategiesByMigrationDir(1))),
		"a strategy the cleanup sweeps but this test does not instantiate would "+
			"never have its suffix checked against the role words")

	// Generation 0 is the canonical post-finalize bucket, which carries no
	// sidecar suffix at all; live migrations start at 1 (see genSuffix).
	for _, gen := range []int{1, 7} {
		strategies := strategiesByMigrationDir(gen)
		for prefix, strategy := range strategies {
			require.Truef(t, strings.HasPrefix(strategy.MigrationDirName(), prefix),
				"%T is filed under %q but names its tracker dir %q",
				strategy, prefix, strategy.MigrationDirName())
			for _, suffix := range []string{
				strategy.ReindexSuffix(), strategy.IngestSuffix(), strategy.BackupSuffix(),
			} {
				assert.Truef(t, isSidecarDirOf(main+suffix, main),
					"%T's %q is not recognized as a sidecar suffix", strategy, suffix)
			}
		}
	}
}

// The names that are NOT sidecars of the swept property, and that the sweep
// would RemoveAll if it read them as such.
func TestIsSidecarDirOfRejectsOtherPropertiesBuckets(t *testing.T) {
	const main = "property_category"

	tests := []struct {
		name string
		dir  string
		want bool
	}{
		{name: "the main bucket itself", dir: main},
		{name: "a property whose name carries the separator", dir: main + "__extra"},
		{name: "a generation-suffixed main bucket", dir: main + "__gen2"},
		{name: "a longer property's main bucket", dir: main + "_x"},
		{name: "a sidecar", dir: main + "__enable_filterable_ingest_1", want: true},
		{name: "a sidecar with no generation", dir: main + "__reindex", want: true},
		{name: "a sidecar a generation-0 bug would leave", dir: main + "__ingest_0", want: true},
		{name: "a property named after a number", dir: main + "__12", want: false},
		{name: "a blockmax backup sidecar", dir: main + "__blockmax_map_3", want: true},
		{name: "a property whose name extends a role word", dir: main + "__ingest_x", want: false},
		// An empty tail is not a generation, so this is property
		// "category__ingest_"'s own main bucket.
		{name: "a property whose name ends in a role word and a separator", dir: main + "__ingest_", want: false},
		{name: "a sidecar whose suffix carries a strategy word", dir: main + "__blockmax_ingest", want: true},
		// Known collision (weaviate/weaviate#12574): sweeping "category"
		// also removes "category__ingest"'s own bucket.
		{name: "a property whose name ends in a role word", dir: main + "__ingest", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSidecarDirOf(tc.dir, main))
		})
	}
}
