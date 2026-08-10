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
		// wantHotTracker proves the walk stopped: a reached shard's tracker dir
		// is removed.
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
		// Fail closed: not reachable in production (the sweep refuses this
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
			className := "ColdSweepEquiv_" + uuid.NewString()[:8]
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
			class := newTestClassWithProps("ColdSweepMultiProp_"+uuid.NewString()[:8],
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
	class := newTestClassWithProps("ColdSweepLoaded_"+uuid.NewString()[:8], []string{propName})
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

	// An empty filtered listing must not pin a backing array sized for the
	// shard's whole bucket count for the rest of the run.
	t.Run("a cached listing does not retain the whole directory", func(t *testing.T) {
		root := t.TempDir()
		for i := range 100 {
			require.NoError(t, os.Mkdir(filepath.Join(root, fmt.Sprintf("property_%d", i)), 0o755))
		}
		cache := &dirNamesCache{}
		_, err := cache.listSidecarCandidates(root)
		require.NoError(t, err)
		cached := cache.listings[dirNamesKey{path: root, filter: "sidecar"}]
		require.Zero(t, cap(cached.names))
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
		tenant   = "cold-tenant"
	)
	ctx := testCtx()
	class := newTestClassWithProps("ColdSweepUnknownType_"+uuid.NewString()[:8], []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, cold)

	err := idx.CleanStalePartialReindexState(ctx, propName, "an-index-type-this-build-does-not-know")

	require.ErrorIs(t, err, ErrCleanupSweepTruncated)
	require.False(t, cold.isLoaded(),
		"refusing the input must not cost a hydration of the whole collection")
	outcome, _ := classifyTerminalSweep(err)
	require.Equal(t, terminalSweepUnknown, outcome,
		"an input the node cannot process is not a swept collection")
}

// Every migration strategy's sidecar suffix must be recognized by
// [isSidecarDirOf]; extend [sidecarRoleWords] if a new one is added.
func TestEverySidecarSuffixIsASidecar(t *testing.T) {
	const main = "property_category"

	// Generation 0 is the canonical post-finalize bucket, which carries no
	// sidecar suffix at all; live migrations start at 1 (see genSuffix).
	for _, gen := range []int{1, 7} {
		strategies := []MigrationStrategy{
			&MapToBlockmaxStrategy{generation: gen},
			&RoaringSetRefreshStrategy{generation: gen},
			&FilterableToRangeableStrategy{generation: gen},
			&SearchableRetokenizeStrategy{generation: gen},
			&FilterableRetokenizeStrategy{generation: gen},
			&EnableFilterableStrategy{generation: gen},
			&EnableSearchableStrategy{generation: gen},
			&RebuildSearchableStrategy{generation: gen},
		}
		for _, strategy := range strategies {
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
		{name: "a sidecar whose suffix carries a strategy word", dir: main + "__blockmax_ingest", want: true},
		// Known collision (weaviate/weaviate#12574): a property named
		// "category__ingest" is indistinguishable from a sidecar of
		// "category", so the sweep would remove its live bucket.
		{name: "a property whose name ends in a role word", dir: main + "__ingest", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSidecarDirOf(tc.dir, main))
		})
	}
}
