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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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

// A completed migration leaves its data under the ingest sidecar name and a
// full copy of the bucket it replaced under the backup name, both waiting for
// the finalize a shard load runs. On an unloaded tenant nothing else ever runs
// it, so a gate that skips the tenant is a gate that never reclaims either
// copy.
func TestIndexCleanStalePartialReindexStateReclaimsDeferredFinalizeResidue(t *testing.T) {
	const (
		residueTenant = "residue-tenant"
		cleanTenant   = "clean-tenant"
		// promoted rides along inside the ingest dir, so the canonical dir it
		// becomes can be told apart from one the reopened bucket created.
		promoted = "promoted.marker"
	)
	completed := []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"}

	// mkBucketDir plants a bucket dir carrying one non-segment file, which the
	// store ignores and a rename carries along.
	mkBucketDir := func(t *testing.T, lsmPath, name, marker string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, name), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(lsmPath, name, marker), []byte("x"), 0o644))
	}

	tests := []struct {
		name      string
		propName  string
		indexType string
		tracker   string
		// props is the property list the migration recorded, which is what
		// finalize renames by.
		props     string
		ingestDir string
		backupDir string
		canonical string
	}{
		{
			name:      "a per-property filterable enable",
			propName:  "category",
			indexType: "filterable",
			tracker:   "enable_filterable_category_1",
			props:     "category",
			ingestDir: "property_category__enable_filterable_ingest_1",
			backupDir: "property_category__enable_filterable_backup_1",
			canonical: "property_category",
		},
		// A class-level tracker is out of the deletion scope altogether, so
		// the leftovers here are only visible through the preserved sidecar.
		{
			name:      "a class-level roaringset refresh",
			propName:  "category",
			indexType: "filterable",
			tracker:   "filterable_roaringset_refresh_2",
			props:     "category",
			ingestDir: "property_category__roaringset_ingest_2",
			backupDir: "property_category__roaringset_backup_2",
			canonical: "property_category",
		},
		{
			name:      "a per-property searchable retokenize",
			propName:  "descr",
			indexType: "searchable",
			tracker:   "searchable_retokenize_descr_1",
			props:     "descr",
			ingestDir: "property_descr_searchable__retokenize_ingest_1",
			backupDir: "property_descr_searchable__retokenize_backup_1",
			canonical: "property_descr_searchable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("ResidueReclaim_"+uuid.NewString()[:8],
				[]string{tc.propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer shd.Shutdown(context.Background())

			residueLSM := shardPathLSM(idx.path(), residueTenant)
			mkTrackerDir(t, residueLSM, tc.tracker, completed...)
			require.NoError(t, os.WriteFile(
				filepath.Join(residueLSM, ".migrations", tc.tracker, "properties.mig"),
				[]byte(tc.props), 0o644))
			mkBucketDir(t, residueLSM, tc.ingestDir, promoted)
			mkBucketDir(t, residueLSM, tc.backupDir, "superseded.marker")

			// The clean tenant carries the canonical bucket a migrated tenant
			// ends up with, so the gate has a real listing to answer from
			// rather than a missing directory.
			cleanLSM := shardPathLSM(idx.path(), cleanTenant)
			mkBucketDir(t, cleanLSM, tc.canonical, "untouched.marker")

			tenants := map[string]*LazyLoadShard{}
			for _, name := range []string{residueTenant, cleanTenant} {
				lazy := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
					idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter,
					idx.shardReindexer, false, idx.bitmapBufPool)
				idx.shards.Store(name, lazy)
				tenants[name] = lazy
			}
			defer func() {
				for _, lazy := range tenants {
					if lazy.isLoaded() {
						require.NoError(t, lazy.Shutdown(context.Background()))
					}
				}
			}()

			require.NoError(t, idx.cleanStalePartialReindexState(ctx, tc.propName, tc.indexType, nil))

			assert.FileExists(t, filepath.Join(residueLSM, tc.canonical, promoted),
				"the ingest dir is the migration's own data, so reclaiming it means "+
					"promoting it to the canonical name, never deleting it")
			assert.False(t, dirExistsAt(t, residueLSM, tc.ingestDir),
				"the data is under its canonical name now")
			assert.False(t, dirExistsAt(t, residueLSM, tc.backupDir),
				"the copy of the bucket the migration replaced is what costs the disk")
			assert.False(t, dirExistsAt(t, residueLSM, ".migrations/"+tc.tracker),
				"and the tracker goes with them, so the next sweep skips this tenant")

			assert.False(t, tenants[cleanTenant].isLoaded(),
				"a tenant with no migration leftovers is the population the gate is "+
					"for, and it must still be left alone")
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

// Pins the not-stale direction: whenever the gate says "nothing here", the
// hydrated sweep must find nothing either, so a shard the gate skips on a
// fresh read never hides removable state. The stale direction isn't
// compared — several stale rows are fail-open answers the hydrated sweep
// decides for itself, at the cost [hasStalePartialReindexState] names.
func TestHasStalePartialReindexStateNotStaleMeansTheSweepFindsNothing(t *testing.T) {
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
		// corruptPayload names a tracker whose payload.mig is written as
		// garbage bytes instead of a recovery record.
		corruptPayload string
		wantStale      bool
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
		// The three per-property searchable strategies, whose state a cancelled
		// reindex leaves on every tenant of the collection.
		{
			name:      "searchable: a sidecar a cancelled enable left behind",
			propName:  "descr",
			indexType: "searchable",
			sidecars:  []string{"property_descr_searchable__enable_searchable_ingest_1"},
			wantStale: true,
		},
		{
			name:      "searchable: a tracker a cancelled enable left behind",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"enable_searchable_descr_1": {"started.mig"}},
			wantStale: true,
		},
		{
			name:      "searchable: a per-property enable awaiting finalize",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"enable_searchable_descr_1": completed},
			sidecars:  []string{"property_descr_searchable__enable_searchable_ingest_1"},
		},
		// Every searchable strategy writes sidecars of one main bucket, so the
		// preserve set is keyed by (suffix, generation), not generation alone.
		{
			name:      "searchable: deferred-finalize enable state plus another strategy's sidecar",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"enable_searchable_descr_1": completed},
			sidecars: []string{
				"property_descr_searchable__enable_searchable_ingest_1",
				"property_descr_searchable__rebuild_searchable_ingest_1",
			},
			wantStale: true,
		},
		{
			name:      "searchable: a rebuild left mid-run",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"rebuild_searchable_descr_2": {"started.mig"}},
			wantStale: true,
		},
		{
			name:      "searchable: a per-property rebuild awaiting finalize",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"rebuild_searchable_descr_1": completed},
			sidecars:  []string{"property_descr_searchable__rebuild_searchable_ingest_1"},
		},
		{
			name:      "searchable: a retokenize left mid-run",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"searchable_retokenize_descr_1": {"started.mig"}},
			wantStale: true,
		},
		{
			name:      "searchable: a per-property retokenize awaiting finalize",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"searchable_retokenize_descr_1": completed},
			sidecars:  []string{"property_descr_searchable__retokenize_ingest_1"},
		},
		{
			name:      "searchable: another property's stale enable is not this property's",
			propName:  "descr",
			indexType: "searchable",
			trackers:  map[string][]string{"enable_searchable_other_1": {"started.mig"}},
			sidecars:  []string{"property_other_searchable__enable_searchable_ingest_1"},
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
		// A payload this sweep can't read could name this property; answering
		// from the name alone would report a shard this sweep owns as clean.
		{
			name:       "a tracker payload the gate cannot read",
			indexType:  "filterable",
			unreadable: ".migrations/enable_filterable_category_other_1",
			trackers:   map[string][]string{"enable_filterable_category_other_1": {"started.mig"}},
			payloads: map[string][]string{
				"enable_filterable_category_other_1": {"category", "other"},
			},
			wantStale: true,
		},
		{
			name:           "a tracker payload the gate cannot parse",
			indexType:      "filterable",
			trackers:       map[string][]string{"enable_filterable_category_other_1": {"started.mig"}},
			corruptPayload: "enable_filterable_category_other_1",
			wantStale:      true,
		},
		// Name and payload are the same sorted property list, and the name is
		// written first, so a name omitting this property is the older witness
		// that no payload can overrule.
		{
			name:           "a corrupt payload on a dir whose name omits this property",
			indexType:      "filterable",
			trackers:       map[string][]string{"enable_filterable_other_1": {"started.mig"}},
			corruptPayload: "enable_filterable_other_1",
			wantStale:      false,
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
			if tc.corruptPayload != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(lsm, ".migrations", tc.corruptPayload, reindexRecoveryPayloadFile),
					[]byte("not a recovery record"), 0o644))
			}
			if tc.unreadable != "" {
				denied := filepath.Join(lsm, tc.unreadable)
				// Restored before the shard shuts down, which needs the dir
				// back: defers run in reverse order of registration.
				defer func() { require.NoError(t, os.Chmod(denied, 0o755)) }()
				require.NoError(t, os.Chmod(denied, 0o000))
			}

			stale, _, _ := hasStalePartialReindexState(lsm, propName, tc.indexType, nil)
			require.Equal(t, tc.wantStale, stale)
			if tc.wantStale {
				// The shard is hydrated, and whatever the sweep then makes of
				// it is the sweep's own business — the other tests here cover
				// what it removes.
				return
			}

			before := lsmDirNames(t, lsm)
			cleanSweep(t, ctx, shard, propName, tc.indexType)
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

	cleanSweep(t, ctx, shard, "category", "filterable")

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
	const (
		tracker = "enable_filterable_a_b_1"
		sidecar = "property_a__enable_filterable_ingest_1"
	)

	tests := []struct {
		name     string
		propName string
		// noPayload leaves payload.mig unwritten: a dir from before the file
		// existed, or a crash between persistRecoveryRecord's MkdirAll and
		// its WriteFile.
		noPayload   bool
		wantTracker bool
		wantStale   bool
	}{
		{name: "swept by its first property", propName: "a", wantStale: true},
		{name: "swept by its second property", propName: "b", wantStale: true},
		{name: "left alone by a property it does not name", propName: "c", wantTracker: true},
		// With no payload the name alone can't prove the tracker is this
		// sweep's, so it survives while its sidecar — whose deletion is not
		// payload-gated — goes; the orphan audit reclaims the leftover dir.
		// See [migrationDirScope].
		{
			name:     "a payload-less tracker survives while its sidecar goes",
			propName: "a", noPayload: true, wantTracker: true, wantStale: true,
		},
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
			if !tc.noPayload {
				mkRecoveryPayload(t, lsm, tracker, "a", "b")
			}
			mkSidecarDir(t, lsm, sidecar)

			stale, _, _ := hasStalePartialReindexState(lsm, tc.propName, "filterable", nil)
			require.Equal(t, tc.wantStale, stale,
				"the gate has to load the shard for exactly the sweeps that would clean it")
			cleanSweep(t, ctx, shard, tc.propName, "filterable")

			require.Equal(t, tc.wantTracker, dirExistsAt(t, lsm, ".migrations/"+tracker))
			// "a"'s sidecar is only in reach of "a"'s own sweep.
			wantSidecar := tc.propName != "a"
			require.Equal(t, wantSidecar, dirExistsAt(t, lsm, sidecar))
		})
	}
}

// Pins #10675: sweeping one property of a completed multi-property migration
// must not remove the tracker or its live sidecar, payload or not.
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
		// Preserve guessing must not shield another property's stale sidecar.
		{
			name:        "a completed tracker of another property, payload gone",
			tracker:     "enable_filterable_other_1",
			wantTracker: true, wantGateHold: true,
		},
		// Ambiguous name ("a"+"x" vs "a_x") over-preserves; see [migrationDirScope].
		{
			name:        "a completed tracker of a property whose name extends this one, payload gone",
			tracker:     "enable_filterable_a_x_1",
			wantTracker: true, wantSidecar: true,
		},
		// This property as a middle "_"-token of the payload-less name: the
		// tracker's gens must still feed the preserve set, else the live
		// sidecar is swept (#10675 shape).
		{
			name:        "a completed tracker naming this property mid-list, payload gone",
			tracker:     "enable_filterable_x_a_y_1",
			wantTracker: true, wantSidecar: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("UnloadedSweepMultiPropDone_"+uuid.NewString()[:8],
				[]string{"a", "b", "other", "a_x"})
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

			gateHold, _, _ := hasStalePartialReindexState(lsm, "a", "filterable", nil)
			require.Equal(t, tc.wantGateHold, gateHold,
				"the gate has to load the shard for exactly the sweeps that would clean it")
			cleanSweep(t, ctx, shard, "a", "filterable")

			require.Equal(t, tc.wantTracker, dirExistsAt(t, lsm, ".migrations/"+tc.tracker))
			require.Equal(t, tc.wantSidecar, dirExistsAt(t, lsm, sidecar),
				"the bucket the in-memory pointer is on")
		})
	}
}

// A crash between [lsmkv.Store.ReplaceBuckets]' two renames leaves the
// displaced bucket at "<mainBucket>___del". Nothing at startup removes it, so
// the sweep that owns the bucket's other leftovers has to own this one, and the
// gate has to hydrate for it.
func TestCleanStalePartialReindexStateRemovesAReplacedBucketDir(t *testing.T) {
	const propName = "category"

	tests := []struct {
		name      string
		indexType string
		// completedTracker and its live sidecar give the sweep a non-empty
		// preserve set, which the leftover must not slip into.
		completedTracker string
		liveSidecar      string
	}{
		{name: "filterable", indexType: "filterable"},
		{name: "searchable", indexType: "searchable"},
		{name: "rangeable", indexType: "rangeable"},
		{
			name:             "next to a completed migration whose sidecars are preserved",
			indexType:        "filterable",
			completedTracker: "enable_filterable_category_1",
			liveSidecar:      "property_category__enable_filterable_ingest_1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("ReplacedBucketDir_"+uuid.NewString()[:8], []string{propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			lsm := shard.pathLSM()

			mainBucket, ok := mainBucketForPropertyIndex(propName, tc.indexType)
			require.True(t, ok)
			leftover := mainBucket + lsmkv.ReplacedBucketDirSuffix
			mkSidecarDir(t, lsm, leftover)
			if tc.completedTracker != "" {
				mkTrackerDir(t, lsm, tc.completedTracker,
					"started.mig", "merged.mig", "swapped.mig", "tidied.mig")
				mkSidecarDir(t, lsm, tc.liveSidecar)
			}

			stale, _, _ := hasStalePartialReindexState(lsm, propName, tc.indexType, nil)
			require.True(t, stale,
				"a shard holding the leftover has state to sweep, so the gate must hydrate it")

			cleanSweep(t, ctx, shard, propName, tc.indexType)

			require.False(t, dirExistsAt(t, lsm, leftover),
				"the crash leftover of a bucket replacement has no other remover")
			if tc.liveSidecar != "" {
				require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
					"the bucket the in-memory pointer is on")
			}
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
	staleAfterArrival, _, _ := hasStalePartialReindexState(lsm, propName, indexType, dirs)
	require.False(t, staleAfterArrival,
		"the stale listing is the point: the gate cannot see what arrived after it")

	require.NoError(t, idx.cleanStalePartialReindexState(ctx, propName, indexType, dirs))

	require.False(t, dirExistsAt(t, lsm, ".migrations/"+tracker),
		"a loaded shard is swept whatever the gate would have said about it")
}

// A sweep that cannot even list a shard's .migrations removed nothing from it.
// Summarizing that as a finished sweep tells an operator the partial state is
// gone while every tracker is still on disk.
func TestIndexCleanStalePartialReindexStateReportsAnUnlistableMigrationsDir(t *testing.T) {
	const (
		propName  = "category"
		indexType = "filterable"
		tracker   = "enable_filterable_category_1"
	)
	ctx := testCtx()
	logger, hook := test.NewNullLogger()
	class := newTestClassWithProps("UnlistableMigrations_"+uuid.NewString()[:8], []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = logger })
	// Loaded, so the sweep reaches the shard and the unlistable directory is
	// the only thing that can fail.
	hot := shd.(*Shard)
	defer hot.Shutdown(context.Background())

	lsm := hot.pathLSM()
	mkTrackerDir(t, lsm, tracker, "started.mig")
	migrations := filepath.Join(lsm, ".migrations")
	require.NoError(t, os.Chmod(migrations, 0o000))
	t.Cleanup(func() { os.Chmod(migrations, 0o755) })
	if _, err := os.ReadDir(migrations); err == nil {
		t.Skip("this user can list a directory with no permissions, so the failure cannot be staged")
	}

	sweepErr := idx.cleanStalePartialReindexState(ctx, propName, indexType, nil)

	require.ErrorIs(t, sweepErr, ErrCleanupShardFailed)
	outcome, _ := ClassifyCleanupSweep(sweepErr)
	require.Equal(t, CleanupSweepFailed, outcome)
	summary := onlySweepSummary(t, hook)
	require.Equal(t, logrus.ErrorLevel, summary.Level)
	require.Contains(t, summary.Message, "could not be swept")

	require.NoError(t, os.Chmod(migrations, 0o755))
	require.True(t, dirExistsAt(t, lsm, ".migrations/"+tracker),
		"the state the summary must not report as swept")
}

// The gate's answer must depend on nothing but this shard's own disk, and must
// leave the loading mutex free for the hydration that follows a "no".
func TestLazyLoadShardCanSkipUnloadedSweep(t *testing.T) {
	const (
		propName   = "category"
		indexType  = "filterable"
		tracker    = "enable_filterable_category_1"
		gateShard  = "gate-tenant"
		otherShard = "other-tenant"
	)

	tests := []struct {
		name string
		load bool
		// plantOnGateShard writes whatever else the gate shard's disk holds.
		plantOnGateShard  func(t *testing.T, lsm string)
		staleOnGateShard  bool
		staleOnOtherShard bool
		wantSkip          bool
	}{
		{
			name:     "unloaded with nothing to sweep",
			wantSkip: true,
		},
		// Only a load reclaims these, so the gate has to stop skipping until
		// one has run.
		{
			name: "unloaded with a completed migration's leftovers",
			plantOnGateShard: func(t *testing.T, lsm string) {
				mkTrackerDir(t, lsm, tracker,
					"started.mig", "merged.mig", "swapped.mig", "tidied.mig")
				mkSidecarDir(t, lsm, "property_category__enable_filterable_ingest_1")
				mkSidecarDir(t, lsm, "property_category__enable_filterable_backup_1")
			},
		},
		// The population the gate is for: migrated once, finalized already,
		// nothing but its canonical buckets left.
		{
			name: "unloaded with nothing but its own bucket dirs",
			plantOnGateShard: func(t *testing.T, lsm string) {
				mkSidecarDir(t, lsm, "property_category")
			},
			wantSkip: true,
		},
		{
			name:             "unloaded with stale state",
			staleOnGateShard: true,
		},
		{
			name: "loaded and clean",
			load: true,
		},
		{
			name:             "loaded with stale state",
			load:             true,
			staleOnGateShard: true,
		},
		{
			name:              "unloaded, stale state on another shard",
			staleOnOtherShard: true,
			wantSkip:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("SweepGate_"+uuid.NewString()[:8], []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer shd.Shutdown(context.Background())

			if tc.staleOnGateShard {
				// A gate reading any other shard's path would answer "skip" here.
				mkTrackerDir(t, shardPathLSM(idx.path(), gateShard), tracker, "started.mig")
			}
			if tc.plantOnGateShard != nil {
				tc.plantOnGateShard(t, shardPathLSM(idx.path(), gateShard))
			}
			if tc.staleOnOtherShard {
				mkTrackerDir(t, shardPathLSM(idx.path(), otherShard), tracker, "started.mig")
			}

			lazy := NewLazyLoadShard(ctx, nil, gateShard, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(gateShard, lazy)
			defer func() {
				if lazy.isLoaded() {
					require.NoError(t, lazy.Shutdown(context.Background()))
				}
			}()
			if tc.load {
				require.NoError(t, lazy.Load(ctx))
			}

			gotSkip, _ := lazy.canSkipUnloadedSweep(propName, indexType, nil)
			assert.Equal(t, tc.wantSkip, gotSkip)

			require.True(t, lazy.mutex.TryLock(),
				"a held loading mutex deadlocks the hydration the caller does next")
			lazy.mutex.Unlock()
		})
	}
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
	outcome, _ := ClassifyCleanupSweep(err)
	require.Equal(t, CleanupSweepUnknown, outcome,
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
		{name: "category__reindex's own bucket, wrongly accepted", dir: main + "__reindex", want: true},
		{name: "category__ingest_0's own bucket, wrongly accepted", dir: main + "__ingest_0", want: true},
		{name: "a property named after a number", dir: main + "__12", want: false},
		{name: "a blockmax backup sidecar", dir: main + "__blockmax_map_3", want: true},
		{name: "a property whose name extends a role word", dir: main + "__ingest_x", want: false},
		// An empty tail is not a generation, so this is property
		// "category__ingest_"'s own main bucket.
		{name: "a property whose name ends in a role word and a separator", dir: main + "__ingest_", want: false},
		{name: "a sidecar whose suffix carries a strategy word", dir: main + "__blockmax_ingest", want: true},
		{
			name: "the dir a crashed bucket replacement left behind",
			dir:  main + lsmkv.ReplacedBucketDirSuffix, want: true,
		},
		// weaviate/weaviate#12621: "category__<word>_<role>" is a property's own
		// main bucket on every index type, and sweeping "category" deletes it.
		{name: "a property whose name ends in a role word", dir: main + "__ingest", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSidecarDirOf(tc.dir, main))
		})
	}
}

// A shard the gate skips has to say so. Without a line here the only three
// things a sweep can leave behind — the gate judged the shard clean, the walk
// never reached it, the sweep never ran — are indistinguishable for every
// collection whose tenants are all unloaded and clean.
func TestIndexCleanStalePartialReindexStateLogsGateSkippedShards(t *testing.T) {
	const (
		propName  = "price_cents"
		indexType = "filterable"
		tenant    = "skipped-tenant"
		gateSkip  = "partial-reindex cleanup: sweep finished, unloaded shards with nothing to sweep left unloaded"
	)
	ctx := testCtx()
	class := newTestClassWithProps("GateSkipLog_"+uuid.NewString()[:8], []string{propName})
	hookLogger, hook := test.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, func(i *Index) { i.logger = hookLogger })
	defer shd.Shutdown(context.Background())

	lazy := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(tenant, lazy)
	defer func() {
		if lazy.isLoaded() {
			require.NoError(t, lazy.Shutdown(context.Background()))
		}
	}()

	hook.Reset() // drop whatever shard startup logged
	require.NoError(t, idx.cleanStalePartialReindexState(ctx, propName, indexType, nil))

	var counts []int
	for _, entry := range hook.AllEntries() {
		if entry.Message != gateSkip {
			continue
		}
		require.Equal(t, propName, entry.Data["property"])
		require.Equal(t, indexType, entry.Data["index_type"])
		_, ok := entry.Data["payload_reads"].(int)
		require.True(t, ok, "the gate-skip line carries an int payload_reads")
		count, ok := entry.Data["skipped_shards"].(int)
		require.True(t, ok, "the gate-skip line carries an int skipped_shards")
		counts = append(counts, count)
	}
	require.Equal(t, []int{1}, counts,
		"one line per sweep, counting exactly the unloaded, clean shard as skipped")
	require.False(t, lazy.isLoaded(), "the skipped shard must not have been hydrated")
}
