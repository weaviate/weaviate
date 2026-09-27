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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func mkTrackerDir(t *testing.T, lsmPath, name string, sentinels ...string) {
	t.Helper()
	dir := filepath.Join(lsmPath, ".migrations", name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, s := range sentinels {
		require.NoError(t, os.WriteFile(filepath.Join(dir, s), []byte("x"), 0o644))
	}
}

// mkRecoveryPayload writes the payload.mig a task persists before it starts,
// which is what says whose properties a tracker dir belongs to.
func mkRecoveryPayload(t *testing.T, lsmPath, trackerName string, props ...string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"payload": map[string]any{"properties": props},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, ".migrations", trackerName, reindexRecoveryPayloadFile),
		payload, 0o644))
}

func mkSidecarDir(t *testing.T, lsmPath, name string) {
	t.Helper()
	dir := filepath.Join(lsmPath, name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "segment-0.db"), []byte("x"), 0o644))
}

// cleanSweep runs one shard's partial-reindex sweep, requires it to succeed,
// and hands back the tracker payloads it read.
func cleanSweep(t *testing.T, ctx context.Context, shard *Shard, propName, indexType string) int {
	t.Helper()
	reads, err := shard.CleanStalePartialReindexState(ctx, propName, indexType)
	require.NoError(t, err)
	return reads
}

// newCategoryShard builds a shard with one "category" property and returns it
// with its LSM path, for the cleanup tests that only need somewhere to plant
// tracker and sidecar dirs.
func newCategoryShard(t *testing.T, ctx context.Context, namePrefix string) (*Shard, string) {
	t.Helper()
	class := newTestClassWithProps(namePrefix+uuid.NewString()[:8], []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })
	return shard, shard.pathLSM()
}

// mustCompletedGens is [completedMigrationGens] for fixtures that are readable
// by construction, so a listing or sentinel error is a broken fixture.
func mustCompletedGens(t *testing.T, scope migrationDirScope) map[int]bool {
	t.Helper()
	gens, err := completedMigrationGens(scope)
	require.NoError(t, err)
	return gens
}

func dirExistsAt(t *testing.T, lsmPath, name string) bool {
	t.Helper()
	info, err := os.Stat(filepath.Join(lsmPath, name))
	if err != nil {
		require.True(t, os.IsNotExist(err), "unexpected stat error: %v", err)
		return false
	}
	return info.IsDir()
}

// TestCleanStalePartialReindexState_PreservesClassLevelDeferredFinalize pins
// issue #295: cleanup must not wipe the live ingest sidecar of a completed
// class-level migration awaiting deferred finalize.
func TestCleanStalePartialReindexState_PreservesClassLevelDeferredFinalize(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		indexType string
		// class-level completed tracker + its live ingest sidecar
		classTracker string
		liveSidecar  string
		// per-prop completed tracker + its live ingest sidecar
		propTracker     string
		propLiveSidecar string
		// stale cancelled class-level attempt, must still be deleted
		staleTracker string
		staleSidecar string
	}{
		{
			name:            "filterable: roaringset refresh gen 2 survives",
			propName:        "category",
			indexType:       "filterable",
			classTracker:    "filterable_roaringset_refresh_2",
			liveSidecar:     "property_category__roaringset_ingest_2",
			propTracker:     "enable_filterable_category_1",
			propLiveSidecar: "property_category__enable_filterable_ingest_1",
			staleTracker:    "filterable_roaringset_refresh_3",
			staleSidecar:    "property_category__roaringset_ingest_3",
		},
		{
			name:            "searchable: map_to_blockmax gen 2 survives",
			propName:        "descr",
			indexType:       "searchable",
			classTracker:    "searchable_map_to_blockmax_2",
			liveSidecar:     "property_descr_searchable__blockmax_ingest_2",
			propTracker:     "searchable_retokenize_descr_1",
			propLiveSidecar: "property_descr_searchable__retokenize_ingest_1",
			staleTracker:    "searchable_map_to_blockmax_3",
			staleSidecar:    "property_descr_searchable__blockmax_ingest_3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupPreserve_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{tc.propName})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			// Completed class-level migration in deferred-finalize state.
			mkTrackerDir(t, lsm, tc.classTracker,
				"started.mig", "merged.mig", "swapped.mig", "tidied.mig", "properties.mig")
			mkSidecarDir(t, lsm, tc.liveSidecar)

			// Completed per-prop migration in deferred-finalize state.
			mkTrackerDir(t, lsm, tc.propTracker,
				"started.mig", "merged.mig", "swapped.mig", "tidied.mig")
			mkSidecarDir(t, lsm, tc.propLiveSidecar)

			// Cancelled (partial) class-level attempt: stale, must be wiped.
			mkTrackerDir(t, lsm, tc.staleTracker, "started.mig")
			mkSidecarDir(t, lsm, tc.staleSidecar)

			cleanSweep(t, ctx, shard, tc.propName, tc.indexType)

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live class-level deferred-finalize ingest dir %s must survive cleanup; "+
					"deleting it is silent index loss on next restart (issue #295)",
				tc.liveSidecar)
			require.True(t, dirExistsAt(t, lsm, tc.propLiveSidecar),
				"live per-prop deferred-finalize ingest dir %s must survive cleanup",
				tc.propLiveSidecar)
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar),
				"stale sidecar %s of a cancelled attempt must be wiped", tc.staleSidecar)

			// Tracker-deletion semantics must be unchanged by the fix.
			require.True(t,
				dirExistsAt(t, filepath.Join(lsm, ".migrations"), tc.classTracker),
				"class-level tracker %s must not be touched by per-prop cleanup",
				tc.classTracker)
			require.True(t,
				dirExistsAt(t, filepath.Join(lsm, ".migrations"), tc.propTracker),
				"completed per-prop tracker %s must be preserved", tc.propTracker)
		})
	}
}

// TestCleanStalePartialReindexState_GenCollisionAcrossStrategies pins the
// bare-gen keying flaw (issue #295): preservation must key on
// (suffix-base, gen), not the generation int alone.
func TestCleanStalePartialReindexState_GenCollisionAcrossStrategies(t *testing.T) {
	cases := []struct {
		name                 string
		completedTracker     string
		completedTrackerMigs []string
		liveSidecar          string
		staleTracker         string
		staleSidecar         string
		wipeReason           string
	}{
		{
			name:                 "completed enable_filterable gen 1 must not preserve stale roaringset ingest_1",
			completedTracker:     "enable_filterable_category_1",
			completedTrackerMigs: []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
			liveSidecar:          "property_category__enable_filterable_ingest_1",
			staleTracker:         "filterable_roaringset_refresh_1",
			staleSidecar:         "property_category__roaringset_ingest_1",
			wipeReason: "stale roaringset ingest_1 must be wiped even though an unrelated " +
				"completed migration shares gen 1 (bare-int keying bug, issue #295)",
		},
		{
			name:                 "completed roaringset gen 2 must not preserve stale enable_filterable ingest_2",
			completedTracker:     "filterable_roaringset_refresh_2",
			completedTrackerMigs: []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig", "properties.mig"},
			liveSidecar:          "property_category__roaringset_ingest_2",
			staleTracker:         "enable_filterable_category_2",
			staleSidecar:         "property_category__enable_filterable_ingest_2",
			wipeReason: "stale enable_filterable ingest_2 must be wiped even though the " +
				"completed roaringset migration shares gen 2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupGenCollide_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"category"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			mkTrackerDir(t, lsm, tc.completedTracker, tc.completedTrackerMigs...)
			mkSidecarDir(t, lsm, tc.liveSidecar)
			mkTrackerDir(t, lsm, tc.staleTracker, "started.mig")
			mkSidecarDir(t, lsm, tc.staleSidecar)

			cleanSweep(t, ctx, shard, "category", "filterable")

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live completed-migration sidecar must survive")
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar), tc.wipeReason)
		})
	}
}

// TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix pins the
// bucket-shutdown half of the bare-gen keying bug (issue #295).
func TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix(t *testing.T) {
	ctx := testCtx()
	className := "CleanupShutdownSkip_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	// Completed class-level migration at gen 2 with its ingest bucket loaded.
	mkTrackerDir(t, lsm, "filterable_roaringset_refresh_2",
		"started.mig", "merged.mig", "swapped.mig", "tidied.mig", "properties.mig")
	liveName := "property_category__roaringset_ingest_2"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, liveName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	// Cancelled per-prop attempt at the same gen; its bucket is stale.
	mkTrackerDir(t, lsm, "enable_filterable_category_2", "started.mig")
	staleName := "property_category__enable_filterable_ingest_2"
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, staleName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))

	cleanSweep(t, ctx, shard, "category", "filterable")

	require.NotNil(t, shard.store.Bucket(liveName),
		"live deferred-finalize sidecar bucket must not be shut down")
	require.True(t, dirExistsAt(t, lsm, liveName),
		"live deferred-finalize sidecar dir must survive")
	require.Nil(t, shard.store.Bucket(staleName),
		"stale sidecar bucket must be shut down despite sharing gen 2 with "+
			"the completed class-level migration")
	require.False(t, dirExistsAt(t, lsm, staleName),
		"stale sidecar dir must be wiped")
}

// Pins that "__" in a property name (e.g. "category__extra") is not
// misread as a sidecar of "category" and shut down as one.
func TestCleanStalePartialReindexState_ShutdownSkipsOtherPropertiesBuckets(t *testing.T) {
	tests := []struct {
		name string
		// bucket is loaded in the store before the sweep runs.
		bucket string
		// wantShutDown is whether the sweep must disconnect it from the store.
		wantShutDown bool
		reason       string
	}{
		{
			// Guards the outer rule: whatever decides a sidecar, the main
			// bucket is never one.
			name:   "the swept property's own main bucket",
			bucket: "property_category",
			reason: "the sweep's job is the sidecars around the main bucket, never the " +
				"main bucket itself",
		},
		{
			name:   "another property whose name carries the sidecar separator",
			bucket: "property_category__extra",
			reason: "property \"category__extra\"'s main bucket shares this sweep's " +
				"prefix but carries no sidecar role word",
		},
		{
			name:   "another property whose name ends in a role word and a non-numeric tail",
			bucket: "property_category__ingest_x",
			reason: "\"ingest_x\" is not a role word plus a generation, so this is " +
				"property \"category__ingest_x\"'s main bucket",
		},
		{
			name:         "a sidecar a cancelled run left behind",
			bucket:       "property_category__enable_filterable_ingest_1",
			wantShutDown: true,
			reason:       "a real sidecar with no completed tracker behind it is what the sweep is for",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CleanupShutdownScope_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{"category"})
			shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			// The main bucket is already loaded from the class schema.
			if shard.store.Bucket(tc.bucket) == nil {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, tc.bucket,
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
			}

			cleanSweep(t, ctx, shard, "category", "filterable")

			if tc.wantShutDown {
				require.Nil(t, shard.store.Bucket(tc.bucket), tc.reason)
				require.False(t, dirExistsAt(t, lsm, tc.bucket), tc.reason)
				return
			}
			require.NotNil(t, shard.store.Bucket(tc.bucket), tc.reason)
			require.True(t, dirExistsAt(t, lsm, tc.bucket), tc.reason)
		})
	}
}

// TestCleanStalePartialReindexState_UnreadablePreserveSource pins issue #12647:
// when the preserve set cannot be computed, the sweep must refuse before it
// deletes state it cannot tell is live. Both faults are triggered without chmod,
// so they reproduce for root too; EACCES, EIO and fd exhaustion land in the same
// branches.
func TestCleanStalePartialReindexState_UnreadablePreserveSource(t *testing.T) {
	tracker := "filterable_roaringset_refresh_2"
	liveSidecar := "property_category__roaringset_ingest_2"

	tests := []struct {
		name string
		// break makes the preserve source unreadable for a shard at lsmPath.
		break_ func(t *testing.T, lsmPath string)
		// wantErr is a fragment the refusal must name.
		wantErr string
	}{
		{
			name:    "the .migrations listing fails",
			wantErr: ".migrations",
			break_: func(t *testing.T, lsmPath string) {
				// A regular file where the dir belongs: ReadDir gives ENOTDIR.
				require.NoError(t, os.WriteFile(
					filepath.Join(lsmPath, ".migrations"), []byte("x"), 0o644))
			},
		},
		{
			name:    "a completed-migration sentinel cannot be statted",
			wantErr: "tidied.mig",
			break_: func(t *testing.T, lsmPath string) {
				mkTrackerDir(t, lsmPath, tracker, "started.mig", "swapped.mig")
				// A symlink to itself: Stat gives ELOOP, so whether this
				// migration completed is unknowable rather than false.
				require.NoError(t, os.Symlink("tidied.mig",
					filepath.Join(lsmPath, ".migrations", tracker, "tidied.mig")))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, lsm := newCategoryShard(t, ctx, "CleanupUnreadable_")

			mkSidecarDir(t, lsm, liveSidecar)
			tc.break_(t, lsm)

			_, err := shard.CleanStalePartialReindexState(ctx, "category", "filterable")

			require.ErrorContains(t, err, tc.wantErr,
				"the refusal must name what could not be read, so an operator "+
					"knows which fault to repair")
			require.True(t, dirExistsAt(t, lsm, liveSidecar),
				"sidecar %s must survive a sweep that could not compute the preserve "+
					"set; after a tidied swap it holds the only copy of the property's data",
				liveSidecar)
		})
	}
}

// TestCleanStalePartialReindexState_OneReadableSentinelIsEnough pins that an
// unreadable sentinel only refuses the sweep when the other one did not already
// prove the migration completed. Either sentinel alone is proof, so refusing
// here would strand reclaimable state on a shard whose state is knowable.
func TestCleanStalePartialReindexState_OneReadableSentinelIsEnough(t *testing.T) {
	ctx := testCtx()
	shard, lsm := newCategoryShard(t, ctx, "CleanupOneSentinel_")

	tracker := "enable_filterable_category_1"
	liveSidecar := "property_category__enable_filterable_ingest_1"
	mkTrackerDir(t, lsm, tracker, "started.mig", "swapped.mig", "merged.mig")
	require.NoError(t, os.Symlink("tidied.mig",
		filepath.Join(lsm, ".migrations", tracker, "tidied.mig")))
	mkSidecarDir(t, lsm, liveSidecar)

	cleanSweep(t, ctx, shard, "category", "filterable")

	require.True(t, dirExistsAt(t, lsm, liveSidecar),
		"merged.mig proves gen 1 completed, so its ingest dir %s stays preserved",
		liveSidecar)
}

// TestCleanStaleMigrationDirs_UnreadableSentinelKeepsTrackerDir pins the
// tracker-dir half of #12647: the DELETE-path sweep reads the same preserve
// source, so an unreadable sentinel must stop it deleting a tracker dir whose
// generation it could not classify.
func TestCleanStaleMigrationDirs_UnreadableSentinelKeepsTrackerDir(t *testing.T) {
	lsm := t.TempDir()
	// Per-property, not class-level: the deletion pass scope omits the class dir.
	tracker := "enable_filterable_category_1"
	mkTrackerDir(t, lsm, tracker, "started.mig", "swapped.mig")
	require.NoError(t, os.Symlink("merged.mig",
		filepath.Join(lsm, ".migrations", tracker, "merged.mig")))

	logger, _ := test.NewNullLogger()
	scope := migrationDirsOf(lsm, nil, "category", "filterable")
	err := cleanStaleMigrationDirsIn(testCtx(), scope, logger)

	require.ErrorContains(t, err, "merged.mig",
		"the refusal must name the sentinel it could not read")
	require.True(t, dirExistsAt(t, filepath.Join(lsm, ".migrations"), tracker),
		"tracker dir %s must survive: an unclassifiable generation may be a "+
			"completed migration awaiting deferred finalize", tracker)
}
