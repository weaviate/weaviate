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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// mkTrackerDir creates .migrations/<name>/ under lsmPath with the given
// sentinel files.
func mkTrackerDir(t *testing.T, lsmPath, name string, sentinels ...string) {
	t.Helper()
	dir := filepath.Join(lsmPath, ".migrations", name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, s := range sentinels {
		require.NoError(t, os.WriteFile(filepath.Join(dir, s), []byte("x"), 0o644))
	}
}

// mkSidecarDir creates a fake on-disk sidecar bucket dir under lsmPath.
func mkSidecarDir(t *testing.T, lsmPath, name string) {
	t.Helper()
	dir := filepath.Join(lsmPath, name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "segment-0.db"), []byte("x"), 0o644))
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

// TestCleanStalePartialReindexState_PreservesClassLevelDeferredFinalize
// pins the soak-observed data-loss bug (issue #295): a completed
// class-level migration (filterable_roaringset_refresh /
// searchable_map_to_blockmax) in deferred-finalize state has its live
// ingest sidecar dir on disk (the in-memory main bucket pointer's backing
// store). A subsequent CleanStalePartialReindexState for the same
// (prop, indexType) — e.g. pre-submit defense-in-depth for an unrelated
// per-prop migration — must NOT delete that ingest dir. Before the fix,
// the preserve set was computed only from the per-prop tracker prefixes,
// so the class-level gen was invisible and the live dir was wiped:
// silent index loss on next restart, ENOENT on the next migration's swap.
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
		// genuinely stale sidecar of a cancelled class-level attempt
		// (tracker without tidied/merged) — must still be deleted
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
				false, false, false)
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

			require.NoError(t,
				shard.CleanStalePartialReindexState(ctx, tc.propName, tc.indexType))

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live class-level deferred-finalize ingest dir %s must survive cleanup; "+
					"deleting it is silent index loss on next restart (issue #295)",
				tc.liveSidecar)
			require.True(t, dirExistsAt(t, lsm, tc.propLiveSidecar),
				"live per-prop deferred-finalize ingest dir %s must survive cleanup",
				tc.propLiveSidecar)
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar),
				"stale sidecar %s of a cancelled attempt must be wiped", tc.staleSidecar)

			// Tracker-deletion semantics are unchanged: class-level tracker
			// dirs are never deleted by the per-prop sweep; the completed
			// per-prop tracker is preserved by the existing tidied/merged gate.
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
// second flaw from issue #295: preservation used to be keyed by BARE
// generation int, so a completed migration of strategy A at gen N
// accidentally preserved (and, symmetrically, could fail to protect) a
// DIFFERENT strategy's sidecar at the same gen. Preservation must be
// keyed by (sidecar-suffix-base, gen): a roaringset ingest dir survives
// iff a completed roaringset-refresh tracker of the SAME gen exists.
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
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)
			lsm := shard.pathLSM()

			mkTrackerDir(t, lsm, tc.completedTracker, tc.completedTrackerMigs...)
			mkSidecarDir(t, lsm, tc.liveSidecar)
			mkTrackerDir(t, lsm, tc.staleTracker, "started.mig")
			mkSidecarDir(t, lsm, tc.staleSidecar)

			require.NoError(t,
				shard.CleanStalePartialReindexState(ctx, "category", "filterable"))

			require.True(t, dirExistsAt(t, lsm, tc.liveSidecar),
				"live completed-migration sidecar must survive")
			require.False(t, dirExistsAt(t, lsm, tc.staleSidecar), tc.wipeReason)
		})
	}
}

// TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix pins the
// bucket-shutdown half of the same keying bug: the skip that protects a
// LIVE loaded sidecar bucket from being shut down must match by
// (suffix-base, gen), and must recognize class-level completed gens.
// Shutting the live bucket down and deleting its dir tears the backing
// store out from under the in-memory main bucket pointer.
func TestCleanStalePartialReindexState_ShutdownSkipKeyedBySuffix(t *testing.T) {
	ctx := testCtx()
	className := "CleanupShutdownSkip_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"category"})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	// Completed class-level migration at gen 2; its ingest bucket is loaded
	// (it IS the main bucket's backing store until next-restart finalize).
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

	require.NoError(t,
		shard.CleanStalePartialReindexState(ctx, "category", "filterable"))

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
