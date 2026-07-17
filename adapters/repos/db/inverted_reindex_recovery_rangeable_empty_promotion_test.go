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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Restart-recovery silent-zero pin for FilterableToRangeable.
// -----------------------------------------------------------------------------
//
// weaviate/weaviate#11985 forensics: a replica that completed an
// enable-rangeable migration in-process and then restarted served a stable 0
// on range queries even though the filterable index still held the data. Root
// cause: [Shard.IsRangeableLocallyReady] defaults an un-seeded property to
// "ready" whenever its rangeable bucket EXISTS, not when it is populated. The
// restart-time finalize that promotes a runtime swap ([finalizeMigrationDir])
// is a bare directory rename with no content check, so a torn/empty ingest dir
// is promoted to a present-but-empty rangeable bucket, and — because the
// migration tracker is removed in the same startup — nothing seeds readiness.
// Served as ready, the empty bucket returns silent zeros with no fallback.
//
// weaviate/weaviate#12226's FinalizeBucketSwapLive removes the restart
// DEPENDENCY on the happy path but keeps this restart finalize as the crash
// safety net, so the gap survives on that path. These tests pin it.

// driveRangeableMigrationToTidied runs a full inline FilterableToRangeable
// migration to the IsTidied state on a throw-away shard, then shuts it down,
// leaving the on-disk state a subsequent restart recovers from. Returns the
// index (reused for the restart), the shard's LSM path, and its name.
func driveRangeableMigrationToTidied(t *testing.T, ctx context.Context, className string) (*Index, string, string) {
	t.Helper()
	propName := filterableToRangeablePropName
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	lsmPath := shard.pathLSM()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	return idx, lsmPath, shardName
}

// restartWithCompletedRangeableSchema simulates a production post-completion
// restart: the migration's DTM task is FINISHED (so it does NOT re-drive — a
// no-op reindexer), and the schema now carries IndexRangeFilters=true (as
// OnMigrationComplete's RAFT flip left it), which is what makes the promoted
// rangeable bucket load into the store.
func restartWithCompletedRangeableSchema(t *testing.T, ctx context.Context, idx *Index, shardName, className string) *Shard {
	t.Helper()
	class := newFilterableToRangeableTestClass(className)
	tr := true
	class.Properties[0].IndexRangeFilters = &tr

	idx.Config.ClassName = schema.ClassName(className)
	idx.shardReindexer = NewShardReindexerV3Noop()

	shd, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard := shd.(*Shard)
	idx.shards.Store(shardName, shd)
	return shard
}

// emptyBucketDirInPlace removes every file inside an LSM bucket directory,
// leaving the directory itself so it reloads as an empty (data-less) bucket.
// It simulates a swap that finalized an empty/torn ingest dir (the node-3
// mechanism) or a wiped fallback.
func emptyBucketDirInPlace(t *testing.T, dir string) {
	t.Helper()
	inner, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, f := range inner {
		require.NoError(t, os.RemoveAll(filepath.Join(dir, f.Name())))
	}
}

// findRangeableIngestDir locates the deferred-rename ingest sidecar dir for the
// `score` rangeable bucket post-tidy (canonical name + the ingest suffix).
func findRangeableIngestDir(t *testing.T, lsmPath string) string {
	t.Helper()
	canonical := helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName)
	entries, err := os.ReadDir(lsmPath)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), canonical) && e.Name() != canonical {
			return filepath.Join(lsmPath, e.Name())
		}
	}
	t.Fatalf("rangeable ingest sidecar dir not found under %s", lsmPath)
	return ""
}

func rangeableReady(t *testing.T, shard *Shard) bool {
	t.Helper()
	return shard.IsRangeableLocallyReady(filterableToRangeablePropName)
}

// TestRangeableRecovery_EmptyPromotedBucket_NotServedAsReady is the core pin:
// a present-but-EMPTY promoted rangeable bucket must NOT be served as locally
// ready while the filterable index still holds the data — otherwise range
// queries silently return zero. Before the fix, IsRangeableLocallyReady
// defaults to true on bucket existence and this assertion fails.
func TestRangeableRecovery_EmptyPromotedBucket_NotServedAsReady(t *testing.T) {
	ctx := testCtx()
	className := "RangeEmptyPromote_" + uuid.NewString()[:8]

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	// Node-3 mechanism: the deferred finalize will promote an EMPTY ingest
	// dir into the canonical rangeable bucket.
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	shard := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard.Shutdown(ctx)

	// Preconditions: the rangeable bucket exists but is empty, and the
	// filterable bucket still holds all the data.
	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
	require.NotNil(t, rangeBucket, "promoted rangeable bucket must exist (existence is exactly what the buggy default trusts)")
	require.False(t, rangeBucket.HasAnyData(), "promoted rangeable bucket must be empty for this repro")
	filtBucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(filterableToRangeablePropName))
	require.NotNil(t, filtBucket)
	require.True(t, filtBucket.HasAnyData(), "filterable bucket must still hold the data to fall back to")

	require.False(t, rangeableReady(t, shard),
		"empty-but-present promoted rangeable bucket must NOT be served as ready while "+
			"the filterable index holds data (else range queries silently return zero)")
}

// TestRangeableRecovery_PopulatedPromotedBucket_StaysReady guards against a
// regression: a correctly-populated promoted bucket must remain ready (the
// existence default is correct here) so we don't force an unnecessary fallback.
func TestRangeableRecovery_PopulatedPromotedBucket_StaysReady(t *testing.T) {
	ctx := testCtx()
	className := "RangePopulated_" + uuid.NewString()[:8]

	idx, _, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	shard := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard.Shutdown(ctx)

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
	require.NotNil(t, rangeBucket)
	require.True(t, rangeBucket.HasAnyData(), "populated promoted bucket must hold data")

	require.True(t, rangeableReady(t, shard),
		"a populated promoted rangeable bucket must stay ready")
}

// TestRangeableRecovery_EmptyRangeableNoFallbackData_StaysReady guards the
// regression-free gate: when there is no populated filterable fallback (here:
// filterable wiped too, e.g. a legitimately empty property), the empty
// rangeable bucket must stay on the existence default — forcing a fallback
// that has no data (or, with IndexFilterable=false, no bucket at all) would be
// pointless or would surface a spurious "not indexed" error.
func TestRangeableRecovery_EmptyRangeableNoFallbackData_StaysReady(t *testing.T) {
	ctx := testCtx()
	className := "RangeNoFallback_" + uuid.NewString()[:8]

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	// Empty BOTH the promoted rangeable bucket and the filterable fallback.
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))
	emptyBucketDirInPlace(t, filepath.Join(lsmPath,
		helpers.BucketFromPropNameLSM(filterableToRangeablePropName)))

	shard := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard.Shutdown(ctx)

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName))
	require.NotNil(t, rangeBucket)
	require.False(t, rangeBucket.HasAnyData())
	filtBucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(filterableToRangeablePropName))
	require.False(t, filtBucket == nil || filtBucket.HasAnyData(),
		"filterable fallback must be empty for this case")

	require.True(t, rangeableReady(t, shard),
		"with no populated fallback, the empty rangeable bucket must stay on the "+
			"existence default (no forced, pointless fallback)")
}

// TestRangeableRecovery_EmptyPromotion_ConvergesAcrossRestarts pins the
// convergence contract: the reconciliation re-evaluates from on-disk content
// on EVERY boot, so a present-but-empty rangeable bucket keeps routing to the
// filterable fallback across repeated restarts — independent of restart timing
// or whether a migration tracker still exists. (After the first restart the
// tracker is gone, yet the not-ready verdict must persist.)
func TestRangeableRecovery_EmptyPromotion_ConvergesAcrossRestarts(t *testing.T) {
	ctx := testCtx()
	className := "RangeConverge_" + uuid.NewString()[:8]

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1), "first restart: must fall back")
	require.NoError(t, shard1.Shutdown(ctx))

	// Second restart: no migration tracker remains on disk, so the verdict
	// must come purely from the (still-empty) bucket vs the populated
	// filterable index.
	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard2.Shutdown(ctx)
	require.False(t, rangeableReady(t, shard2),
		"second restart (no tracker): empty rangeable bucket must still route to the "+
			"filterable fallback — convergence must not depend on restart timing")
}
