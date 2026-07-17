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

// Restart-recovery silent-zero pin (weaviate/weaviate#11985):
// [Shard.IsRangeableLocallyReady] defaults to ready once the bucket exists,
// even if promoted empty from a torn ingest dir; these tests pin the gap on
// the restart-finalize crash-safety path.

// setupRangeableMigratedShard creates a shard and drives a FilterableToRangeable
// migration through the swap, leaving the shard running.
func setupRangeableMigratedShard(t *testing.T, ctx context.Context, className string) (*Shard, *Index) {
	t.Helper()
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, filterableToRangeablePropName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	return shard, idx
}

// driveRangeableMigrationToTidied drives a migration to IsTidied then shuts
// the shard down, leaving on-disk state for a restart to recover.
func driveRangeableMigrationToTidied(t *testing.T, ctx context.Context, className string) (*Index, string, string) {
	t.Helper()
	shard, idx := setupRangeableMigratedShard(t, ctx, className)
	lsmPath := shard.pathLSM()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	return idx, lsmPath, shardName
}

// restartWithCompletedRangeableSchema simulates a post-completion restart: a
// FINISHED task (no re-drive) and schema with IndexRangeFilters=true, so the
// promoted rangeable bucket loads.
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

// emptyBucketDirInPlace empties an LSM bucket dir in place so it reloads as
// an empty bucket (simulates a swap that finalized a torn ingest dir).
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

// TestRangeableRecovery_EmptyPromotedBucket_NotServedAsReady pins: an empty
// promoted rangeable bucket must not be served ready while filterable still
// holds data (weaviate/weaviate#11985).
func TestRangeableRecovery_EmptyPromotedBucket_NotServedAsReady(t *testing.T) {
	ctx := testCtx()
	className := "RangeEmptyPromote_" + uuid.NewString()[:8]

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)

	// The deferred finalize promotes an EMPTY ingest dir into the canonical
	// rangeable bucket.
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	shard := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard.Shutdown(ctx)

	// Preconditions: rangeable bucket empty, filterable bucket populated.
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

// TestRangeableRecovery_PopulatedPromotedBucket_StaysReady guards the
// regression case: a populated promoted bucket must stay ready.
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
// regression case: with no populated filterable fallback, the empty
// rangeable bucket stays on the existence default.
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

// TestRangeableRecovery_EmptyPromotion_ConvergesAcrossRestarts pins: the
// readiness reconciliation re-evaluates on-disk state every boot, so an
// empty promoted bucket keeps routing to filterable across restarts, even
// once the migration tracker is gone.
func TestRangeableRecovery_EmptyPromotion_ConvergesAcrossRestarts(t *testing.T) {
	ctx := testCtx()
	className := "RangeConverge_" + uuid.NewString()[:8]

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1), "first restart: must fall back")
	require.NoError(t, shard1.Shutdown(ctx))

	// Second restart: no tracker remains, so the verdict comes purely from
	// bucket content.
	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard2.Shutdown(ctx)
	require.False(t, rangeableReady(t, shard2),
		"second restart (no tracker): empty rangeable bucket must still route to the "+
			"filterable fallback — convergence must not depend on restart timing")
}
