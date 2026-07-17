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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// -----------------------------------------------------------------------------
// Partial-population hole in the boot-time rangeable content gate.
// -----------------------------------------------------------------------------
//
// weaviate/0-weaviate-issues#335 residual (2): reconcileRangeableReadinessAfterInit
// seeds readiness=false only when the promoted rangeable bucket is EMPTY while
// the filterable index holds data. Once new post-gate writes land SOME postings
// (writes are schema+existence gated, not readiness gated — see
// TestRangeableMasking_WriteSide_NotReadinessGated), the rangeable bucket passes
// HasAnyData on the next boot even though its HISTORICAL postings are still
// missing. The content heuristic alone then serves the short bucket as ready and
// silently under-reports history.
//
// The fix makes the not-ready verdict durable: the seeding writes a marker in
// the shard's .migrations dir that survives across boots and is cleared only by
// an explicit rebuild (FilterableToRangeableStrategy.OnMigrationComplete).

// partiallyRepopulateRangeable lands n post-gate rangeable postings directly in
// the promoted bucket, exactly as [Shard.addToPropertyRangeBucket] does when a
// client write hits a property with HasRangeableIndex=true. docIDs are disjoint
// from the migration backfill so the induced postings are unambiguous. Applied
// directly (not via PutObject) because the analyzer reads HasRangeableIndex from
// the schema getter, which restartWithCompletedRangeableSchema does not flip —
// the same reason the sibling tail-loss tests write to the bucket directly.
func partiallyRepopulateRangeable(t *testing.T, rb *lsmkv.Bucket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		docID := uint64(2000 + i)
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, int64(docID)), docID))
	}
}

// TestRangeableRecovery_PartialRepopulation_StaysNotReadyAcrossRestart is the
// two-boot red/green pin for the partial-population hole. Boot 1 gates the empty
// promoted bucket not-ready; a few post-gate writes then partially repopulate
// it; boot 2 must STILL be not-ready even though HasAnyData now returns true —
// otherwise the missing history is served as complete.
func TestRangeableRecovery_PartialRepopulation_StaysNotReadyAcrossRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangePartialRepop_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	// Node-3 mechanism: the deferred finalize promotes an EMPTY ingest dir into
	// the canonical rangeable bucket.
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	// Boot 1: the content gate must route to the filterable fallback.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1),
		"boot1: empty promoted rangeable bucket must gate not-ready")

	// Post-gate writes land SOME rangeable postings, partially repopulating the
	// bucket while the historical postings remain absent (they only ever lived
	// in the filterable index for this torn shard).
	rb1 := shard1.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rb1)
	partiallyRepopulateRangeable(t, rb1, 5)
	require.True(t, rb1.HasAnyData(),
		"post-gate writes must have partially repopulated the rangeable bucket")
	require.NoError(t, shard1.Shutdown(ctx))

	// Boot 2: the bucket now passes HasAnyData, so the content heuristic ALONE
	// would serve it as ready and hide the missing history. The durable verdict
	// must keep it not-ready.
	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard2.Shutdown(ctx)
	rb2 := shard2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rb2)
	require.True(t, rb2.HasAnyData(),
		"boot2 precondition: partially-repopulated bucket passes HasAnyData")
	filt2 := shard2.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, filt2)
	require.True(t, filt2.HasAnyData(),
		"boot2 precondition: filterable index still holds the (larger) history")

	require.False(t, rangeableReady(t, shard2),
		"boot2: a partially-repopulated rangeable bucket that still misses history "+
			"MUST stay not-ready (durable verdict), else range queries under-report")
}

// TestRangeableRecovery_IncompleteVerdict_ClearedRestoresReady proves the
// durable verdict is not permanently sticky: once the marker is cleared (which
// FilterableToRangeableStrategy.OnMigrationComplete does after an explicit
// rebuild repopulates the index from the objects bucket), a subsequent boot
// trusts the now-complete bucket again and serves it as ready.
func TestRangeableRecovery_IncompleteVerdict_ClearedRestoresReady(t *testing.T) {
	ctx := testCtx()
	className := "RangeVerdictCleared_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	// Boot that gates not-ready and persists the durable verdict.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1), "must gate not-ready and persist the marker")
	// Repopulate the bucket so HasAnyData is true (mirrors a completed rebuild)
	// and clear the durable verdict the way OnMigrationComplete does.
	rb1 := shard1.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rb1)
	partiallyRepopulateRangeable(t, rb1, 5)
	require.NoError(t, shard1.removeRangeableIncompleteSentinel(propName))
	require.NoError(t, shard1.Shutdown(ctx))

	// Next boot: no marker, bucket has data → back to the existence default.
	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	defer shard2.Shutdown(ctx)
	require.True(t, rangeableReady(t, shard2),
		"after the verdict is cleared, a populated rangeable bucket must be ready again")
}
