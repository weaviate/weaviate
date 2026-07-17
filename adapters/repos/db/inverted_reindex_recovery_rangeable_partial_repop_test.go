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

// Partial-population hole (weaviate/0-weaviate-issues#335): the boot-time
// content gate only flags readiness=false while the bucket is EMPTY;
// post-gate writes can later flip HasAnyData true while history is still
// missing, so the fix persists a durable marker until an explicit rebuild.

// partiallyRepopulateRangeable writes n disjoint postings directly to the
// bucket (not via PutObject, since restartWithCompletedRangeableSchema
// doesn't flip the schema's HasRangeableIndex).
func partiallyRepopulateRangeable(t *testing.T, rb *lsmkv.Bucket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		docID := uint64(2000 + i)
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, int64(docID)), docID))
	}
}

// TestRangeableRecovery_PartialRepopulation_StaysNotReadyAcrossRestart pins:
// a partially-repopulated bucket must stay not-ready across a restart even
// though HasAnyData now returns true.
func TestRangeableRecovery_PartialRepopulation_StaysNotReadyAcrossRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangePartialRepop_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	// The deferred finalize promotes an EMPTY ingest dir into the canonical
	// rangeable bucket.
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	// Boot 1: the content gate must route to the filterable fallback.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1),
		"boot1: empty promoted rangeable bucket must gate not-ready")

	// Post-gate writes partially repopulate the bucket; historical postings
	// remain absent.
	rb1 := shard1.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rb1)
	partiallyRepopulateRangeable(t, rb1, 5)
	require.True(t, rb1.HasAnyData(),
		"post-gate writes must have partially repopulated the rangeable bucket")
	require.NoError(t, shard1.Shutdown(ctx))

	// Boot 2: HasAnyData alone would now serve it ready; the durable verdict
	// must override.
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

// TestRangeableRecovery_IncompleteVerdict_ClearedRestoresReady pins: once the
// durable marker is cleared (as OnMigrationComplete does post-rebuild), a
// subsequent boot serves the bucket ready again.
func TestRangeableRecovery_IncompleteVerdict_ClearedRestoresReady(t *testing.T) {
	ctx := testCtx()
	className := "RangeVerdictCleared_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName

	idx, lsmPath, shardName := driveRangeableMigrationToTidied(t, ctx, className)
	emptyBucketDirInPlace(t, findRangeableIngestDir(t, lsmPath))

	// Boot that gates not-ready and persists the durable verdict.
	shard1 := restartWithCompletedRangeableSchema(t, ctx, idx, shardName, className)
	require.False(t, rangeableReady(t, shard1), "must gate not-ready and persist the marker")
	// Repopulate + clear the marker, mirroring a completed rebuild.
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
