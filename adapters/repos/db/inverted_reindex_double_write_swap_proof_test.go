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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestDoubleWrite_AfterCleanup_DoesNotPoisonCanonicalBucket pins the
// inverse of weaviate/weaviate#11688: an abandoned task's still-armed
// double-write callback must NOT mirror into the canonical bucket when it
// was cleanup, not a swap, that tore the sidecar down.
func TestDoubleWrite_AfterCleanup_DoesNotPoisonCanonicalBucket(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	// Under `word` the text yields three terms; under `field` it yields the
	// whole string as one. The joined term is therefore proof of which
	// tokenization produced the posting.
	const text = "alpha bravo charlie"
	const fieldTerm = text

	className := "DWPoison_" + uuid.NewString()[:8]
	class := retokenizeGateClass(className, propName, models.PropertyTokenizationWord)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(bucketName).Strategy()
	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField, preStrategy)
	persistTestRecoveryPayload(t, task, shard.pathLSM(), ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		Collection:         className,
		Properties:         []string{propName},
		TargetTokenization: models.PropertyTokenizationField,
	})

	// Reindex only: the ingest sidecar exists and the double-write
	// callbacks are armed for the target tokenization. No swap has run.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	sidecarName := bucketName + "__retokenize_ingest_1"
	require.NotNil(t, shard.store.Bucket(sidecarName),
		"sanity: the ingest sidecar must be live before cleanup")

	// A write while the sidecar is live mirrors into it, not into the
	// canonical bucket.
	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, text)))
	require.Contains(t, fingerprintInvertedBucket(t, shard.store.Bucket(sidecarName)), fieldTerm,
		"sanity: the armed callback must mirror target-tokenized postings into the sidecar")
	require.NotContains(t, fingerprintInvertedBucket(t, shard.store.Bucket(bucketName)), fieldTerm,
		"sanity: the canonical bucket must only ever hold source-tokenized postings")

	// The task is abandoned. Cleanup tears the sidecar down but does not
	// disable the callbacks.
	require.NoError(t, shard.CleanStalePartialReindexState(ctx, propName, "searchable"))
	require.Nil(t, shard.store.Bucket(sidecarName),
		"sanity: cleanup must have unregistered the sidecar name")

	require.NoError(t, shard.PutObject(ctx, createTestObjectWithText(className, text)))

	canonical := fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
	require.NotContains(t, canonical, fieldTerm,
		"the sidecar is gone because cleanup removed it, not because a swap ran — mirroring "+
			"into the canonical bucket writes %q-tokenized postings into an index the schema "+
			"reads as %q-tokenized, and BM25 for that term then matches documents no query "+
			"under the live schema can address",
		models.PropertyTokenizationField, models.PropertyTokenizationWord)
	require.Contains(t, canonical, "alpha",
		"the ordinary write path must still index the object under the live tokenization")
}

// TestDoubleWrite_SwapStartedFlag pins weaviate/weaviate#11688: swapStarted
// must already be true before runtimeSwap's first SwapBucketPointer call,
// or a write in that window loses its mirror.
func TestDoubleWrite_SwapStartedFlag(t *testing.T) {
	ctx := testCtx()
	className := "DWSwapFlag_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	propName := filterableToRangeablePropName

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.False(t, task.swapStarted.Load(), "a fresh task has not started a swap")

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.False(t, task.swapStarted.Load(),
		"reindexing alone must not arm the fallback — the sidecar is still live, and a "+
			"sidecar that vanishes at this point was removed by cleanup")

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.False(t, task.swapStarted.Load(), "preparing must not arm the fallback either")

	var flagAtFirstSwap bool
	origSwap := task.processOneSwapPropFn
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker,
		propIdx int, prop string,
	) (*lsmkv.Bucket, error) {
		flagAtFirstSwap = task.swapStarted.Load()
		return origSwap(ctx, store, rt, propIdx, prop)
	}

	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, flagAtFirstSwap,
		"the flag must already be set when the first SwapBucketPointer runs; set it later "+
			"and a write in that window has its mirror dropped")
	require.True(t, task.swapStarted.Load())
}
