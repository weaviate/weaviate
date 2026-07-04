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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// TestAtomicOverlaySwap_BM25NeverSeesZeroCount proves the prop-discovery
// half of the FINALIZING-window fix: pre-fix, the bucket-pointer flip and
// the overlay set were two independent critical sections, so a concurrent
// BM25 query could read the post-flip bucket with the pre-set overlay,
// tokenize the phrase the OLD way, and miss → transient count==0. With
// flip+overlay in ONE critical section (SwapBucketAndSetOverlay) and the
// query reading both under the same RLock, no mixed pair is observable.
//
// The two-tokenization fixture emulates the swap: a real SwapBucketPointer
// redirects the FIELD prop's searchable bucket to the WORD-content bucket
// through the production wiring, with a TEST-ONLY 50ms hook widening the
// flip↔overlay window; a concurrent query loop asserts it only ever sees
// validCount. The lookup-time half is proven by TestPinBucketDrain_*.
func TestAtomicOverlaySwap_BM25NeverSeesZeroCount(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t)
	require.False(t, sawBad,
		"WITH FIX: concurrent query observed an inconsistent (bucket, tokenization) pair during the swap window: %s (reads=%d)",
		detail, reads)
}

// TestAtomicOverlaySwap_OldCodeIsRacy is the asymmetry run: with the
// pre-fix two-critical-sections behavior restored, the SAME proof DOES
// observe the inconsistent pair — the proof is sensitive.
func TestAtomicOverlaySwap_OldCodeIsRacy(t *testing.T) {
	tokenizationOverlayNonAtomicForTest = true
	t.Cleanup(func() { tokenizationOverlayNonAtomicForTest = false })

	sawBad, detail, reads := runAtomicOverlaySwapProof(t)
	require.True(t, sawBad,
		"WITHOUT FIX: the pre-fix two-step code was expected to expose an inconsistent (bucket, tokenization) pair during the swap window, but the concurrent query never saw one (reads=%d)",
		reads)
	t.Logf("WITHOUT FIX (expected): %s", detail)
}

// runAtomicOverlaySwapProof drives one field→word searchable-retokenization
// swap with a 50ms flip↔overlay window while a concurrent query loop reads
// the (tokenization, bucket) pair via the production read-side resolver and
// looks the phrase up using that pair. It returns whether any read observed
// a non-validCount result (an inconsistent pair), a human-readable detail of
// the first such observation, and the number of reads performed.
func runAtomicOverlaySwapProof(t *testing.T) (sawBadOut bool, detailOut string, readsOut int64) {
	ctx := testCtx()
	const hookSleepMs = 50

	fx := setupTwoTokenizationShard(t, ctx, "RetokWordRace")
	shard, idx := fx.shard, fx.idx
	fieldBucket, wordBucket := fx.fieldBucket, fx.wordBucket
	className, fieldProp, wordProp := fx.className, fx.fieldProp, fx.wordProp
	phrase, validCount := fx.phrase, fx.matchDocs

	// Baseline equivalence the proof hinges on: both CONSISTENT pairs find
	// validCount docs; both MIXED pairs (the bug's signature) miss → 0.
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase),
		"(FIELD tok, FIELD bucket) must find the phrase docs")
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationWord, wordBucket, className, phrase),
		"(WORD tok, WORD bucket) must find the phrase docs")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationField, wordBucket, className, phrase),
		"(FIELD tok, WORD bucket) — the bug's signature — must MISS")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationWord, fieldBucket, className, phrase),
		"(WORD tok, FIELD bucket) — the bug's signature — must MISS")

	// Override processOneSwapPropFn so the per-prop "flip" redirects the
	// FIELD prop's searchable bucket to the WORD-content bucket via the real
	// store.SwapBucketPointer — the same swap the runtime migration performs.
	task := NewRuntimeSearchableRetokenizeTask(
		idx.logger, fieldProp, models.PropertyTokenizationWord,
		className, lsmkv.StrategyInverted, className, 1,
	)
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(wordProp)
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store,
		_ reindexTracker, _ int, _ string,
	) (*lsmkv.Bucket, error) {
		// Redirect fieldBucketName → the WORD-content bucket. Returns the
		// displaced (old FIELD) bucket, mirroring processOneSwapProp.
		return store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
	}

	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		Collection:         className,
		Properties:         []string{fieldProp},
		TargetTokenization: models.PropertyTokenizationWord,
	}
	require.True(t, maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task}),
		"overlay wiring must be active for a tokenization-changing migration")

	// Widen the flip↔overlay window so the race is deterministically
	// observable.
	task.afterFlipBeforeOverlayHook = func() {
		time.Sleep(hookSleepMs * time.Millisecond)
	}

	// Concurrent query loop: resolve the (tokenization, bucket) snapshot
	// via the production read-side resolver and look the phrase up with
	// that pair, recording the first non-validCount observation.
	var (
		stop       atomic.Bool
		queryWG    sync.WaitGroup
		sawBad     atomic.Bool
		badOnce    sync.Once
		badDetail  string
		totalReads atomic.Int64
	)
	query := func() (int, string) {
		tok, bkt, release := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
		defer release()
		which := "other"
		switch bkt {
		case fieldBucket:
			which = "FIELD"
		case wordBucket:
			which = "WORD"
		}
		return lookupCount(ctx, tok, bkt, className, phrase),
			"tok=" + tok + " bucket=" + which
	}
	queryWG.Add(1)
	go func() {
		defer queryWG.Done()
		for !stop.Load() {
			c, detail := query()
			totalReads.Add(1)
			if c != validCount {
				badOnce.Do(func() {
					badDetail = "observed count=" + strconv.Itoa(c) +
						" (valid=" + strconv.Itoa(validCount) + ") with " + detail
					sawBad.Store(true)
				})
			}
		}
	}()

	// Head start so the loop is actively reading when the swap window opens.
	time.Sleep(5 * time.Millisecond)

	oldBucket, err := task.swapPropAtomic(ctx, shard.store, nil, 0, fieldProp)
	require.NoError(t, err)
	require.NotNil(t, oldBucket, "swap must return the displaced old FIELD bucket")

	// Keep querying briefly past the swap to catch any late inconsistent read.
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	queryWG.Wait()

	require.Positive(t, totalReads.Load(), "query loop must have executed at least once")

	// Post-swap the FIELD prop resolves to the WORD bucket with overlay=word.
	// (Holds in both modes — the toggle only affects atomicity, not the end
	// state.)
	tokPost, bktPost, releasePost := shard.PinTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	defer releasePost()
	require.Equal(t, models.PropertyTokenizationWord, tokPost,
		"overlay must route the FIELD prop to WORD post-swap")
	require.Same(t, wordBucket, bktPost,
		"post-swap the FIELD prop's searchable bucket must resolve to the WORD bucket")
	require.Equal(t, validCount, lookupCount(ctx, tokPost, bktPost, className, phrase),
		"post-swap: the (WORD tok, WORD bucket) pair must find the phrase docs")

	return sawBad.Load(), badDetail, totalReads.Load()
}
