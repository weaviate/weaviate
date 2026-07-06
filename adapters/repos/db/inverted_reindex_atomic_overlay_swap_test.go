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

// TestAtomicOverlaySwap_BM25NeverSeesZeroCount: pre-fix, the bucket flip
// and the overlay set were two independent critical sections, so a
// concurrent BM25 query could see a mixed (bucket, tokenization) pair and
// transiently return 0. The lookup-time half is TestPinBucketDrain_*.
func TestAtomicOverlaySwap_BM25NeverSeesZeroCount(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t, false)
	require.False(t, sawBad,
		"WITH FIX: concurrent query observed an inconsistent (bucket, tokenization) pair during the swap window: %s (reads=%d)",
		detail, reads)
}

// Asymmetry run: with the pre-fix behavior reproduced test-side, the SAME
// proof must observe the inconsistent pair — proving the proof is sensitive.
// Production carries no non-atomic knob; the nonAtomic mode composes the same
// existing seams the pre-fix code used (a two-step flip+overlay swap closure
// and separate TokenizationFor / AcquireBucketForRead reads).
func TestAtomicOverlaySwap_OldCodeIsRacy(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t, true)
	require.True(t, sawBad,
		"WITHOUT FIX: the pre-fix two-step code was expected to expose an inconsistent (bucket, tokenization) pair during the swap window, but the concurrent query never saw one (reads=%d)",
		reads)
	t.Logf("WITHOUT FIX (expected): %s", detail)
}

// runAtomicOverlaySwapProof drives one field→word swap with a widened
// flip↔overlay window while a concurrent query loop reads the
// (tokenization, bucket) pair, reporting the first non-validCount
// observation.
//
// With nonAtomic=false it exercises the production atomic path: the swap
// runs flip+overlay as one critical section (Shard.SwapBucketAndSetOverlay,
// wired by maybeWirePerPropOverlaySet) and the query resolves the pair under
// one RLock (Shard.PinTokenizationAndSearchableBucket) — no mixed pair even
// with the window widened INSIDE the lock.
//
// With nonAtomic=true it reproduces the pre-fix two-step behavior entirely
// test-side, without any production knob: the write is a swap closure that
// flips, waits, then sets the overlay as a SEPARATE step, and the read
// resolves tokenization (TokenizationFor) and bucket (AcquireBucketForRead)
// under SEPARATE locks — exactly what the old SwapBucketAndSetOverlay /
// PinTokenizationAndSearchableBucket did before the fix, so the mid-swap
// window exposes a mixed pair.
func runAtomicOverlaySwapProof(t *testing.T, nonAtomic bool) (sawBadOut bool, detailOut string, readsOut int64) {
	ctx := testCtx()
	const hookSleepMs = 50

	fx := setupTwoTokenizationShard(t, ctx, "RetokWordRace")
	shard, idx := fx.shard, fx.idx
	fieldBucket, wordBucket := fx.fieldBucket, fx.wordBucket
	className, fieldProp, wordProp := fx.className, fx.fieldProp, fx.wordProp
	phrase, validCount := fx.phrase, fx.matchDocs

	// Baseline: CONSISTENT pairs find validCount docs; MIXED pairs miss → 0.
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase),
		"(FIELD tok, FIELD bucket) must find the phrase docs")
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationWord, wordBucket, className, phrase),
		"(WORD tok, WORD bucket) must find the phrase docs")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationField, wordBucket, className, phrase),
		"(FIELD tok, WORD bucket) — the bug's signature — must MISS")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationWord, fieldBucket, className, phrase),
		"(WORD tok, FIELD bucket) — the bug's signature — must MISS")

	// The "flip" redirects the FIELD prop's searchable bucket to the
	// WORD-content bucket via the same SwapBucketPointer the migration uses.
	task := NewRuntimeSearchableRetokenizeTask(
		idx.logger, fieldProp, models.PropertyTokenizationWord,
		className, lsmkv.StrategyInverted, className, 1,
	)
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(wordProp)
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store,
		_ reindexTracker, _ int, _ string,
	) (*lsmkv.Bucket, error) {
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

	// Widen the flip↔overlay window so the race is deterministically observable.
	if nonAtomic {
		// PRE-FIX WRITE reproduction: replace the atomically-wired
		// swapPropAtomic with the old two-critical-sections form — flip, gap,
		// then overlay set as a SEPARATE step. This is literally the pre-fix
		// SwapBucketAndSetOverlay, composed test-side from the same seams
		// (processOneSwapPropFn + SetTokenizationOverlay) so production carries
		// no non-atomic branch.
		task.swapPropAtomic = func(ctx context.Context, store *lsmkv.Store,
			rt reindexTracker, propIdx int, propName string,
		) (*lsmkv.Bucket, error) {
			oldMainBucket, err := task.processOneSwapPropFn(ctx, store, rt, propIdx, propName)
			if err != nil {
				return nil, err
			}
			time.Sleep(hookSleepMs * time.Millisecond)
			shard.SetTokenizationOverlay(propName, models.PropertyTokenizationWord)
			return oldMainBucket, nil
		}
	} else {
		// WITH FIX: drive the REAL atomic critical section
		// (Shard.SwapBucketAndSetOverlay) through the existing swapPropAtomic DI
		// field — the same seam maybeWirePerPropOverlaySet wires in production —
		// and widen the flip↔overlay window INSIDE that lock, proving it covers
		// the gap. The sleep sits between the bucket flip (processOneSwapPropFn)
		// and the overlay set, exactly where the pre-fix code left the window open.
		task.swapPropAtomic = func(ctx context.Context, store *lsmkv.Store,
			rt reindexTracker, propIdx int, propName string,
		) (*lsmkv.Bucket, error) {
			return shard.SwapBucketAndSetOverlay(propName, models.PropertyTokenizationWord,
				func() (*lsmkv.Bucket, error) {
					oldMainBucket, err := task.processOneSwapPropFn(ctx, store, rt, propIdx, propName)
					if err != nil {
						return nil, err
					}
					time.Sleep(hookSleepMs * time.Millisecond)
					return oldMainBucket, nil
				})
		}
	}

	var (
		stop       atomic.Bool
		queryWG    sync.WaitGroup
		sawBad     atomic.Bool
		badOnce    sync.Once
		badDetail  string
		totalReads atomic.Int64
	)
	query := func() (int, string) {
		var (
			tok     string
			bkt     *lsmkv.Bucket
			release func()
		)
		if nonAtomic {
			// PRE-FIX READ reproduction: resolve the bucket and the
			// tokenization under SEPARATE locks — exactly the old
			// PinTokenizationAndSearchableBucket path — so a mid-swap read can
			// observe a mixed (bucket, tokenization) pair.
			bkt, release = shard.store.AcquireBucketForRead(
				helpers.BucketSearchableFromPropNameLSM(fieldProp))
			tok = shard.TokenizationFor(fieldProp, models.PropertyTokenizationField)
		} else {
			tok, bkt, release = shard.PinTokenizationAndSearchableBucket(
				fieldProp, models.PropertyTokenizationField)
		}
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

	// Post-swap state holds in both modes — the toggle only affects
	// atomicity, not the end state.
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
