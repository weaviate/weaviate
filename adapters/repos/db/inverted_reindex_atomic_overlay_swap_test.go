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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// Pins the write-time bucket/overlay race: a concurrent BM25 query must
// never see a mixed (bucket, tokenization) pair mid-swap (lookup-time half:
// TestPinBucketDrain_*).
func TestAtomicOverlaySwap_BM25NeverSeesZeroCount(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t, false)
	require.False(t, sawBad,
		"WITH FIX: concurrent query observed an inconsistent (bucket, tokenization) pair during the swap window: %s (reads=%d)",
		detail, reads)
}

// Sensitivity check: the same proof, with the two-step (non-atomic)
// flip+overlay behavior reproduced test-side, must observe the mixed pair.
func TestAtomicOverlaySwap_OldCodeIsRacy(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t, true)
	require.True(t, sawBad,
		"WITHOUT FIX: the pre-fix two-step code was expected to expose an inconsistent (bucket, tokenization) pair during the swap window, but the concurrent query never saw one (reads=%d)",
		reads)
	t.Logf("WITHOUT FIX (expected): %s", detail)
}

// runAtomicOverlaySwapProof swaps field→word while a query loop reports the first wrong count;
// nonAtomic flips and sets the overlay in two steps with a widened gap.
func runAtomicOverlaySwapProof(t *testing.T, nonAtomic bool) (sawBadOut bool, detailOut string, readsOut int64) {
	ctx := testCtx()

	fx := setupTwoTokenizationShard(t, ctx, "RetokWordRace")
	shard := fx.shard
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
	// WORD-content bucket, as the migration's flip does.
	pair := lsmkv.BucketSwap{
		Target: helpers.BucketSearchableFromPropNameLSM(fieldProp),
		Source: helpers.BucketSearchableFromPropNameLSM(wordProp),
	}
	wordOverlay := inverted.PropertyOverlay{Tokenization: models.PropertyTokenizationWord}

	var (
		stop       atomic.Bool
		queryWG    sync.WaitGroup
		sawBad     atomic.Bool
		badOnce    sync.Once
		badDetail  string
		totalReads atomic.Int64
	)

	swap := func() (*lsmkv.Bucket, error) {
		displaced, err := shard.SwapBucketsAndSetOverlay(
			map[string]inverted.PropertyOverlay{fieldProp: wordOverlay}, []lsmkv.BucketSwap{pair})
		if err != nil {
			return nil, err
		}
		return displaced[0], nil
	}
	if nonAtomic {
		// Two-step reproduction: flip, gap, then set the overlay as a
		// SEPARATE step (composed test-side; production has no such branch).
		swap = func() (*lsmkv.Bucket, error) {
			oldMainBucket, err := shard.store.SwapBucketPointer(ctx, pair.Target, pair.Source)
			if err != nil {
				return nil, err
			}
			// Hold the gap open until the query lands in it: a fixed window
			// can pass unobserved on a loaded runner.
			deadline := time.Now().Add(10 * time.Second)
			for !sawBad.Load() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			shard.SetPropertyOverlay(fieldProp, wordOverlay)
			return oldMainBucket, nil
		}
	}

	query := func() (int, string) {
		var (
			tok     string
			bkt     *lsmkv.Bucket
			release func()
		)
		if nonAtomic {
			// Resolve bucket and tokenization under SEPARATE locks, so a
			// mid-swap read can observe a mixed pair.
			bkt, release = shard.store.AcquireBucketForRead(
				helpers.BucketSearchableFromPropNameLSM(fieldProp))
			tok = queryTokenization(shard, fieldProp, models.PropertyTokenizationField)
		} else {
			tok, bkt, release = shard.PinTokenizationAndBucket(
				fieldProp, helpers.BucketSearchableFromPropNameLSM(fieldProp), models.PropertyTokenizationField)
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

	oldBucket, err := swap()
	require.NoError(t, err)
	require.NotNil(t, oldBucket, "swap must return the displaced old FIELD bucket")

	// Keep querying briefly past the swap to catch any late inconsistent read.
	time.Sleep(20 * time.Millisecond)
	stop.Store(true)
	queryWG.Wait()

	require.Positive(t, totalReads.Load(), "query loop must have executed at least once")

	// Post-swap state holds in both modes — the toggle only affects
	// atomicity, not the end state.
	tokPost, bktPost, releasePost := shard.PinTokenizationAndBucket(fieldProp,
		helpers.BucketSearchableFromPropNameLSM(fieldProp), models.PropertyTokenizationField)
	defer releasePost()
	require.Equal(t, models.PropertyTokenizationWord, tokPost,
		"overlay must route the FIELD prop to WORD post-swap")
	require.Same(t, wordBucket, bktPost,
		"post-swap the FIELD prop's searchable bucket must resolve to the WORD bucket")
	require.Equal(t, validCount, lookupCount(ctx, tokPost, bktPost, className, phrase),
		"post-swap: the (WORD tok, WORD bucket) pair must find the phrase docs")

	return sawBad.Load(), badDetail, totalReads.Load()
}
