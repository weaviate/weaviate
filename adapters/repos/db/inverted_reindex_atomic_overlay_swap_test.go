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

// TestAtomicOverlaySwap_BM25NeverSeesZeroCount is the deterministic,
// in-process proof for the field→word searchable-retokenization
// FINALIZING-window fix.
//
// SCENARIO. A class with two searchable text properties carrying the SAME
// two-word phrase ("hello world") in every doc:
//
//   - "field" — tokenized FIELD. Its searchable bucket keys the whole
//     phrase as one term, so a FIELD-tokenized phrase query returns all N
//     docs. This stands in for the property BEFORE the retokenization.
//   - "word"  — tokenized WORD. Its searchable bucket keys [hello, world],
//     so a WORD-tokenized query returns all N docs. This stands in for the
//     SAME property's content AFTER the retokenization.
//
// The field→word retokenization's runtime swap, on each replica, flips the
// property's canonical searchable-bucket pointer from the FIELD-content
// bucket to the WORD-content bucket BEFORE the cluster-wide schema flip
// commits. The per-shard tokenization overlay (overlay="word") bridges that
// local window so a query tokenizes "hello world" the WORD way and finds
// the docs in the WORD bucket. We reproduce that exact flip with a real
// store.SwapBucketPointer that redirects the "field" property's searchable
// bucket to the "word"-content bucket, run through the production
// critical-section wiring (maybeWirePerPropOverlaySet → swapPropAtomic →
// Shard.SwapBucketAndSetOverlay), with a TEST-ONLY 50ms hook fired between
// the flip and the overlay set to widen the window.
//
// THE BUG. Pre-fix, the bucket-pointer flip (under the store's
// bucketAccessLock) and the overlay set (under tokenizationOverlayMu) were
// two independent critical sections. A concurrent BM25 query that read the
// post-flip WORD bucket but the pre-set FIELD overlay tokenized the phrase
// the FIELD way ("hello world" as one term) and looked it up in a
// WORD-keyed bucket — finding nothing → count==0, a transient WRONG result.
//
// THE FIX. The flip and overlay set become ONE critical section under
// tokenizationOverlayMu (write side), and the query reads the overlay AND
// the searchable-bucket pointer under the same RLock (read side), so a
// concurrent query observes either the full pre-swap state (FIELD bucket,
// FIELD overlay → count==N) or the full post-swap state (WORD bucket, WORD
// overlay → count==N), never the mixed pair → never count==0.
//
// ASYMMETRY. With the pre-fix two-step code + the 50ms hook the concurrent
// query observes count==0 and this test FAILS; with the fix it PASSES even
// with the 50ms hook. See the QA report for both runs.
//
// SCOPE. This proves atomicity of the (tokenization, searchable-bucket)
// resolve the query performs at PROP DISCOVERY — i.e. the two reads the bug
// report identifies (GetBucket + TokenizationFor), now unified in
// Shard.PinTokenizationAndSearchableBucket. The SEPARATE lookup-time
// bucket re-fetch in createTerm / createBlockTerm is closed by threading
// the pinned bucket into the lookup phase; that half is proven by
// TestPinBucketDrain_* (inverted_reindex_pin_bucket_drain_test.go).
//
// TestAtomicOverlaySwap_BM25NeverSeesZeroCount is the WITH-FIX run: a
// concurrent query never observes an inconsistent (bucket, tokenization)
// pair during the swap window, even with the 50ms hook widening it.
func TestAtomicOverlaySwap_BM25NeverSeesZeroCount(t *testing.T) {
	sawBad, detail, reads := runAtomicOverlaySwapProof(t)
	require.False(t, sawBad,
		"WITH FIX: concurrent query observed an inconsistent (bucket, tokenization) pair during the swap window: %s (reads=%d)",
		detail, reads)
}

// TestAtomicOverlaySwap_OldCodeIsRacy is the WITHOUT-FIX run: with
// tokenizationOverlayNonAtomicForTest toggled on, SwapBucketAndSetOverlay
// and PinTokenizationAndSearchableBucket revert to the pre-fix
// two-separate-critical-sections behavior, and the SAME concurrent query —
// with the SAME 50ms hook — DOES observe the inconsistent pair. This pins
// the asymmetry: the proof is sensitive, and the old code is racy.
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

	// Baseline equivalence the proof hinges on — verified by emulating a
	// query END-TO-END from a (tokenization, bucket) pair: tokenize the
	// phrase with the tokenization, look every term up in the bucket, take
	// the best hit count. This is exactly how a real query's correctness
	// depends on the pair: the query input must be tokenized the SAME way
	// the bucket was indexed.
	//
	//   - (FIELD tok, FIELD bucket) → the whole phrase is one term, present
	//     in matchDocs docs → validCount.
	//   - (WORD  tok, WORD  bucket) → [hello, world], each present in
	//     matchDocs docs → validCount.
	//   - the MIXED pairs the bug exposes both MISS:
	//       (FIELD tok, WORD bucket): phrase term absent from a word index → 0
	//       (WORD  tok, FIELD bucket): word terms absent from a field index → 0
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase),
		"(FIELD tok, FIELD bucket) must find the phrase docs")
	require.Equal(t, validCount, lookupCount(ctx, models.PropertyTokenizationWord, wordBucket, className, phrase),
		"(WORD tok, WORD bucket) must find the phrase docs")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationField, wordBucket, className, phrase),
		"(FIELD tok, WORD bucket) — the bug's signature — must MISS")
	require.Equal(t, 0, lookupCount(ctx, models.PropertyTokenizationWord, fieldBucket, className, phrase),
		"(WORD tok, FIELD bucket) — the bug's signature — must MISS")

	// Build the field→word SearchableRetokenize task targeting the FIELD
	// prop, and wire the production overlay critical section. We override
	// processOneSwapPropFn so the per-prop "flip" redirects the FIELD prop's
	// searchable bucket to the WORD-content bucket via the real
	// store.SwapBucketPointer — the same in-memory pointer swap the runtime
	// migration performs.
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
	// observable. Fired inside swapPropAtomic's critical section, between
	// the flip and the overlay set.
	task.afterFlipBeforeOverlayHook = func() {
		time.Sleep(hookSleepMs * time.Millisecond)
	}

	// Concurrent query loop on the FIELD prop. Each iteration is exactly
	// what the production read path does for a searchable text prop: resolve
	// the effective tokenization AND the searchable bucket as one snapshot
	// (Shard.PinTokenizationAndSearchableBucket — the read-side fix),
	// then look the phrase up using THAT pair while holding the pin. With
	// the fix the pair is always consistent → always validCount. Pre-fix,
	// the read could pick up the post-flip WORD bucket with the pre-set
	// FIELD overlay (or vice versa) and miss → 0. The loop records the
	// first non-validCount result.
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

	// Give the query loop a head start so it is actively reading when the
	// swap window opens.
	time.Sleep(5 * time.Millisecond)

	// Perform the swap through the wired atomic critical section (flip +
	// 50ms hook + overlay set, all under tokenizationOverlayMu).
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
