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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
// report identifies (GetBucket + TokenizationFor, bm25_searcher.go ~216/239,
// now unified in Shard.EffectiveTokenizationAndSearchableBucket). It does NOT
// cover the SEPARATE, narrower lookup-time bucket re-fetch in createTerm /
// createBlockTerm (bm25_searcher.go:603 / bm25_searcher_block.go:42), which
// re-fetches the bucket by name independently of this snapshot; closing that
// safely requires coordinating Phase-2b bucket shutdown with in-flight
// queries and is left as a documented residual (see the QA report).
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
// and EffectiveTokenizationAndSearchableBucket revert to the pre-fix
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
	const (
		className   = "RetokWordRace"
		fieldProp   = "alpha" // FIELD-tokenized: pre-swap content
		wordProp    = "beta"  // WORD-tokenized: post-swap content
		phrase      = "hello world"
		filler      = "lorem ipsum"
		numDocs     = 8
		matchDocs   = 4 // docs carrying the queried phrase (the rest carry filler)
		validCount  = matchDocs
		hookSleepMs = 50
	)

	class := buildTwoTokenizationClass(className, fieldProp, wordProp)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })

	// Both props start at Inverted/BlockMax (UsingBlockMaxWAND:true).
	for _, p := range []string{fieldProp, wordProp} {
		require.Equal(t, lsmkv.StrategyInverted,
			shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(p)).Strategy(),
			"searchable bucket for %q must start at Inverted", p)
	}

	// Import N docs. The first matchDocs carry the queried phrase in BOTH
	// props; the rest carry filler text. Keeping the queried terms out of
	// 100% of docs avoids BM25's over-frequent-term IDF collapse, so the
	// phrase reliably scores above zero and the matched docs are returned.
	for i := 0; i < numDocs; i++ {
		text := phrase
		if i >= matchDocs {
			text = filler
		}
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					fieldProp: text,
					wordProp:  text,
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Capture the two distinct bucket identities up front so the proof can
	// assert which bucket the resolver hands back at any instant.
	fieldBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(fieldProp))
	wordBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(wordProp))
	require.NotNil(t, fieldBucket)
	require.NotNil(t, wordBucket)
	require.NotSame(t, fieldBucket, wordBucket, "field and word buckets must be distinct objects")

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
	// (Shard.EffectiveTokenizationAndSearchableBucket — the read-side fix),
	// then look the phrase up using THAT pair. With the fix the pair is
	// always consistent → always validCount. Pre-fix, the read could pick up
	// the post-flip WORD bucket with the pre-set FIELD overlay (or vice
	// versa) and miss → 0. The loop records the first non-validCount result.
	var (
		stop       atomic.Bool
		queryWG    sync.WaitGroup
		sawBad     atomic.Bool
		badOnce    sync.Once
		badDetail  string
		totalReads atomic.Int64
	)
	query := func() (int, string) {
		tok, bkt := shard.EffectiveTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
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
	tokPost, bktPost := shard.EffectiveTokenizationAndSearchableBucket(fieldProp, models.PropertyTokenizationField)
	require.Equal(t, models.PropertyTokenizationWord, tokPost,
		"overlay must route the FIELD prop to WORD post-swap")
	require.Same(t, wordBucket, bktPost,
		"post-swap the FIELD prop's searchable bucket must resolve to the WORD bucket")
	require.Equal(t, validCount, lookupCount(ctx, tokPost, bktPost, className, phrase),
		"post-swap: the (WORD tok, WORD bucket) pair must find the phrase docs")

	return sawBad.Load(), badDetail, totalReads.Load()
}

// buildTwoTokenizationClass builds a class with one FIELD-tokenized and one
// WORD-tokenized searchable text property, on Inverted/BlockMax searchable
// buckets (the production default for a field→word retokenization).
func buildTwoTokenizationClass(className, fieldProp, wordProp string) *models.Class {
	vFalse := false
	vTrue := true
	mkProp := func(name, tok string) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    tok,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Bm25:                   &models.BM25Config{K1: 1.2, B: 0.75},
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			// Inverted (BlockMax) searchable buckets — the production default
			// and the strategy a field→word searchable retokenization runs on.
			UsingBlockMaxWAND: true,
		},
		Properties: []*models.Property{
			mkProp(fieldProp, models.PropertyTokenizationField),
			mkProp(wordProp, models.PropertyTokenizationWord),
		},
	}
}

// lookupCount emulates a keyword query END-TO-END from a (tokenization,
// bucket) pair, the unit of consistency the FINALIZING-window fix protects:
// it tokenizes the query the given way, looks every resulting term up in the
// given searchable bucket via the real DocPointerWithScoreList primitive,
// and returns the best per-term hit count. A CONSISTENT pair (query
// tokenized the same way the bucket was indexed) finds the docs; a MIXED
// pair (the bug) misses → 0.
//
// We drive the lookup directly off the (tok, bucket) pair rather than
// through the full BM25 scoring pipeline so the proof isolates exactly the
// pair-consistency property — the higher-level scorer adds IDF/limit
// behavior irrelevant to the race and is exercised separately by the
// integration BM25 suites.
func lookupCount(ctx context.Context, tokenization string, bucket *lsmkv.Bucket, className, query string) int {
	if bucket == nil {
		return 0
	}
	terms := tokenizer.TokenizeForClass(tokenization, query, className)
	best := 0
	for _, term := range terms {
		dp, err := bucket.DocPointerWithScoreList(ctx, []byte(term), 1)
		if err != nil {
			return -1
		}
		if len(dp) > best {
			best = len(dp)
		}
	}
	return best
}
