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

package reindex_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// twoTokenizationFixture: a shard with a FIELD- and a WORD-tokenized prop
// carrying identical content, so redirecting the FIELD prop's bucket pointer
// to the WORD bucket emulates a runtime field→word swap.
//
// Sibling of the same-named fixture in package db (used there by the
// lookup-time half of this proof, TestPinBucketDrain_*). Duplicated rather
// than shared because the two live in different test packages.
type twoTokenizationFixture struct {
	shard  *db.Shard
	logger logrus.FieldLogger
	// captured pre-swap for identity assertions
	fieldBucket *lsmkv.Bucket
	wordBucket  *lsmkv.Bucket

	className string
	fieldProp string // FIELD-tokenized: pre-swap content
	wordProp  string // WORD-tokenized: post-swap content
	phrase    string // present verbatim in matchDocs docs, in BOTH props
	matchDocs int    // docs carrying phrase; validCount for consistent pairs
}

// setupTwoTokenizationShard writes numDocs docs of which matchDocs carry the
// phrase in BOTH props; keeping the phrase out of 100% of docs avoids BM25's
// over-frequent-term IDF collapse, so matched docs reliably score above zero.
func setupTwoTokenizationShard(t *testing.T, ctx context.Context, className string) *twoTokenizationFixture {
	t.Helper()
	const (
		fieldProp = "alpha"
		wordProp  = "beta"
		phrase    = "hello world"
		filler    = "lorem ipsum"
		numDocs   = 8
		matchDocs = 4
	)

	class := buildTwoTokenizationClass(className, fieldProp, wordProp)
	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })

	// Inverted/BlockMax is the production default and the strategy a
	// field→word retokenization runs on.
	for _, p := range []string{fieldProp, wordProp} {
		require.Equal(t, lsmkv.StrategyInverted,
			shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(p)).Strategy(),
			"searchable bucket for %q must start at Inverted", p)
	}

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

	fieldBucket := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(fieldProp))
	wordBucket := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(wordProp))
	require.NotNil(t, fieldBucket)
	require.NotNil(t, wordBucket)
	require.NotSame(t, fieldBucket, wordBucket, "field and word buckets must be distinct objects")

	return &twoTokenizationFixture{
		shard:       shard,
		logger:      f.Logger(),
		fieldBucket: fieldBucket,
		wordBucket:  wordBucket,
		className:   className,
		fieldProp:   fieldProp,
		wordProp:    wordProp,
		phrase:      phrase,
		matchDocs:   matchDocs,
	}
}

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
			UsingBlockMaxWAND:      true,
		},
		Properties: []*models.Property{
			mkProp(fieldProp, models.PropertyTokenizationField),
			mkProp(wordProp, models.PropertyTokenizationWord),
		},
	}
}

// lookupCount emulates a keyword query from a (tokenization, bucket) pair —
// a CONSISTENT pair finds the docs, a MIXED pair (the bug) misses → 0.
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

// runAtomicOverlaySwapProof drives one field→word swap with a widened
// flip↔overlay window while a concurrent query loop reads the
// (tokenization, bucket) pair, reporting the first non-validCount
// observation. nonAtomic swaps in the two-step (separate flip/overlay,
// separate-lock read) behavior instead of the atomic production path.
func runAtomicOverlaySwapProof(t *testing.T, nonAtomic bool) (sawBadOut bool, detailOut string, readsOut int64) {
	ctx := testCtx()
	const hookSleepMs = 50

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
	// WORD-content bucket via the same SwapBucketPointer the migration uses.
	task := reindex.NewRuntimeSearchableRetokenizeTask(
		fx.logger, fieldProp, models.PropertyTokenizationWord,
		className, lsmkv.StrategyInverted, className, 1,
	)
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(wordProp)
	task.ProcessOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store,
		_ reindex.ReindexTracker, _ int, _ string,
	) (*lsmkv.Bucket, error) {
		return store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
	}

	payload := &reindex.ReindexTaskPayload{
		MigrationType:      reindex.ReindexTypeChangeTokenization,
		Collection:         className,
		Properties:         []string{fieldProp},
		TargetTokenization: models.PropertyTokenizationWord,
	}
	require.True(t, reindex.MaybeWirePerPropOverlaySet(shard, payload, []*reindex.ShardReindexTaskGeneric{task}),
		"overlay wiring must be active for a tokenization-changing migration")

	// Widen the flip↔overlay window so the race is deterministically observable.
	if nonAtomic {
		// Two-step reproduction: flip, gap, then set the overlay as a
		// SEPARATE step (composed test-side; production has no such branch).
		task.SetSwapPropAtomic(func(ctx context.Context, store *lsmkv.Store,
			rt reindex.ReindexTracker, propIdx int, propName string,
		) (*lsmkv.Bucket, error) {
			oldMainBucket, err := task.ProcessOneSwapPropFn(ctx, store, rt, propIdx, propName)
			if err != nil {
				return nil, err
			}
			time.Sleep(hookSleepMs * time.Millisecond)
			shard.SetTokenizationOverlay(propName, models.PropertyTokenizationWord)
			return oldMainBucket, nil
		})
	} else {
		// Drives the real atomic critical section (Shard.SwapBucketAndSetOverlay),
		// widening the flip↔overlay window INSIDE the lock to prove it covers the gap.
		task.SetSwapPropAtomic(func(ctx context.Context, store *lsmkv.Store,
			rt reindex.ReindexTracker, propIdx int, propName string,
		) (*lsmkv.Bucket, error) {
			return shard.SwapBucketAndSetOverlay(propName, models.PropertyTokenizationWord,
				func() (*lsmkv.Bucket, error) {
					oldMainBucket, err := task.ProcessOneSwapPropFn(ctx, store, rt, propIdx, propName)
					if err != nil {
						return nil, err
					}
					time.Sleep(hookSleepMs * time.Millisecond)
					return oldMainBucket, nil
				})
		})
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
			// Resolve bucket and tokenization under SEPARATE locks, so a
			// mid-swap read can observe a mixed pair.
			bkt, release = shard.Store().AcquireBucketForRead(
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

	oldBucket, err := task.SwapPropAtomic(ctx, shard.Store(), nil, 0, fieldProp)
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
