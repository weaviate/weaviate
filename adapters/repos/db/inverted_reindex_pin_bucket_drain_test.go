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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestPinBucketDrain_LookupHoldsPinAcrossSwapShutdown is the deterministic,
// in-process proof for the LOOKUP-TIME bucket-pin + Shutdown-drain fix — the
// residual #11540 explicitly left open (see the SCOPE note in
// inverted_reindex_atomic_overlay_swap_test.go).
//
// THE RESIDUAL. A BM25 query touches the searchable bucket TWICE: once at
// prop discovery (where #11540 pairs tokenization+bucket atomically) and
// AGAIN at lookup, where createTerm / createBlockTerm RE-FETCH the searchable
// bucket BY NAME. A field→word retokenization's per-prop swap
// (store.SwapBucketPointer) followed by oldBucket.Shutdown (which frees the
// old bucket's mmap'd segments) can land between those two touches: the query
// tokenized FIELD-way at discovery but the lookup re-fetch lands on the
// post-swap WORD bucket → wrong count; worse, the Shutdown could free the
// bucket's mmap under a live read → use-after-free.
//
// THE FIX. Prop discovery now PINS each searchable bucket
// (lifetimeLock.RLock, via Store.AcquireBucketForRead, under the
// tokenization-overlay RLock so the pin is the same snapshot as the
// tokenization). The pinned pointer is threaded into createTerm /
// createBlockTerm, so lookup uses the EXACT bucket discovery resolved — never
// a re-fetch. Bucket.Shutdown takes lifetimeLock.Lock as its first action,
// DRAINING every in-flight pin before freeing any mmap.
//
// This test drives the REAL production query path (BM25F → wandBlock →
// createBlockTerm) on the FIELD prop, holds it in the
// prop-discovery→lookup gap with a test hook, and while it is held performs
// a real store.SwapBucketPointer(field→word) + oldBucket.Shutdown in another
// goroutine. It asserts:
//
//	(a) CONSISTENCY: the held query returns the pre-swap count (the pinned
//	    FIELD bucket's matchDocs), never 0 and never the WORD bucket's view.
//	(b) DRAIN: oldBucket.Shutdown does NOT complete until the pinned query
//	    releases — proven by ordering the "shutdown returned" timestamp
//	    strictly after the "query released its pin" timestamp.
//	(c) NO DEADLOCK / NO UAF: the whole thing finishes well within a generous
//	    timeout and is race-clean under -race.
//
// The asymmetry (pre-fix re-fetch yields the wrong count) is proven by the
// sibling TestPinBucketDrain_OldRefetchYieldsWrongCount.
func TestPinBucketDrain_LookupHoldsPinAcrossSwapShutdown(t *testing.T) {
	res := runPinBucketDrainProof(t, false /* forceRefetch */)

	require.False(t, res.deadlocked, "the proof must not deadlock")
	require.Equal(t, res.validCount, res.queryCount,
		"WITH FIX: the held query must return the pinned (pre-swap FIELD) bucket's count, never the post-swap WORD view")
	require.NoError(t, res.queryErr, "the held query must not error")

	// DRAIN: Shutdown of the displaced old bucket must have BLOCKED until the
	// pinned query released. We prove it by ordering: the query released its
	// pin (queryReleasedAt) strictly before Shutdown returned (shutdownAt).
	require.False(t, res.shutdownAt.IsZero(), "old bucket Shutdown must have completed")
	require.False(t, res.queryReleasedAt.IsZero(), "the query must have released its pin")
	require.True(t, res.shutdownAt.After(res.queryReleasedAt) || res.shutdownAt.Equal(res.queryReleasedAt),
		"DRAIN VIOLATED: old bucket Shutdown returned at %s BEFORE the pinned query released at %s — Shutdown did not drain the in-flight pin",
		res.shutdownAt.Format(time.StampNano), res.queryReleasedAt.Format(time.StampNano))

	// Additionally prove Shutdown was actually blocked (not merely fast): it
	// must not have completed before the swap-side goroutine even observed the
	// query had taken its pin and started the Shutdown.
	require.True(t, res.shutdownStartedAt.Before(res.shutdownAt) || res.shutdownStartedAt.Equal(res.shutdownAt),
		"sanity: Shutdown must start before it returns")
	t.Logf("WITH FIX: queryCount=%d (valid=%d), pin released at %s, shutdown started %s, shutdown returned %s (blocked %s)",
		res.queryCount, res.validCount,
		res.queryReleasedAt.Format(time.StampMicro),
		res.shutdownStartedAt.Format(time.StampMicro),
		res.shutdownAt.Format(time.StampMicro),
		res.shutdownAt.Sub(res.shutdownStartedAt))
}

// TestPinBucketDrain_OldRefetchYieldsWrongCount is the asymmetry run. With
// the lookup forced back to the pre-fix re-fetch-by-name behavior
// (WithForceLookupRefetchForTest), the SAME held query — after the SAME
// field→word swap — re-fetches the post-swap WORD bucket and, having
// tokenized the phrase FIELD-way, MISSES → count 0. This pins the proof's
// sensitivity: the lookup-time pin is what makes the WITH-FIX run correct.
//
// NOTE: this variant deliberately does NOT shut the old bucket down (the
// re-fetch already discards it), so the wrong-count signal is observed
// cleanly without depending on use-after-free timing.
func TestPinBucketDrain_OldRefetchYieldsWrongCount(t *testing.T) {
	res := runPinBucketDrainProof(t, true /* forceRefetch */)

	require.False(t, res.deadlocked, "the asymmetry proof must not deadlock")
	require.NoError(t, res.queryErr, "the query must not error (it just finds nothing)")
	require.Equal(t, 0, res.queryCount,
		"WITHOUT FIX (re-fetch): the held query tokenized FIELD-way but re-fetched the post-swap WORD bucket, so it must MISS → 0 (valid would be %d)",
		res.validCount)
	t.Logf("WITHOUT FIX (expected): queryCount=%d (valid would be %d) — re-fetch landed on the post-swap WORD bucket",
		res.queryCount, res.validCount)
}

type pinBucketDrainResult struct {
	validCount        int
	queryCount        int
	queryErr          error
	deadlocked        bool
	queryReleasedAt   time.Time
	shutdownStartedAt time.Time
	shutdownAt        time.Time
}

// runPinBucketDrainProof builds a shard with a FIELD-tokenized and a
// WORD-tokenized searchable prop (same two-tokenization fixture as the
// #11540 proof), inserts docs so a FIELD-phrase query on the FIELD prop
// returns matchDocs, then drives a single real BM25F query on the FIELD prop
// held in the prop-discovery→lookup gap while a concurrent goroutine swaps
// the FIELD prop's searchable pointer to the WORD bucket and (when
// !forceRefetch) shuts the displaced old FIELD bucket down.
func runPinBucketDrainProof(t *testing.T, forceRefetch bool) pinBucketDrainResult {
	ctx := testCtx()
	const (
		className = "PinDrainRetok"
		fieldProp = "alpha" // FIELD-tokenized: pre-swap content the query expects
		wordProp  = "beta"  // WORD-tokenized: post-swap content
		phrase    = "hello world"
		filler    = "lorem ipsum"
		numDocs   = 8
		matchDocs = 4 // docs carrying the phrase; rest carry filler
	)

	class := buildTwoTokenizationClass(className, fieldProp, wordProp)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { _ = shard.Shutdown(ctx) })

	for _, p := range []string{fieldProp, wordProp} {
		require.Equal(t, lsmkv.StrategyInverted,
			shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(p)).Strategy(),
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

	fieldBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(fieldProp))
	wordBucket := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(wordProp))
	require.NotNil(t, fieldBucket)
	require.NotNil(t, wordBucket)
	require.NotSame(t, fieldBucket, wordBucket, "field and word buckets must be distinct")

	// Build the production BM25Searcher exactly like shard_read.go, plus the
	// pinning resolver and the test hooks.
	bm25Config := idx.GetInvertedIndexConfig().BM25
	logger := idx.logger.WithFields(logrus.Fields{"class": className, "shard": shard.name})

	var (
		pinnedCh   = make(chan struct{}) // closed inside the hook once pins are held
		releaseCh  = make(chan struct{}) // closed by the test to let the held query proceed
		releasedAt atomic.Pointer[time.Time]
	)

	bm25 := inverted.NewBM25Searcher(bm25Config, shard.store,
		idx.getSchema.ReadOnlyClass, shard.propertyIndices, idx.classSearcher,
		idx.getStopwordProvider(), shard.GetPropertyLengthTracker(), logger,
		shard.versioner.Version()).
		WithTokenizationResolver(shard.TokenizationFor).
		WithSearchableBucketPinningResolver(shard.PinTokenizationAndSearchableBucket).
		WithForceLookupRefetchForTest(forceRefetch).
		WithAfterPinBeforeLookupHookForTest(func() {
			// Pins are now held. Signal the swap goroutine and block until the
			// test releases us — widening the prop-discovery→lookup window so
			// the swap+Shutdown is guaranteed to be in flight while we hold the
			// pin.
			close(pinnedCh)
			<-releaseCh
		})

	// Determine the validCount the FIELD/FIELD pair yields, via the same
	// end-to-end lookup the #11540 proof uses (independent of the held query).
	validCount := lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase)
	require.Equal(t, matchDocs, validCount, "(FIELD tok, FIELD bucket) baseline must equal matchDocs")

	// Run the held query in a goroutine.
	var (
		queryWG    sync.WaitGroup
		queryCount int
		queryErr   error
	)
	queryWG.Add(1)
	go func() {
		defer queryWG.Done()
		kwr := searchparams.KeywordRanking{
			Type:       "bm25",
			Properties: []string{fieldProp},
			Query:      phrase,
		}
		objs, _, err := bm25.BM25F(ctx, nil, schema.ClassName(className), 1000, kwr, additional.Properties{})
		// Record the instant the query has fully returned — by then its
		// deferred pins.release() has run, so this bounds "pin released".
		now := time.Now()
		releasedAt.Store(&now)
		queryCount = len(objs)
		queryErr = err
	}()

	// Wait until the query has pinned the FIELD bucket and is parked in the
	// hook. A timeout here would mean the hook never fired (path regression).
	select {
	case <-pinnedCh:
	case <-time.After(10 * time.Second):
		close(releaseCh)
		queryWG.Wait()
		return pinBucketDrainResult{validCount: validCount, deadlocked: true}
	}

	// The query now holds the pin on the OLD FIELD bucket. Perform the real
	// swap (FIELD prop's searchable pointer → WORD bucket) and, when not in
	// the re-fetch-asymmetry variant, shut the displaced old FIELD bucket
	// down. Shutdown MUST block on the in-flight pin until the query releases.
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(wordProp)

	oldBucket, err := shard.store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
	require.NoError(t, err)
	require.Same(t, fieldBucket, oldBucket, "swap must return the displaced old FIELD bucket")

	var (
		shutdownStartedAt time.Time
		shutdownAtPtr     atomic.Pointer[time.Time] // nil until Shutdown returns
		shutdownWG        sync.WaitGroup
	)
	if !forceRefetch {
		shutdownWG.Add(1)
		go func() {
			defer shutdownWG.Done()
			shutdownStartedAt = time.Now()
			require.NoError(t, oldBucket.Shutdown(ctx))
			now := time.Now()
			shutdownAtPtr.Store(&now)
		}()

		// Give the Shutdown goroutine a head start so it is provably parked on
		// lifetimeLock.Lock (draining) BEFORE we release the query. If the
		// drain were broken, Shutdown would race ahead and free the mmap here,
		// while the held query is about to read it — caught by -race / a
		// crash. With the drain, Shutdown blocks and cannot complete yet.
		time.Sleep(50 * time.Millisecond)
		require.Nil(t, shutdownAtPtr.Load(),
			"DRAIN VIOLATED: old bucket Shutdown completed while the query still held its pin")
	}

	// Release the held query. It proceeds to lookup using the PINNED old FIELD
	// bucket (WITH FIX) — finding the phrase docs — then returns and releases
	// the pin, unblocking the draining Shutdown.
	close(releaseCh)
	queryWG.Wait()

	if !forceRefetch {
		shutdownWG.Wait()
	}

	rp := releasedAt.Load()
	res := pinBucketDrainResult{
		validCount:        validCount,
		queryCount:        queryCount,
		queryErr:          queryErr,
		shutdownStartedAt: shutdownStartedAt,
	}
	if rp != nil {
		res.queryReleasedAt = *rp
	}
	if sp := shutdownAtPtr.Load(); sp != nil {
		res.shutdownAt = *sp
	}
	return res
}
