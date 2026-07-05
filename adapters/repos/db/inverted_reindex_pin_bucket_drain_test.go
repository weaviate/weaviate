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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// TestPinBucketDrain_LookupHoldsPinAcrossSwapShutdown proves the
// lookup-time half of the fix: pre-fix, createTerm / createBlockTerm
// RE-FETCHED the searchable bucket by name, so a swap+Shutdown landing in
// the prop-discovery→lookup gap gave the query the post-swap bucket (wrong
// count) or a freed mmap (UAF). With the pin threaded through, lookup uses
// the exact bucket discovery pinned, and Shutdown drains the pin first.
//
// It drives the REAL query path (BM25F → wandBlock → createBlockTerm),
// holds it in that gap via a test hook while a real SwapBucketPointer +
// oldBucket.Shutdown runs concurrently, and asserts (a) the held query
// returns the pre-swap count, (b) Shutdown does not return until the pin
// releases (timestamp-ordered), (c) no deadlock, race-clean. The asymmetry
// is proven by TestPinBucketDrain_OldRefetchYieldsWrongCount.
func TestPinBucketDrain_LookupHoldsPinAcrossSwapShutdown(t *testing.T) {
	res := runPinBucketDrainProof(t, false /* forceRefetch */)

	require.False(t, res.deadlocked, "the proof must not deadlock")
	require.Equal(t, res.validCount, res.queryCount,
		"WITH FIX: the held query must return the pinned (pre-swap FIELD) bucket's count, never the post-swap WORD view")
	require.NoError(t, res.queryErr, "the held query must not error")

	// DRAIN, proven by ordering: pin released strictly before Shutdown returned.
	require.False(t, res.shutdownAt.IsZero(), "old bucket Shutdown must have completed")
	require.False(t, res.queryReleasedAt.IsZero(), "the query must have released its pin")
	require.True(t, res.shutdownAt.After(res.queryReleasedAt) || res.shutdownAt.Equal(res.queryReleasedAt),
		"DRAIN VIOLATED: old bucket Shutdown returned at %s BEFORE the pinned query released at %s — Shutdown did not drain the in-flight pin",
		res.shutdownAt.Format(time.StampNano), res.queryReleasedAt.Format(time.StampNano))

	require.True(t, res.shutdownStartedAt.Before(res.shutdownAt) || res.shutdownStartedAt.Equal(res.shutdownAt),
		"sanity: Shutdown must start before it returns")
	t.Logf("WITH FIX: queryCount=%d (valid=%d), pin released at %s, shutdown started %s, shutdown returned %s (blocked %s)",
		res.queryCount, res.validCount,
		res.queryReleasedAt.Format(time.StampMicro),
		res.shutdownStartedAt.Format(time.StampMicro),
		res.shutdownAt.Format(time.StampMicro),
		res.shutdownAt.Sub(res.shutdownStartedAt))
}

// TestPinBucketDrain_OldRefetchYieldsWrongCount is the asymmetry run: with
// lookup forced back to re-fetch-by-name, the SAME held query misses → 0.
// Deliberately does NOT shut the old bucket down, so the wrong-count signal
// is observed cleanly without depending on use-after-free timing.
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

// runPinBucketDrainProof drives one real BM25F query held in the
// prop-discovery→lookup gap while a concurrent goroutine swaps the FIELD
// prop's searchable pointer to the WORD bucket and (when !forceRefetch)
// shuts the displaced old FIELD bucket down.
func runPinBucketDrainProof(t *testing.T, forceRefetch bool) pinBucketDrainResult {
	ctx := testCtx()

	fx := setupTwoTokenizationShard(t, ctx, "PinDrainRetok")
	shard, idx := fx.shard, fx.idx
	fieldBucket := fx.fieldBucket
	className, fieldProp, wordProp := fx.className, fx.fieldProp, fx.wordProp
	phrase, matchDocs := fx.phrase, fx.matchDocs

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
			// Pins held: signal the swap goroutine, park until released.
			close(pinnedCh)
			<-releaseCh
		})

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

	// A timeout here means the hook never fired (path regression).
	select {
	case <-pinnedCh:
	case <-time.After(10 * time.Second):
		close(releaseCh)
		queryWG.Wait()
		return pinBucketDrainResult{validCount: validCount, deadlocked: true}
	}

	// Query parked holding the pin on the OLD FIELD bucket: perform the
	// real swap (and, unless in the re-fetch variant, the Shutdown).
	fieldBucketName := helpers.BucketSearchableFromPropNameLSM(fieldProp)
	wordBucketName := helpers.BucketSearchableFromPropNameLSM(wordProp)

	oldBucket, err := shard.store.SwapBucketPointer(ctx, fieldBucketName, wordBucketName)
	require.NoError(t, err)
	require.Same(t, fieldBucket, oldBucket, "swap must return the displaced old FIELD bucket")

	var (
		shutdownStartedAt time.Time
		shutdownStarted   atomic.Bool
		shutdownErr       error
		shutdownAtPtr     atomic.Pointer[time.Time] // nil until Shutdown returns
		shutdownWG        sync.WaitGroup
	)
	if !forceRefetch {
		shutdownWG.Add(1)
		go func() {
			defer shutdownWG.Done()
			shutdownStartedAt = time.Now()
			shutdownStarted.Store(true)
			shutdownErr = oldBucket.Shutdown(ctx)
			now := time.Now()
			shutdownAtPtr.Store(&now)
		}()

		// Head start so Shutdown is provably parked in the drain BEFORE we
		// release the query — a broken drain would complete here instead.
		// The started-flag check keeps the negative from passing vacuously
		// if the goroutine never got scheduled.
		require.Eventually(t, shutdownStarted.Load, 5*time.Second, time.Millisecond,
			"Shutdown goroutine must have started")
		time.Sleep(50 * time.Millisecond)
		require.Nil(t, shutdownAtPtr.Load(),
			"DRAIN VIOLATED: old bucket Shutdown completed while the query still held its pin")
	}

	// Release the query: it looks up via the PINNED old bucket, returns,
	// and its released pin unblocks the draining Shutdown.
	close(releaseCh)
	queryWG.Wait()

	if !forceRefetch {
		shutdownWG.Wait()
		require.NoError(t, shutdownErr, "old bucket Shutdown must succeed")
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
