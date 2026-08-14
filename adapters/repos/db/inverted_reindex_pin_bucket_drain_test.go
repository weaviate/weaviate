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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// Pins the read-time race: a swap+Shutdown landing in the discovery→lookup
// gap must not hand the query a post-swap bucket (wrong count) or a freed mmap (UAF).
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

// Asymmetry check: lookup forced back to re-fetch-by-name must miss (→0).
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
// prop's bucket pointer and (when !forceRefetch) shuts the old bucket down.
func runPinBucketDrainProof(t *testing.T, forceRefetch bool) pinBucketDrainResult {
	ctx := testCtx()

	fx := setupTwoTokenizationShard(t, ctx, "PinDrainRetok")
	shard, idx := fx.shard, fx.idx
	fieldBucket := fx.fieldBucket
	className, fieldProp, wordProp := fx.className, fx.fieldProp, fx.wordProp
	phrase, matchDocs := fx.phrase, fx.matchDocs

	// Drives BM25Searcher through its production resolver seams only; no
	// test-only knobs on the searcher. Omitting the pinning resolver
	// reproduces the old re-fetch-by-name behavior.
	bm25Config := idx.GetInvertedIndexConfig().BM25
	logger := idx.logger.WithFields(logrus.Fields{"class": className, "shard": shard.name})

	var (
		pinnedCh   = make(chan struct{}) // closed once the query holds its pin(s)
		releaseCh  = make(chan struct{}) // closed by the test to let the held query proceed
		releasedAt atomic.Pointer[time.Time]
		parkOnce   sync.Once
	)
	// park freezes the query in the prop-discovery→lookup gap with its pin(s)
	// already held; sync.Once keeps it one-shot across resolver re-entry.
	park := func() {
		parkOnce.Do(func() {
			close(pinnedCh)
			<-releaseCh
		})
	}

	bm25 := inverted.NewBM25Searcher(bm25Config, shard.store,
		idx.getSchema.ReadOnlyClass, shard.propertyIndices, idx.classSearcher,
		idx.getStopwordProvider(), shard.GetPropertyLengthTracker(), logger,
		shard.versioner.Version())

	if forceRefetch {
		// No pinning resolver: lookup re-fetches by name and lands on the
		// post-swap WORD bucket. Park in the tokenization resolver, the only
		// seam left on this path — tok is still captured FIELD-way pre-swap.
		bm25.WithTokenizationResolver(func(propName, schemaTok string) string {
			tok := shard.TokenizationFor(propName, schemaTok)
			park()
			return tok
		})
	} else {
		// The pinning resolver takes the pin (held across the concurrent
		// swap + old-bucket Shutdown, forcing the drain) then parks.
		bm25.WithTokenizationResolver(shard.TokenizationFor).
			WithSearchableBucketPinningResolver(
				func(propName, schemaTok string) (string, *lsmkv.Bucket, func()) {
					tok, bkt, release := shard.PinTokenizationAndSearchableBucket(propName, schemaTok)
					park()
					return tok, bkt, release
				})
	}

	validCount := lookupCount(ctx, models.PropertyTokenizationField, fieldBucket, className, phrase)
	require.Equal(t, matchDocs, validCount, "(FIELD tok, FIELD bucket) baseline must equal matchDocs")

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
		// By now the deferred pins.release() has run — bounds "pin released".
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

		// Shutdown must be parked in the drain BEFORE the query is released —
		// a broken drain would complete here instead.
		require.Eventually(t, shutdownStarted.Load, 5*time.Second, time.Millisecond,
			"Shutdown goroutine must have started")
		time.Sleep(50 * time.Millisecond)
		require.Nil(t, shutdownAtPtr.Load(),
			"DRAIN VIOLATED: old bucket Shutdown completed while the query still held its pin")
	}

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
