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

//go:build integrationTest

package lsmkv

import (
	"context"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestBlockMaxWandPropertyBoostMatchesBruteForce is the differential from
// TestBlockMaxWandMatchesBruteForce, but with a non-default propertyBoost.
//
// It guards the else-branch "heaviest term to advance" selection in
// DoBlockMaxWand, which seeds maxWeight from the pivot term. That seed only
// steers which term advances (a performance heuristic) and never the returned
// set, so a differential can only catch a selection change that also corrupts
// the traversal — a dropped or mis-scored top-K doc. The boost is uniform
// across every term of a call (one call == one property, see
// bm25_searcher_block.go), so idf and idf*propertyBoost rank the terms
// identically; this pins that boosted queries stay correct regardless.
func TestBlockMaxWandPropertyBoostMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	// single flushed segment so a doc's postings are never split and the brute
	// force is unambiguous.
	bucket.SetMemtableThreshold(1 << 30)

	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const nDocs, vocab, termsPerDoc = 8000, 3000, 30
	rng := rand.New(rand.NewSource(7))
	zipf := rand.NewZipf(rng, 1.07, 1.0, uint64(vocab-1))
	keyFor := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }

	termDocCount := map[string]int{}
	for d := 0; d < nDocs; d++ {
		pl := float32(20 + rng.Intn(480))
		seen := make(map[uint64]uint32, termsPerDoc)
		for k := 0; k < termsPerDoc; k++ {
			seen[zipf.Uint64()]++
		}
		for tid, tf := range seen {
			key := keyFor(tid)
			require.NoError(t, bucket.MapSet([]byte(key),
				NewMapPairFromDocIdAndTf(uint64(d), float32(tf), pl, false)))
			termDocCount[key]++
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())
	avgPropLen, _ := bucket.GetAveragePropertyLength()

	type termFreq struct {
		key string
		n   int
	}
	ranked := make([]termFreq, 0, len(termDocCount))
	for k, n := range termDocCount {
		ranked = append(ranked, termFreq{k, n})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].n != ranked[j].n {
			return ranked[i].n > ranked[j].n
		}
		return ranked[i].key < ranked[j].key
	})
	require.Greaterf(t, ranked[0].n, terms.BLOCK_SIZE,
		"corpus produced no multi-block term (max df %d <= BLOCK_SIZE %d); else branch never steps blocks",
		ranked[0].n, terms.BLOCK_SIZE)

	query := make([]string, 0, 60)
	for i := 0; i < 60 && i < len(ranked); i++ {
		query = append(query, ranked[i].key)
	}
	dupBoosts := make([]int, len(query))
	for i := range dupBoosts {
		dupBoosts[i] = 1
	}

	bruteForce := func(t *testing.T, boost float32) map[uint64]float64 {
		view := bucket.GetConsistentView()
		defer view.ReleaseView()
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
			query, "", boost, dupBoosts, bm25)
		require.NoError(t, err)
		scores := map[uint64]float64{}
		for _, segTerms := range diskTerms {
			for _, term := range segTerms {
				for !term.Exhausted() {
					id, s, _ := term.Score(avgPropLen, false)
					scores[id] += s
					term.Advance()
				}
			}
		}
		return scores
	}

	runWand := func(t *testing.T, boost float32, limit int) map[uint64]float32 {
		view := bucket.GetConsistentView()
		defer view.ReleaseView()
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
			query, "", boost, dupBoosts, bm25)
		require.NoError(t, err)
		out := map[uint64]float32{}
		nonEmpty := 0
		for _, segTerms := range diskTerms {
			if len(segTerms) == 0 {
				continue
			}
			nonEmpty++
			h, err := DoBlockMaxWand(ctx, limit, segTerms, avgPropLen, false, len(query), 1, logger)
			require.NoError(t, err)
			for h != nil && h.Len() > 0 {
				it := h.Pop()
				out[it.ID] = it.Dist
			}
		}
		require.Equalf(t, 1, nonEmpty, "expected a single flushed segment, got %d non-empty term groups", nonEmpty)
		return out
	}

	cases := []struct {
		name  string
		boost float32
		limit int
	}{
		{"boost0.5/k10", 0.5, 10},
		{"boost1.0/k100", 1.0, 100},
		{"boost2.5/k10", 2.5, 10},
		{"boost2.5/k100", 2.5, 100},
		{"boost7.0/k50", 7.0, 50},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brute := bruteForce(t, tc.boost)
			wand := runWand(t, tc.boost, tc.limit)

			expectedN := tc.limit
			if len(brute) < expectedN {
				expectedN = len(brute)
			}
			require.Lenf(t, wand, expectedN, "WAND returned %d results, want %d", len(wand), expectedN)

			scores := make([]float64, 0, len(brute))
			for _, s := range brute {
				scores = append(scores, s)
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(scores)))
			kth := math.Inf(-1)
			if expectedN > 0 && len(scores) >= expectedN {
				kth = scores[expectedN-1]
			}
			tol := func(v float64) float64 { return 1e-3 * (1 + math.Abs(v)) }

			for id, dist := range wand {
				b, ok := brute[id]
				require.Truef(t, ok, "WAND returned doc %d that has no query term", id)
				require.InDeltaf(t, b, float64(dist), tol(b),
					"doc %d score mismatch: brute %.6f vs wand %.6f", id, b, float64(dist))
				require.GreaterOrEqualf(t, b, kth-tol(kth),
					"doc %d (score %.6f) ranks below the K-th best (%.6f): WAND included a non-top-K doc", id, b, kth)
			}
			for id, b := range brute {
				if b > kth+tol(kth) {
					_, ok := wand[id]
					require.Truef(t, ok,
						"WAND dropped genuine top-K doc %d (score %.6f > K-th best %.6f): recall bug", id, b, kth)
				}
			}
		})
	}
}
