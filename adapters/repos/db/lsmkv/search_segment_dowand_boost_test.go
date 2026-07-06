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
	"github.com/weaviate/weaviate/entities/schema"
)

// TestDoWandPropertyBoostMatchesBruteForce is the non-block-max sibling of
// TestBlockMaxWandPropertyBoostMatchesBruteForce: DoWand must return the same
// top-K as a full scan that scores every posting with the SAME formula.
//
// It guards the pivot in Terms.FindMinIDWand, which sums per-term upper bounds
// and compares them to worstDist (a real BM25 score carrying propertyBoost). A
// raw-idf sum under-counts boosted terms, sinks the pivot too deep and drops
// genuine top-K docs — invisible at propertyBoost==1, corrupting recall above it.
func TestDoWandPropertyBoostMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}

	const nDocs, vocab, termsPerDoc = 4000, 1200, 25
	rng := rand.New(rand.NewSource(11))
	zipf := rand.NewZipf(rng, 1.07, 1.0, uint64(vocab-1))
	keyFor := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }

	// build a posting list per term id; docs are appended in ascending id order,
	// so each Data slice is already sorted as DoWand requires.
	postings := make(map[uint64][]terms.DocPointerWithScore)
	var totalLen float64
	for d := 0; d < nDocs; d++ {
		pl := float32(20 + rng.Intn(480))
		totalLen += float64(pl)
		seen := make(map[uint64]uint32, termsPerDoc)
		for k := 0; k < termsPerDoc; k++ {
			seen[zipf.Uint64()]++
		}
		for tid, tf := range seen {
			postings[tid] = append(postings[tid], terms.DocPointerWithScore{
				Id: uint64(d), Frequency: float32(tf), PropLength: pl,
			})
		}
	}
	avgPropLen := totalLen / float64(nDocs)

	idfOf := func(tid uint64) float64 {
		n := float64(len(postings[tid]))
		return math.Log(1 + (float64(nDocs)-n+0.5)/(n+0.5))
	}

	// rank by document frequency; head terms have the long posting lists that
	// make the pivot skip, the regime where the FindMinIDWand bug bites.
	ranked := make([]uint64, 0, len(postings))
	for tid := range postings {
		ranked = append(ranked, tid)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if len(postings[ranked[i]]) != len(postings[ranked[j]]) {
			return len(postings[ranked[i]]) > len(postings[ranked[j]])
		}
		return ranked[i] < ranked[j]
	})
	require.Greater(t, len(postings[ranked[0]]), 200, "head term posting list too short to force pivoting")

	query := make([]uint64, 0, 40)
	for i := 0; i < 40 && i < len(ranked); i++ {
		query = append(query, ranked[i])
	}

	bruteForce := func(boost float32) map[uint64]float64 {
		scores := map[uint64]float64{}
		for _, tid := range query {
			idf := idfOf(tid)
			for _, dp := range postings[tid] {
				freq := float64(dp.Frequency)
				tf := freq / (freq + bm25.K1*(1-bm25.B+bm25.B*float64(dp.PropLength)/avgPropLen))
				scores[dp.Id] += tf * idf * float64(boost)
			}
		}
		return scores
	}

	runWand := func(boost float32, limit int) map[uint64]float32 {
		ts := make([]terms.TermInterface, 0, len(query))
		for i, tid := range query {
			tr := terms.NewTerm(keyFor(tid), i, boost, bm25)
			tr.Data = postings[tid] // read-only in DoWand; never mutated
			tr.SetIdf(idfOf(tid))
			tr.SetPosPointer(0)
			tr.SetIdPointer(tr.Data[0].Id)
			ts = append(ts, tr)
		}
		tt := &terms.Terms{T: ts, Count: len(query)}
		h := DoWand(ctx, limit, tt, avgPropLen, false, 1, logger)
		out := map[uint64]float32{}
		for h != nil && h.Len() > 0 {
			it := h.Pop()
			out[it.ID] = it.Dist
		}
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
			brute := bruteForce(tc.boost)
			wand := runWand(tc.boost, tc.limit)

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
