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
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/schema"
)

// zipfCorpus builds a deterministic zipf-distributed corpus: one posting list
// per term (appended in ascending doc-id order, as WAND requires) and the mean
// property length. Shared by the block-max and non-block boost tests.
func zipfCorpus(seed int64, nDocs, vocab, termsPerDoc int) (map[uint64][]terms.DocPointerWithScore, float64) {
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, 1.07, 1.0, uint64(vocab-1))
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
	return postings, totalLen / float64(nDocs)
}

// topTermsByDF returns the n most frequent term ids — the long posting lists
// that make the WAND pivot skip, the regime the boost fixes touch.
func topTermsByDF(postings map[uint64][]terms.DocPointerWithScore, n int) []uint64 {
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
	return ranked[:n]
}

func idfOf(nDocs, docFreq int) float64 {
	n := float64(docFreq)
	return math.Log(1 + (float64(nDocs)-n+0.5)/(n+0.5))
}

// requireWandTopK asserts a WAND result matches the brute-force top-K. Every
// returned doc must carry the brute score and rank within a boundary band of the
// K-th best; every doc firmly above that band must be returned. The band absorbs
// block-max WAND's inherent boundary approximation (~0.2% of score, from per-
// block bounds); a bound that drops propertyBoost mis-prunes docs far above the
// cutoff (well beyond the band), so this still fails on it.
func requireWandTopK(t *testing.T, brute map[uint64]float64, wand map[uint64]float32, limit int) {
	t.Helper()
	n := limit
	if len(brute) < n {
		n = len(brute)
	}
	require.Lenf(t, wand, n, "want %d results, got %d", n, len(wand))

	scores := make([]float64, 0, len(brute))
	for _, s := range brute {
		scores = append(scores, s)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(scores)))
	kth := math.Inf(-1)
	if n > 0 && len(scores) >= n {
		kth = scores[n-1]
	}
	band := 0.01 * (1 + math.Abs(kth))

	for id, dist := range wand {
		b, ok := brute[id]
		require.Truef(t, ok, "WAND returned doc %d that has no query term", id)
		require.InDeltaf(t, b, float64(dist), 1e-3*(1+math.Abs(b)), "doc %d score mismatch", id)
		require.GreaterOrEqualf(t, b, kth-band, "doc %d (%.4f) ranks firmly below K-th best (%.4f): included a non-top-K doc", id, b, kth)
	}
	for id, b := range brute {
		if b > kth+band {
			_, ok := wand[id]
			require.Truef(t, ok, "WAND dropped firmly-top-K doc %d (%.4f > K-th %.4f): recall bug", id, b, kth)
		}
	}
}

// TestDoWandPropertyBoost exercises the non-block WAND pivot
// (Terms.FindMinIDWand) for propertyBoost consistency; see requireWandTopK.
// Fails on a bare-idf pivot bound at boost > 1.
func TestDoWandPropertyBoost(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}

	const nDocs, limit = 4000, 10
	postings, avgPropLen := zipfCorpus(11, nDocs, 800, 20)
	query := topTermsByDF(postings, 40)

	bruteForce := func(boost float32) map[uint64]float64 {
		scores := map[uint64]float64{}
		for _, tid := range query {
			idf := idfOf(nDocs, len(postings[tid]))
			for _, dp := range postings[tid] {
				freq := float64(dp.Frequency)
				tf := freq / (freq + bm25.K1*(1-bm25.B+bm25.B*float64(dp.PropLength)/avgPropLen))
				scores[dp.Id] += tf * idf * float64(boost)
			}
		}
		return scores
	}

	runWand := func(boost float32) map[uint64]float32 {
		ts := make([]terms.TermInterface, 0, len(query))
		for i, tid := range query {
			tr := terms.NewTerm(strconv.FormatUint(tid, 10), i, boost, bm25)
			tr.Data = postings[tid] // read-only in DoWand
			tr.SetIdf(idfOf(nDocs, len(postings[tid])))
			tr.SetPosPointer(0)
			tr.SetIdPointer(tr.Data[0].Id)
			ts = append(ts, tr)
		}
		h := DoWand(ctx, limit, &terms.Terms{T: ts, Count: len(query)}, avgPropLen, false, 1, logger)
		out := map[uint64]float32{}
		for h != nil && h.Len() > 0 {
			it := h.Pop()
			out[it.ID] = it.Dist
		}
		return out
	}

	for _, boost := range []float32{1, 5} {
		t.Run(strconv.FormatFloat(float64(boost), 'g', -1, 32), func(t *testing.T) {
			requireWandTopK(t, bruteForce(boost), runWand(boost), limit)
		})
	}
}
