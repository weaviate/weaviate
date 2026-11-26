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

//go:build postingedgebench

package postingedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The tests in this file left search_test.go: what they measure needs a
// corpus of thousands of vectors to mean anything, and under -race with
// coverage it costs minutes. TestSearchRecallGrowsWithEF keeps a small version
// there. The logic the other two measure is checked in the package by
// TestSourceMarginCutsTheSourcesBeyondTheThreshold (the cut, member by member)
// and TestLoadScansPostingCodesAndRanksWithSidecarCodes (which codes scan and
// which rank).

// The index must find the true neighbors through its edges, starting from a
// single posting, and ef must buy recall. The corpus has no cluster structure
// at all (one blob around a shared offset, the anisotropic shape of real
// embeddings), so the neighbors of a query are spread over many postings and
// nothing but the edges can lead to them.
func TestSearchRecallGrowsWithEFAtScale(t *testing.T) {
	const k = 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(8200, 32, 1, 71)
	vectors, queries := all[:8000], all[8000:]
	truth := normalizedCopy(vectors)
	normalizedQueries := normalizedCopy(queries)

	index := buildTestIndex(t, vectors, provider, smallConfig())
	memory, ok := index.MemoryStats()
	require.True(t, ok)
	require.Greater(t, memory.Postings, 100, "the corpus must be spread over many postings")

	recallWith := func(ef int) float64 {
		uc := smallConfig()
		uc.EF = ef
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		return recallAt(t, index, truth, normalizedQueries, k, provider)
	}

	atK := recallWith(k)
	at4K := recallWith(4 * k)
	at20K := recallWith(20 * k)
	t.Logf("recall@%d: ef=k %.3f, ef=4k %.3f, ef=20k %.3f", k, atK, at4K, at20K)

	// Measured 0.83 / 0.97 / 0.999; the bounds leave room for the graph
	// over the centroids, which is built concurrently and so differs
	// slightly from run to run.
	assert.Greater(t, atK, 0.72, "the natural stop at ef=k must already find most neighbors")
	assert.Greater(t, at4K, 0.93)
	assert.Greater(t, at20K, 0.985)
	assert.Greater(t, at4K, atK, "ef must buy recall")
}

// A margin trades work for recall. Wide enough it changes nothing; tighter,
// it must save endpoint scoring, and what it costs in recall must stay small
// as long as it is not absurdly tight.
func TestSourceMarginSavesWorkAtScale(t *testing.T) {
	const k = 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(8200, 32, 1, 71)
	vectors, queries := all[:8000], all[8000:]
	truth := normalizedCopy(vectors)
	normalizedQueries := normalizedCopy(queries)

	base := smallConfig()
	base.EF = 4 * k
	index := buildTestIndex(t, vectors, provider, base)

	type outcome struct {
		results   [][]uint64
		endpoints int
		cut       int
		recall    float64
	}
	run := func(margin float32) outcome {
		uc := base
		uc.SourceMargin = margin
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		var out outcome
		for _, query := range queries {
			ids, _, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
			require.NoError(t, err)
			out.results = append(out.results, ids)
			out.endpoints += stats.EndpointsScored
			out.cut += stats.SourcesCut
			require.Equal(t, stats.EdgesFollowed,
				stats.EndpointsInLoaded+stats.EndpointsRepeated()+stats.EndpointsScored)
		}
		out.recall = recallAt(t, index, truth, normalizedQueries, k, provider)
		return out
	}

	none := run(0)
	wide := run(10)
	tight := run(0.02)
	t.Logf("endpoints scored: no margin %d, margin 0.02 %d (%d sources cut); recall %.3f vs %.3f",
		none.endpoints, tight.endpoints, tight.cut, none.recall, tight.recall)

	assert.Zero(t, none.cut)
	assert.Zero(t, wide.cut, "a margin wider than any distance cuts nothing")
	assert.Equal(t, none.results, wide.results)
	assert.Equal(t, none.endpoints, wide.endpoints)

	assert.Positive(t, tight.cut)
	assert.Less(t, tight.endpoints, none.endpoints*3/4, "the cut must save a good part of the scoring")
	assert.Greater(t, tight.recall, none.recall-0.05, "and must not wreck the recall")
}

// Short scan codes next to full sidecar codes keep what the full codes are
// there for: the postings a search loads are picked by sidecar estimates, so
// for the same number of postings loaded it covers what an index with full
// codes covers, far more than one with short codes for everything.
//
// What changes is when the search stops. The stop threshold is the ef-th best
// scan estimate. Scan estimates are noisier than the sidecar estimates the
// postings are ranked by, so the best of them fall further below the true
// distances, and no posting looks as close: at the same ef the search stops
// earlier, and equal work takes a larger ef.
func TestShortScanCodesRouteLikeFullCodesAtScale(t *testing.T) {
	const dims, n, k = 512, 6000, 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(n+40, dims, 1, 531)
	vectors, queries := all[:n], all[n:]
	truth := normalizedCopy(vectors)
	normalizedQueries := normalizedCopy(queries)

	type outcome struct{ loads, recall float64 }
	build := func(codeBits, scanCodeBits int) func(ef int) outcome {
		uc := smallConfig()
		uc.CodeBits, uc.ScanCodeBits = codeBits, scanCodeBits
		// Every scanned vector is rescored, so the recall is the share of
		// the neighbors that live in the postings the search loaded.
		uc.RescoreLimit = n
		index := buildTestIndex(t, vectors, provider, uc)
		return func(ef int) outcome {
			uc.EF = ef
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			loads := 0
			for _, query := range queries {
				_, _, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
				require.NoError(t, err)
				loads += stats.PostingsLoaded
			}
			return outcome{
				loads:  float64(loads) / float64(len(queries)),
				recall: recallAt(t, index, truth, normalizedQueries, k, provider),
			}
		}
	}
	full, short, apart := build(0, 0), build(128, 0), build(0, 128)

	fullAt40, shortAt40 := full(40), short(40)
	apartAt40, apartAt160 := apart(40), apart(160)
	t.Logf("ef=40: full codes %.1f loads, recall %.3f; 128-bit codes %.1f loads, recall %.3f", fullAt40.loads,
		fullAt40.recall, shortAt40.loads, shortAt40.recall)
	t.Logf("128-bit scan codes, full sidecar codes: ef=40 %.1f loads, recall %.3f; ef=160 %.1f loads, recall %.3f",
		apartAt40.loads, apartAt40.recall, apartAt160.loads, apartAt160.recall)

	// Measured: 37 loads and recall 0.74 with full codes, 37 loads and 0.39
	// with 128-bit codes; apart, 14 loads at ef=40, and 35 loads and 0.73 at
	// ef=160.
	assert.Less(t, apartAt40.loads, 0.7*fullAt40.loads, "at the same ef the search stops earlier")

	assert.Less(t, apartAt160.loads, 1.15*fullAt40.loads, "the work compared must be about the same")
	assert.Less(t, apartAt160.loads, 1.15*shortAt40.loads)
	assert.Greater(t, apartAt160.recall, fullAt40.recall-0.08, "for the same loads, the cover of full codes")
	assert.Greater(t, apartAt160.recall, shortAt40.recall+0.2, "and far more than short codes find")
}
