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

// TestBlockMaxWandMatchesBruteForce is an exact differential for the WAND
// traversal: DoBlockMaxWand must return the same top-K (ids and scores) as a
// naive full scan that scores every posting with the SAME Score() code. The
// reference shares the scoring path, so the only thing under test is the
// candidate traversal — the pivot scan, pruning, and the prune/matched-branch
// re-sort (sortByID / reinsertRight) gated on needFullSort.
//
// The corpus is sized so the query terms span many blocks (>BLOCK_SIZE docs
// each): a multi-term query then drives repeated multi-term shallow advances
// that step blocks, the regime where the prune branch fires with needFullSort
// toggling. A mis-sort there would corrupt the next ascending pivot scan and
// surface here as a dropped or mis-scored top-K doc — which the long-query
// termination watchdog (the only other test in this regime) cannot see.
func TestBlockMaxWandMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	// one segment: keep the whole corpus in a single flushed segment so a doc's
	// postings are never split across segments and the brute force is unambiguous.
	bucket.SetMemtableThreshold(1 << 30)

	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const nDocs, vocab, termsPerDoc = 20000, 6000, 40
	rng := rand.New(rand.NewSource(42))
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

	// rank terms by document frequency; the head terms are the multi-block ones
	// that make the prune branch work.
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
		"corpus produced no multi-block term (max df %d <= BLOCK_SIZE %d); cannot exercise needFullSort",
		ranked[0].n, terms.BLOCK_SIZE)

	commonQuery := make([]string, 0, 80)
	for i := 0; i < 80 && i < len(ranked); i++ {
		commonQuery = append(commonQuery, ranked[i].key)
	}
	// common head + a spread of mid-frequency terms: mixes long multi-block
	// posting lists with short ones, so the pivot moves unevenly across terms.
	mixedQuery := make([]string, 0, 80)
	mixedQuery = append(mixedQuery, commonQuery[:20]...)
	for i := 200; i < len(ranked) && len(mixedQuery) < 80; i += 25 {
		mixedQuery = append(mixedQuery, ranked[i].key)
	}

	boostsFor := func(q []string) []int {
		b := make([]int, len(q))
		for i := range b {
			b[i] = 1
		}
		return b
	}

	// bruteForce scores every posting of every query term with the production
	// Score(), summed per doc — the exact ranking WAND must reproduce.
	bruteForce := func(t *testing.T, query []string) map[uint64]float64 {
		view := bucket.GetConsistentView()
		defer view.ReleaseView()
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
			query, "", 1, boostsFor(query), bm25)
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

	runWand := func(t *testing.T, query []string, limit int) map[uint64]float32 {
		view := bucket.GetConsistentView()
		defer view.ReleaseView()
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
			query, "", 1, boostsFor(query), bm25)
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
		query []string
		limit int
	}{
		{"common/k10", commonQuery, 10},
		{"common/k100", commonQuery, 100},
		{"common/k500", commonQuery, 500},
		{"mixed/k10", mixedQuery, 10},
		{"mixed/k100", mixedQuery, 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brute := bruteForce(t, tc.query)
			wand := runWand(t, tc.query, tc.limit)

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
			kth := math.Inf(-1) // smallest score a correct top-K may contain
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

	// Prove the search actually steps blocks (the needFullSort trigger). Without
	// this, a future corpus shrink could keep every term single-block, silently
	// reducing the differential above to the cheap reinsertRight path only.
	t.Run("steps_blocks_during_search", func(t *testing.T) {
		prev := collectBlockMetrics
		collectBlockMetrics = true
		defer func() { collectBlockMetrics = prev }()

		view := bucket.GetConsistentView()
		defer view.ReleaseView()
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
			commonQuery, "", 1, boostsFor(commonQuery), bm25)
		require.NoError(t, err)

		var segTerms []*SegmentBlockMax
		for _, g := range diskTerms {
			if len(g) > 0 {
				segTerms = g
				break
			}
		}
		require.NotEmpty(t, segTerms)

		blocksBefore := 0 // each term decoded its first block at construction
		for _, term := range segTerms {
			blocksBefore += int(term.Metrics.BlockCountDecodedDocIds)
		}
		h, err := DoBlockMaxWand(ctx, 10, segTerms, avgPropLen, false, len(commonQuery), 1, logger)
		require.NoError(t, err)
		for h != nil && h.Len() > 0 {
			h.Pop()
		}
		blocksAfter := 0
		for _, term := range segTerms {
			blocksAfter += int(term.Metrics.BlockCountDecodedDocIds)
		}
		require.Greaterf(t, blocksAfter, blocksBefore,
			"search decoded no new blocks (%d->%d): no block-stepping, so needFullSort never toggled",
			blocksBefore, blocksAfter)
	})
}
