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
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestBlockMaxAndCrossPropMatchesBruteForce is the differential for the
// cross-property AND traversal: DoBlockMaxAndCrossProp over MergedTerms must
// return the same top-K (ids and scores) as a naive scan that, with the SAME
// Score() code, keeps a doc iff every query token appears in at least one
// property and sums the per-(term,property) contributions. The brute force
// shares the scoring path, so the only thing under test is the merge + the
// document-at-a-time intersection across properties.
func TestBlockMaxAndCrossPropMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const nDocs, nProps, vocab, termsPerProp = 3000, 3, 300, 12
	rng := rand.New(rand.NewSource(7))
	zipf := rand.NewZipf(rng, 1.05, 1.0, uint64(vocab-1))
	keyFor := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }

	// one inverted bucket per property; one flushed segment each so a doc's
	// postings for a property never split across segments.
	buckets := make([]*Bucket, nProps)
	for p := 0; p < nProps; p++ {
		dir := t.TempDir()
		bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted))
		require.NoError(t, err)
		bucket.SetMemtableThreshold(1 << 30)
		buckets[p] = bucket
	}
	t.Cleanup(func() {
		for _, bk := range buckets {
			require.NoError(t, bk.Shutdown(ctx))
		}
	})

	// document frequency per term across ALL properties — used to pick query
	// terms common enough that the cross-property AND has matches.
	globalDocCount := map[string]int{}
	for d := 0; d < nDocs; d++ {
		for p := 0; p < nProps; p++ {
			pl := float32(15 + rng.Intn(200))
			seen := make(map[uint64]uint32, termsPerProp)
			for k := 0; k < termsPerProp; k++ {
				seen[zipf.Uint64()]++
			}
			perDocSeen := map[string]struct{}{}
			for tid, tf := range seen {
				key := keyFor(tid)
				require.NoError(t, buckets[p].MapSet([]byte(key),
					NewMapPairFromDocIdAndTf(uint64(d), float32(tf), pl, false)))
				perDocSeen[key] = struct{}{}
			}
			for key := range perDocSeen {
				globalDocCount[key]++
			}
		}
	}
	for _, bk := range buckets {
		require.NoError(t, bk.FlushAndSwitch())
	}

	ranked := make([]string, 0, len(globalDocCount))
	for k := range globalDocCount {
		ranked = append(ranked, k)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if globalDocCount[ranked[i]] != globalDocCount[ranked[j]] {
			return globalDocCount[ranked[i]] > globalDocCount[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})

	boostsFor := func(q []string) []int {
		b := make([]int, len(q))
		for i := range b {
			b[i] = 1
		}
		return b
	}

	// diskTermsAcrossProps builds allResults ([property][segment][term]) and the
	// consistent views that must stay open for the duration of a search. The
	// caller must call release() once it is done iterating the terms.
	diskTermsAcrossProps := func(t *testing.T, query []string) ([][][]*SegmentBlockMax, func()) {
		allResults := make([][][]*SegmentBlockMax, 0, nProps)
		views := make([]BucketConsistentView, 0, nProps)
		for _, bk := range buckets {
			view := bk.GetConsistentView()
			views = append(views, view)
			dt, _, _, err := bk.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
				query, "", 1, boostsFor(query), bm25)
			require.NoError(t, err)
			allResults = append(allResults, dt)
		}
		return allResults, func() {
			for _, v := range views {
				v.ReleaseView()
			}
		}
	}

	// bruteForce keeps a doc iff it has every query term in at least one property,
	// scoring it as the sum over terms of the per-property contributions (one
	// source per (doc, term, property) given the single-segment corpus).
	bruteForce := func(t *testing.T, query []string) map[uint64]float64 {
		allResults, release := diskTermsAcrossProps(t, query)
		defer release()

		perTermScore := make([]map[uint64]float64, len(query))
		for i := range perTermScore {
			perTermScore[i] = map[uint64]float64{}
		}
		for _, perProperty := range allResults {
			for _, segTerms := range perProperty {
				for _, term := range segTerms {
					ti := term.QueryTermIndex()
					for !term.Exhausted() {
						id, s, _ := term.Score(0, false)
						perTermScore[ti][id] += s
						term.Advance()
					}
				}
			}
		}

		// a query term with no source anywhere makes the AND unsatisfiable
		for i := range query {
			if len(perTermScore[i]) == 0 {
				return map[uint64]float64{}
			}
		}

		out := map[uint64]float64{}
		for id, s0 := range perTermScore[0] {
			total := s0
			matched := true
			for i := 1; i < len(query); i++ {
				s, ok := perTermScore[i][id]
				if !ok {
					matched = false
					break
				}
				total += s
			}
			if matched {
				out[id] = total
			}
		}
		return out
	}

	runCrossProp := func(t *testing.T, query []string, limit int) map[uint64]float32 {
		allResults, release := diskTermsAcrossProps(t, query)
		defer release()

		mergedTerms, ok := BuildCrossPropMergedTerms(allResults, len(query))
		out := map[uint64]float32{}
		if !ok {
			return out
		}
		h := DoBlockMaxAndCrossProp(ctx, limit, mergedTerms, 0, false, len(query), logger)
		for h != nil && h.Len() > 0 {
			it := h.Pop()
			out[it.ID] = it.Dist
		}
		return out
	}

	common3 := []string{ranked[0], ranked[1], ranked[2]}
	mixed := []string{ranked[0], ranked[5], ranked[20]}
	missing := []string{ranked[0], ranked[1], keyFor(uint64(vocab + 1))} // last term in no doc

	cases := []struct {
		name  string
		query []string
		limit int
	}{
		{"common3/k10", common3, 10},
		{"common3/k100", common3, 100},
		{"common3/all", common3, 0},
		{"mixed/k10", mixed, 10},
		{"mixed/k50", mixed, 50},
		{"missingTerm/k10", missing, 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			brute := bruteForce(t, tc.query)

			limit := tc.limit
			if limit == 0 {
				limit = len(brute) + 1
			}
			cross := runCrossProp(t, tc.query, limit)

			expectedN := limit
			if len(brute) < expectedN {
				expectedN = len(brute)
			}
			require.Lenf(t, cross, expectedN, "cross-prop AND returned %d results, want %d", len(cross), expectedN)

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

			for id, dist := range cross {
				b, ok := brute[id]
				require.Truef(t, ok, "cross-prop returned doc %d that does not satisfy the AND", id)
				require.InDeltaf(t, b, float64(dist), tol(b),
					"doc %d score mismatch: brute %.6f vs cross %.6f", id, b, float64(dist))
				require.GreaterOrEqualf(t, b, kth-tol(kth),
					"doc %d (score %.6f) ranks below the K-th best (%.6f): included a non-top-K doc", id, b, kth)
			}
			for id, b := range brute {
				if b > kth+tol(kth) {
					_, ok := cross[id]
					require.Truef(t, ok,
						"cross-prop dropped genuine top-K doc %d (score %.6f > K-th best %.6f): recall bug", id, b, kth)
				}
			}
		})
	}

	// A term present in no property at all must yield zero matches.
	t.Run("unsatisfiable_term_empty", func(t *testing.T) {
		cross := runCrossProp(t, missing, 100)
		require.Empty(t, cross)
	})
}
