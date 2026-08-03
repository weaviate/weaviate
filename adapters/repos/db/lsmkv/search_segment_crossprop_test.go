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
// property, taking per (doc, term, property) the MAX contribution across that
// property's sources and summing those maxima across properties. The brute
// force shares the scoring path, so the only thing under test is the merge +
// the document-at-a-time intersection across properties. The layout variants
// cover the production source shapes: one flushed segment per property,
// several disk segments per property, and disk segments plus the active
// memtable.
func TestBlockMaxAndCrossPropMatchesBruteForce(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const nDocs, nProps, vocab, termsPerProp = 3000, 3, 300, 12
	rng := rand.New(rand.NewSource(7))
	zipf := rand.NewZipf(rng, 1.05, 1.0, uint64(vocab-1))
	keyFor := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }

	// fixed corpus shared by every layout variant, so query-term selection and
	// expected results are identical across them
	type propDoc struct {
		pl  float32
		tfs map[uint64]uint32
	}
	corpus := make([][]propDoc, nDocs)
	// document frequency per term across ALL properties — used to pick query
	// terms common enough that the cross-property AND has matches.
	globalDocCount := map[string]int{}
	for d := 0; d < nDocs; d++ {
		corpus[d] = make([]propDoc, nProps)
		for p := 0; p < nProps; p++ {
			pl := float32(15 + rng.Intn(200))
			tfs := make(map[uint64]uint32, termsPerProp)
			for k := 0; k < termsPerProp; k++ {
				tfs[zipf.Uint64()]++
			}
			corpus[d][p] = propDoc{pl: pl, tfs: tfs}
			for tid := range tfs {
				globalDocCount[keyFor(tid)]++
			}
		}
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

	newBuckets := func(t *testing.T) []*Bucket {
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
		return buckets
	}

	// populate writes the corpus in `batches` contiguous doc ranges, flushing
	// every bucket after each batch; flushLast=false keeps the final batch in
	// the active memtable. Docs never repeat across batches, so a doc's
	// postings for a property live in exactly one source.
	populate := func(t *testing.T, buckets []*Bucket, batches int, flushLast bool) {
		batchSize := (nDocs + batches - 1) / batches
		for b := 0; b < batches; b++ {
			lo := b * batchSize
			hi := lo + batchSize
			if hi > nDocs {
				hi = nDocs
			}
			for d := lo; d < hi; d++ {
				for p := 0; p < nProps; p++ {
					for tid, tf := range corpus[d][p].tfs {
						require.NoError(t, buckets[p].MapSet([]byte(keyFor(tid)),
							NewMapPairFromDocIdAndTf(uint64(d), float32(tf), corpus[d][p].pl, false)))
					}
				}
			}
			if b < batches-1 || flushLast {
				for _, bk := range buckets {
					require.NoError(t, bk.FlushAndSwitch())
				}
			}
		}
	}

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
	diskTermsAcrossProps := func(t *testing.T, buckets []*Bucket, query []string) ([][][]*SegmentBlockMax, func()) {
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

	// bruteForce keeps a doc iff it has every query term in at least one property.
	// Per (doc, term, property) it takes the MAX across that property's sources —
	// mirroring MergedTerm.Score's per-property collapse — then sums across
	// properties.
	bruteForce := func(t *testing.T, buckets []*Bucket, query []string) map[uint64]float64 {
		allResults, release := diskTermsAcrossProps(t, buckets, query)
		defer release()

		perTermScore := make([]map[uint64]float64, len(query))
		for i := range perTermScore {
			perTermScore[i] = map[uint64]float64{}
		}
		for _, perProperty := range allResults {
			propMax := make([]map[uint64]float64, len(query))
			for i := range propMax {
				propMax[i] = map[uint64]float64{}
			}
			for _, segTerms := range perProperty {
				for _, term := range segTerms {
					ti := term.QueryTermIndex()
					for !term.Exhausted() {
						id, s, _ := term.Score(0, false)
						if cur, ok := propMax[ti][id]; !ok || s > cur {
							propMax[ti][id] = s
						}
						term.Advance()
					}
				}
			}
			for ti := range propMax {
				for id, s := range propMax[ti] {
					perTermScore[ti][id] += s
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

	runCrossProp := func(t *testing.T, buckets []*Bucket, query []string, limit int) map[uint64]float32 {
		allResults, release := diskTermsAcrossProps(t, buckets, query)
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

	queryCases := []struct {
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

	variants := []struct {
		name      string
		batches   int
		flushLast bool
	}{
		{"singleSegment", 1, true},    // one source per (doc, term, property)
		{"multiSegment", 3, true},     // several disk segments per property
		{"memtableAndDisk", 3, false}, // final batch stays in the active memtable
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			buckets := newBuckets(t)
			populate(t, buckets, v.batches, v.flushLast)

			for _, tc := range queryCases {
				t.Run(tc.name, func(t *testing.T) {
					brute := bruteForce(t, buckets, tc.query)

					limit := tc.limit
					if limit == 0 {
						limit = len(brute) + 1
					}
					cross := runCrossProp(t, buckets, tc.query, limit)

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
				cross := runCrossProp(t, buckets, missing, 100)
				require.Empty(t, cross)
			})
		})
	}
}

// TestBlockMaxAndCrossPropSamePropDuplicateDocID pins the per-property collapse:
// with the same (doc, term, property) live in two flushed segments at different
// tf values, the cross-prop score must use that property's MAX source
// contribution — not the sum — and add the other property's contribution.
func TestBlockMaxAndCrossPropSamePropDuplicateDocID(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const docID = uint64(42)
	const key = "dup"
	const pl = float32(20)

	newBucket := func() *Bucket {
		dir := t.TempDir()
		bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted))
		require.NoError(t, err)
		bucket.SetMemtableThreshold(1 << 30)
		t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
		return bucket
	}

	// prop 0: the same posting in two flushed segments, different tf; MapSet
	// writes no tombstone, so both stay live
	prop0 := newBucket()
	require.NoError(t, prop0.MapSet([]byte(key), NewMapPairFromDocIdAndTf(docID, 2, pl, false)))
	require.NoError(t, prop0.FlushAndSwitch())
	require.NoError(t, prop0.MapSet([]byte(key), NewMapPairFromDocIdAndTf(docID, 5, pl, false)))
	require.NoError(t, prop0.FlushAndSwitch())

	// prop 1: a single posting, so the sum across properties stays observable
	prop1 := newBucket()
	require.NoError(t, prop1.MapSet([]byte(key), NewMapPairFromDocIdAndTf(docID, 3, pl, false)))
	require.NoError(t, prop1.FlushAndSwitch())

	buckets := []*Bucket{prop0, prop1}
	query := []string{key}

	diskTerms := func(t *testing.T) ([][][]*SegmentBlockMax, func()) {
		allResults := make([][][]*SegmentBlockMax, 0, len(buckets))
		views := make([]BucketConsistentView, 0, len(buckets))
		for _, bk := range buckets {
			view := bk.GetConsistentView()
			views = append(views, view)
			dt, _, _, err := bk.createDiskTermFromCV(ctx, view, 10, nil, query, "", 1, []int{1}, bm25)
			require.NoError(t, err)
			allResults = append(allResults, dt)
		}
		return allResults, func() {
			for _, v := range views {
				v.ReleaseView()
			}
		}
	}

	// individual per-source contributions for docID, grouped by property
	allResults, release := diskTerms(t)
	perProp := make([][]float64, len(buckets))
	for p, perProperty := range allResults {
		for _, segTerms := range perProperty {
			for _, term := range segTerms {
				for !term.Exhausted() {
					id, s, _ := term.Score(0, false)
					if id == docID {
						perProp[p] = append(perProp[p], s)
					}
					term.Advance()
				}
			}
		}
	}
	release()

	require.Lenf(t, perProp[0], 2, "want the duplicate posting live in both prop-0 segments, got %d sources", len(perProp[0]))
	require.Len(t, perProp[1], 1)
	require.NotEqual(t, perProp[0][0], perProp[0][1], "different tf values must yield different source scores")

	want := math.Max(perProp[0][0], perProp[0][1]) + perProp[1][0]
	sum := perProp[0][0] + perProp[0][1] + perProp[1][0]
	tol := 1e-4 * (1 + math.Abs(want))
	require.Greater(t, sum, want+10*tol, "max and sum must be distinguishable at this tolerance")

	allResults2, release2 := diskTerms(t)
	defer release2()
	mergedTerms, ok := BuildCrossPropMergedTerms(allResults2, len(query))
	require.True(t, ok)
	h := DoBlockMaxAndCrossProp(ctx, 10, mergedTerms, 0, false, len(query), logger)
	require.Equal(t, 1, h.Len())
	it := h.Pop()
	require.Equal(t, docID, it.ID)
	require.InDeltaf(t, want, float64(it.Dist), tol,
		"cross-prop score %.6f must equal max-per-property total %.6f, not the sum %.6f", float64(it.Dist), want, sum)
}
