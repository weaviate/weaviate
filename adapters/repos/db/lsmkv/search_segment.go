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
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func DoBlockMaxWand(ctx context.Context, limit int, results Terms, averagePropLength float64, additionalExplanations bool,
	termCount, minimumOrTokensMatch int, logger logrus.FieldLogger,
) (*priorityqueue.Queue[[]*terms.DocPointerWithScore], error) {
	var docInfos []*terms.DocPointerWithScore
	// limit+1: InsertAndPop holds limit+1 items between insert and pop, so an
	// exact-limit capacity forces one slice regrow per query.
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit + 1)
	worstDist := float64(-10000) // tf score can be negative
	results.sortByID()
	iterations := 0
	var firstNonExhausted int
	pivotID := uint64(0)
	var pivotPoint int
	upperBound := float32(0)

	done := ctx.Done()
	ctxCheck := 0
	for {
		iterations++

		// periodic cancellation check; a countdown avoids a 64-bit modulo on the
		// hottest loop in BMW (the modulo was ~1.5% of self time).
		ctxCheck++
		if ctxCheck == 100000 {
			ctxCheck = 0
			select {
			case <-done:
				segmentPath := ""
				terms := ""
				filterCardinality := -1
				for _, r := range results {
					if r == nil {
						continue
					}
					if r.segment != nil {
						segmentPath = r.segment.path
						if r.filterDocIds != nil {
							filterCardinality = r.filterDocIds.Len()
						}
					}
					terms += r.QueryTerm() + ":" + strconv.Itoa(int(r.IdPointer())) + ":" + strconv.Itoa(r.Count()) + ", "
				}
				logger.WithFields(logrus.Fields{
					"segment":           segmentPath,
					"iterations":        iterations,
					"pivotID":           pivotID,
					"firstNonExhausted": firstNonExhausted,
					"lenResults":        len(results),
					"pivotPoint":        pivotPoint,
					"upperBound":        upperBound,
					"terms":             terms,
					"filterCardinality": filterCardinality,
					"limit":             limit,
				}).Warnf("doBlockMaxWand: search timed out, returning partial results")
				helpers.AnnotateSlowQueryLog(ctx, "kwd_4_iters", iterations)
				return topKHeap, fmt.Errorf("doBlockMaxWand: search timed out, returning partial results")
			default:
			}
		}

		cumScore := float64(0)
		firstNonExhausted = -1
		pivotID = math.MaxUint64

		for pivotPoint = 0; pivotPoint < len(results); pivotPoint++ {
			if results[pivotPoint].exhausted {
				continue
			}
			if firstNonExhausted == -1 {
				firstNonExhausted = pivotPoint
			}
			cumScore += results[pivotPoint].idf
			if cumScore >= worstDist {
				pivotID = results[pivotPoint].idPointer
				for i := pivotPoint + 1; i < len(results); i++ {
					if results[i].idPointer != pivotID {
						break
					}
					pivotPoint = i
				}
				break
			}
		}
		if firstNonExhausted == -1 || pivotID == math.MaxUint64 {
			helpers.AnnotateSlowQueryLog(ctx, "kwd_4_iters", iterations)
			return topKHeap, nil
		}

		upperBound = float32(0)
		// reslice to [0,pivotPoint] (pivotPoint < len, guaranteed by the pivotID
		// check above) so the compiler drops the per-iteration bounds check.
		candidates := results[:pivotPoint+1]
		for i := range candidates {
			// exhausted terms carry currentBlockMaxId==MaxUint64 (so the shallow
			// advance is skipped) and currentBlockImpact==0 (so the add is a no-op),
			// so no explicit exhausted guard is needed here.
			t := candidates[i]
			if t.currentBlockMaxId < pivotID {
				t.AdvanceAtLeastShallow(pivotID)
			}
			upperBound += t.currentBlockImpact
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			firstTerm := results[firstNonExhausted]
			if pivotID == firstTerm.idPointer {
				// a deferred tombstone hit: advance the aligned terms past the pivot
				// (below) but skip scoring/enqueue. See deferTombstoneToScore.
				pivotTombstoned := deferTombstoneToScore && firstTerm.tombstoned(pivotID)
				if !pivotTombstoned {
					if additionalExplanations {
						docInfos = make([]*terms.DocPointerWithScore, termCount)
					}
					score := 0.0
					termsMatched := 0
					for _, term := range results {
						if term.idPointer != pivotID {
							break
						}
						termsMatched++
						_, s, d := term.Score(averagePropLength, additionalExplanations)
						score += s

						if additionalExplanations {
							docInfos[term.QueryTermIndex()] = d
						}

					}
					if topKHeap.ShouldEnqueue(float32(score), limit) && termsMatched >= minimumOrTokensMatch {
						topKHeap.InsertAndPop(pivotID, score, limit, &worstDist, docInfos)
					}
				}
				for _, term := range results {
					if !term.exhausted && term.idPointer != pivotID {
						break
					}
					term.Advance()
				}

				results.sortByID()

			} else {
				nextList := pivotPoint
				for results[nextList].idPointer == pivotID {
					nextList--
				}
				results[nextList].AdvanceAtLeast(pivotID)

				// only results[nextList] moved (rightward), so a single re-insertion
				// restores order — same permutation as the old swap bubble, one move
				// per step instead of a two-write swap.
				results.reinsertRight(nextList)

			}
		} else {
			// single pass over [0, pivotPoint] computing both the heaviest term to
			// advance (over [0, pivotPoint)) and the smallest block-max id (over
			// [0, pivotPoint]) — formerly two separate scans.
			nextList := pivotPoint
			maxWeight := results[nextList].idf
			next := uint64(math.MaxUint64) // max uint

			candidates := results[:pivotPoint+1]
			for i := range candidates {
				t := candidates[i]
				if i < pivotPoint && t.idf > maxWeight {
					nextList = i
					maxWeight = t.idf
				}
				if t.currentBlockMaxId < next {
					next = t.currentBlockMaxId
				}
			}

			next += 1

			if pivotPoint+1 < len(results) && results[pivotPoint+1].idPointer < next {
				next = results[pivotPoint+1].idPointer
			}

			if next <= pivotID {
				next = pivotID + 1
			}
			results[nextList].AdvanceAtLeast(next)

			// AdvanceAtLeast only ever increases results[nextList].idPointer (or
			// exhausts it to MaxUint64), so the slice is sorted everywhere except
			// that one element, which must move right. Re-insert it and stop the
			// instant it settles — the old loop had no early break and rescanned the
			// whole tail every iteration (its exhausted-swap branch only ever
			// reordered MaxUint64 sentinels, which no reader observes).
			results.reinsertRight(nextList)

		}

	}
}

func DoBlockMaxAnd(ctx context.Context, limit int, resultsByTerm Terms, averagePropLength float64, additionalExplanations bool,
	termCount int, minimumOrTokensMatch int, logger logrus.FieldLogger,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	results := TermsBySize(resultsByTerm)
	var docInfos []*terms.DocPointerWithScore
	// limit+1: InsertAndPop holds limit+1 items between insert and pop, so an
	// exact-limit capacity forces one slice regrow per query.
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit + 1)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	pivotID := uint64(0)
	upperBound := float32(0)

	if minimumOrTokensMatch > len(results) {
		return topKHeap
	}

	ctxCheck := 0
	for {
		iterations++

		// periodic cancellation check; countdown instead of a 64-bit modulo on the
		// hot loop (same change as DoBlockMaxWand), same every-100000 cadence.
		ctxCheck++
		if ctxCheck == 100000 {
			ctxCheck = 0
			if ctx != nil && ctx.Err() != nil {
				segmentPath := ""
				terms := ""
				filterCardinality := -1
				for _, r := range results {
					if r == nil {
						continue
					}
					if r.segment != nil {
						segmentPath = r.segment.path
						if r.filterDocIds != nil {
							filterCardinality = r.filterDocIds.Len()
						}
					}
					terms += r.QueryTerm() + ":" + strconv.Itoa(int(r.IdPointer())) + ":" + strconv.Itoa(r.Count()) + ", "
				}
				logger.WithFields(logrus.Fields{
					"segment":           segmentPath,
					"iterations":        iterations,
					"pivotID":           pivotID,
					"lenResults":        len(results),
					"upperBound":        upperBound,
					"terms":             terms,
					"filterCardinality": filterCardinality,
					"limit":             limit,
				}).Warnf("DoBlockMaxAnd: search timed out, returning partial results")
				return topKHeap
			}
		}

		results[0].AdvanceAtLeast(pivotID)

		if results[0].idPointer == math.MaxUint64 {
			return topKHeap
		}

		pivotID = results[0].idPointer

		// AND is terminal the instant any term exhausts: no higher doc id can align
		// across every term, so the heap is frozen and we return at the advance site
		// rather than rescanning all terms at the top of each iteration. results[0]
		// is covered just above; the others can only exhaust here or in the
		// candidate-advance loop below.
		upperBound = results[0].currentBlockImpact
		for i := 1; i < len(results); i++ {
			results[i].AdvanceAtLeastShallow(pivotID)
			if results[i].exhausted {
				return topKHeap
			}
			upperBound += results[i].currentBlockImpact
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			isCandidate := true
			for i := 1; i < len(results); i++ {
				results[i].AdvanceAtLeast(pivotID)
				if results[i].exhausted {
					return topKHeap
				}
				if results[i].idPointer != pivotID {
					isCandidate = false
					break
				}
			}
			if isCandidate {
				// deferred tombstone hit (see deferTombstoneToScore): advance past the
				// candidate without scoring it.
				if deferTombstoneToScore && results[0].tombstoned(pivotID) {
					for _, term := range results {
						term.Advance()
					}
				} else {
					score := 0.0
					if additionalExplanations {
						docInfos = make([]*terms.DocPointerWithScore, termCount)
					}
					for _, term := range results {
						_, s, d := term.Score(averagePropLength, additionalExplanations)
						score += s
						if additionalExplanations {
							docInfos[term.QueryTermIndex()] = d
						}
						term.Advance()
					}
					if topKHeap.ShouldEnqueue(float32(score), limit) {
						topKHeap.InsertAndPop(pivotID, score, limit, &worstDist, docInfos)
					}
				}
			} else {
				pivotID += 1
			}
		} else {

			// max uint
			pivotID = uint64(math.MaxUint64)

			for i := 0; i < len(results); i++ {
				if results[i].currentBlockMaxId < pivotID {
					pivotID = results[i].currentBlockMaxId
				}
			}

			pivotID += 1
		}
	}
}

func DoWand(ctx context.Context, limit int, results *terms.Terms, averagePropLength float64, additionalExplanations bool,
	minimumOrTokensMatch int, logger logrus.FieldLogger,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	// limit+1: InsertAndPop holds limit+1 items between insert and pop, so an
	// exact-limit capacity forces one slice regrow per query.
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit + 1)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	for {
		iterations++

		if results.CompletelyExhausted() || results.Pivot(worstDist) {
			helpers.AnnotateSlowQueryLog(ctx, "kwd_4_iters", iterations)
			return topKHeap
		}

		id, score, additional, ok := results.ScoreNext(averagePropLength, additionalExplanations, minimumOrTokensMatch)
		results.SortFull()
		if topKHeap.ShouldEnqueue(float32(score), limit) && ok {
			topKHeap.InsertAndPop(id, score, limit, &worstDist, additional)
		}

		if iterations%100000 == 0 && ctx != nil && ctx.Err() != nil {
			logger.WithFields(logrus.Fields{
				"iterations": iterations,
				"limit":      limit,
			}).Warnf("DoWand: search timed out, returning partial results")
			helpers.AnnotateSlowQueryLog(ctx, "kwd_4_iters", iterations)
			return topKHeap
		}
	}
}

type Terms []*SegmentBlockMax

// provide sort interface for
func (t Terms) Len() int {
	return len(t)
}

func (t Terms) Less(i, j int) bool {
	return t[i].idPointer < t[j].idPointer
}

func (t Terms) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// reinsertRight restores ascending-by-idPointer order when exactly one element
// (at index i) has had its idPointer increased and everything else is already
// sorted. It shifts the element rightward to its slot and stops as soon as it
// settles — O(displacement), with one move per step instead of a two-write swap.
func (t Terms) reinsertRight(i int) {
	cur := t[i]
	id := cur.idPointer
	j := i
	for j+1 < len(t) && t[j+1].idPointer < id {
		t[j] = t[j+1]
		j++
	}
	t[j] = cur
}

// sortByID sorts the terms ascending by idPointer with a concrete insertion
// sort. It replaces sort.Sort on the WAND hot path: the term list is small
// (query-term count) and, after the first pass, nearly sorted — the regime where
// insertion sort wins — and comparing idPointer directly avoids the
// sort.Interface Less/Swap dispatch that showed up as ~5% self time.
func (t Terms) sortByID() {
	for i := 1; i < len(t); i++ {
		cur := t[i]
		id := cur.idPointer
		j := i - 1
		for j >= 0 && t[j].idPointer > id {
			t[j+1] = t[j]
			j--
		}
		t[j+1] = cur
	}
}

type TermsBySize []*SegmentBlockMax

// provide sort interface for
func (t TermsBySize) Len() int {
	return len(t)
}

func (t TermsBySize) Less(i, j int) bool {
	return t[i].Count() < t[j].Count()
}

func (t TermsBySize) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}
