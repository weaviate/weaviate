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
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	results.sortByID()
	iterations := 0
	var firstNonExhausted int
	pivotID := uint64(0)
	var pivotPoint int
	upperBound := float32(0)

	// nil for a non-cancellable ctx; guard below then skips the select.
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	ctxCheck := 0
	for {
		iterations++

		// counter, not iterations%N: avoids a modulo on the hottest loop.
		ctxCheck++
		if ctxCheck == 100000 {
			ctxCheck = 0
			if done != nil {
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
		for i := 0; i <= pivotPoint; i++ {
			// No exhausted guard: shallow-advance no-ops and impact is 0 when
			// exhausted; and currentBlockMaxId isn't an exhausted sentinel (can be 0).
			t := results[i]
			if t.currentBlockMaxId < pivotID {
				t.AdvanceAtLeastShallow(pivotID)
			}
			upperBound += t.currentBlockImpact
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			if additionalExplanations {
				docInfos = make([]*terms.DocPointerWithScore, termCount)
			}
			if pivotID == results[firstNonExhausted].idPointer {
				// a deferred tombstone hit: advance the aligned terms past the pivot
				// (below) but skip scoring/enqueue. See deferTombstoneToScore.
				pivotTombstoned := deferTombstoneToScore && results[firstNonExhausted].tombstoned(pivotID)
				if !pivotTombstoned {
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

				// only nextList moved; one re-insertion restores order.
				results.reinsertRight(nextList)

			}
		} else {
			nextList := pivotPoint
			maxWeight := results[nextList].idf
			next := uint64(math.MaxUint64) // max uint

			for i := 0; i <= pivotPoint; i++ {
				if i < pivotPoint && results[i].idf > maxWeight {
					nextList = i
					maxWeight = results[i].idf
				}
				if results[i].currentBlockMaxId < next {
					next = results[i].currentBlockMaxId
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

			// Full pass, not reinsertRight: AdvanceAtLeastShallow above de-sorts the
			// whole prefix, not just nextList; repairing one element hangs long queries.
			for i := nextList + 1; i < len(results); i++ {
				if results[i].idPointer < results[i-1].idPointer {
					results[i], results[i-1] = results[i-1], results[i]
				} else if results[i].exhausted && i < len(results)-1 {
					results[i], results[i+1] = results[i+1], results[i]
				}
			}

		}

	}
}

func DoBlockMaxAnd(ctx context.Context, limit int, resultsByTerm Terms, averagePropLength float64, additionalExplanations bool,
	termCount int, minimumOrTokensMatch int, logger logrus.FieldLogger,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	results := TermsBySize(resultsByTerm)
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	pivotID := uint64(0)
	upperBound := float32(0)

	if minimumOrTokensMatch > len(results) {
		return topKHeap
	}

	for {
		iterations++

		if iterations%100000 == 0 && ctx != nil && ctx.Err() != nil {
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

		for i := 0; i < len(results); i++ {
			if results[i].exhausted {
				return topKHeap
			}
		}

		results[0].AdvanceAtLeast(pivotID)

		if results[0].idPointer == math.MaxUint64 {
			return topKHeap
		}

		pivotID = results[0].idPointer

		for i := 1; i < len(results); i++ {
			results[i].AdvanceAtLeastShallow(pivotID)
		}

		upperBound = float32(0)
		for i := 0; i < len(results); i++ {
			upperBound += results[i].currentBlockImpact
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			isCandidate := true
			for i := 1; i < len(results); i++ {
				results[i].AdvanceAtLeast(pivotID)
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
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
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

// reinsertRight re-sorts when only t[i]'s idPointer increased; the rest stays
// sorted. Caller must guarantee that single-element precondition.
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
