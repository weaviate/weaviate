package lsmkv

import (
	"math"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func DoBlockMaxWand(limit int, results Terms, averagePropLength float64, additionalExplanations bool,
	termCount int,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	var docInfos []*terms.DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	iterations := 0
	var firstNonExhausted int
	pivotID := uint64(0)
	var pivotPoint int
	upperBound := float32(0)

	for {
		iterations++

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
			cumScore += float64(results[pivotPoint].Idf())
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
			return topKHeap
		}

		upperBound = float32(0)
		for i := 0; i < pivotPoint+1; i++ {
			if results[i].exhausted {
				continue
			}
			if results[i].currentBlockMaxId < pivotID {
				results[i].AdvanceAtLeastShallow(pivotID)
			}
			upperBound += results[i].currentBlockImpact
		}

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			if additionalExplanations {
				docInfos = make([]*terms.DocPointerWithScore, termCount)
			}
			if pivotID == results[firstNonExhausted].idPointer {
				score := float32(0.0)
				for _, term := range results {
					if term.idPointer != pivotID {
						break
					}
					_, s, d := term.Score(averagePropLength, additionalExplanations)
					score += float32(s)
					upperBound -= term.currentBlockImpact - float32(s)

					if additionalExplanations {
						docInfos[term.QueryTermIndex()] = d
					}

					//if !topKHeap.ShouldEnqueue(upperBound, limit) {
					//	break
					//}
				}
				for _, term := range results {
					if !term.exhausted && term.idPointer != pivotID {
						break
					}
					term.Advance()
				}
				if topKHeap.ShouldEnqueue(score, limit) {
					topKHeap.InsertAndPop(pivotID, float64(score), limit, &worstDist, docInfos)
				}

				sort.Sort(results)

			} else {
				nextList := pivotPoint
				for results[nextList].idPointer == pivotID {
					nextList--
				}
				results[nextList].AdvanceAtLeast(pivotID)

				// sort partial
				for i := nextList + 1; i < len(results); i++ {
					if results[i].idPointer < results[i-1].idPointer {
						// swap
						results[i], results[i-1] = results[i-1], results[i]
					} else {
						break
					}
				}

			}
		} else {
			nextList := pivotPoint
			maxWeight := results[nextList].Idf()

			for i := 0; i < pivotPoint; i++ {
				if results[i].Idf() > maxWeight {
					nextList = i
					maxWeight = results[i].Idf()
				}
			}

			// max uint
			next := uint64(math.MaxUint64)

			for i := 0; i <= pivotPoint; i++ {
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

			for i := nextList + 1; i < len(results); i++ {
				if results[i].idPointer < results[i-1].idPointer {
					// swap
					results[i], results[i-1] = results[i-1], results[i]
				} else if results[i].exhausted && i < len(results)-1 {
					results[i], results[i+1] = results[i+1], results[i]
				} else {
					break
				}
			}

		}

	}
}

func DoWand(limit int, results *terms.Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	topKHeap := priorityqueue.NewMinWithId[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	for {

		if results.CompletelyExhausted() || results.Pivot(worstDist) {
			return topKHeap
		}

		id, score, additional := results.ScoreNext(averagePropLength, additionalExplanations)
		results.SortFull()
		if topKHeap.ShouldEnqueue(float32(score), limit) {
			topKHeap.InsertAndPop(id, score, limit, &worstDist, additional)
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
