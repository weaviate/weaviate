package terms

import "github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"

func DoWand(limit int, results *Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*DocPointerWithScore] {
	topKHeap := priorityqueue.NewMinWithId[[]*DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	results.SortFull()
	for {

		if results.CompletelyExhausted() || results.PivotWand(worstDist) {
			return topKHeap
		}

		id, score, additional := results.ScoreNext(averagePropLength, additionalExplanations)
		results.SortFull()
		if topKHeap.ShouldEnqueue(float32(score), limit) {
			topKHeap.InsertAndPop(id, score, limit, &worstDist, additional)
		}
	}
}

func DoBlockMaxWand(limit int, results *Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*DocPointerWithScore] {
	// averagePropLength = 40
	var docInfos []*DocPointerWithScore
	topKHeap := priorityqueue.NewMinWithId[[]*DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative

	results.SortFull()
	for {
		if results.CompletelyExhausted() {
			return topKHeap
		}

		pivotID, pivotPoint, notFoundPivot := results.FindMinID(worstDist)
		if notFoundPivot {
			return topKHeap
		}

		upperBound := results.GetBlockUpperBound(pivotPoint, pivotID)

		if topKHeap.ShouldEnqueue(upperBound, limit) {
			if additionalExplanations {
				docInfos = make([]*DocPointerWithScore, results.Count)
			}
			if pivotID == results.T[0].IdPointer() {
				score := float32(0.0)
				for _, term := range results.T {
					if term.IdPointer() != pivotID {
						break
					}
					_, s, d := term.Score(averagePropLength, additionalExplanations)
					score += float32(s)
					upperBound -= term.CurrentBlockImpact() - float32(s)

					if additionalExplanations {
						docInfos[term.QueryTermIndex()] = d
					}
					//if !topKHeap.ShouldEnqueue(upperBound, limit) {
					//	break
					//}
				}
				for _, term := range results.T {
					if term.IdPointer() != pivotID {
						break
					}
					term.Advance()
				}

				topKHeap.InsertAndPop(pivotID, float64(score), limit, &worstDist, docInfos)

				results.SortFull()
			} else {
				nextList := pivotPoint
				for results.T[nextList].IdPointer() == pivotID {
					nextList--
				}
				results.T[nextList].AdvanceAtLeast(pivotID)

				results.SortPartial(nextList)

			}
		} else {
			nextList := pivotPoint
			maxWeight := results.T[nextList].CurrentBlockImpact()

			for i := 0; i < pivotPoint; i++ {
				if results.T[i].CurrentBlockImpact() > maxWeight {
					nextList = i
					maxWeight = results.T[i].CurrentBlockImpact()
				}
			}

			next := uint64(999999999999999)

			for i := 0; i <= pivotPoint; i++ {
				if results.T[i].CurrentBlockMaxId() < next {
					next = results.T[i].CurrentBlockMaxId()
				}
			}

			next += 1

			if pivotPoint+1 < len(results.T) && results.T[pivotPoint+1].IdPointer() < next {
				next = results.T[pivotPoint+1].IdPointer()
			}

			if next <= pivotID {
				next = pivotID + 1
			}
			results.T[nextList].AdvanceAtLeast(next)

			results.SortPartial(nextList)

		}

	}
}
