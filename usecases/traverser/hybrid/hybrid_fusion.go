//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hybrid

import (
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/search"
)

func FusionRanked(weights []float64, resultSets [][]*search.Result, setNames []string) []*search.Result {
	combinedResults := map[strfmt.UUID]*search.Result{}
	for resultSetIndex, resultSet := range resultSets {
		for i, res := range resultSet {
			if res.DocID == nil {
				panic("doc id is nil")
			}
			tempResult := res
			docId := tempResult.ID
			score := float32(weights[resultSetIndex] / float64(i+60)) // TODO replace 60 with a class configured variable in the schema

			if tempResult.AdditionalProperties == nil {
				tempResult.AdditionalProperties = map[string]interface{}{}
			}

			// Get previous results from the map, if any
			previousResult, ok := combinedResults[docId]
			if ok {
				tempResult.AdditionalProperties["explainScore"] = fmt.Sprintf(
					"%v\nHybrid (Result Set %v) Document %v contributed %v to the score",
					previousResult.AdditionalProperties["explainScore"], setNames[resultSetIndex], tempResult.ID, score)
				score += previousResult.Score
			} else {
				tempResult.AdditionalProperties["explainScore"] = fmt.Sprintf(
					"%v\nHybrid (Result Set %v) Document %v contributed %v to the score",
					tempResult.ExplainScore, setNames[resultSetIndex], tempResult.ID, score)
			}
			tempResult.AdditionalProperties["rank_score"] = score
			tempResult.AdditionalProperties["score"] = score

			tempResult.Score = float32(score)
			combinedResults[docId] = tempResult
		}
	}

	// Sort the results
	var (
		sortList = make([]*search.Result, len(combinedResults))
		i        = 0
	)
	for _, res := range combinedResults {
		res.ExplainScore = res.AdditionalProperties["explainScore"].(string)
		sortList[i] = res
		i++
	}

	sort.Slice(sortList, func(i, j int) bool {
		a_b := float64(sortList[j].Score - sortList[i].Score)
		if a_b*a_b < 1e-14 {
			a_b2 := float64(sortList[j].SecondarySortValue - sortList[i].SecondarySortValue)
			if a_b2*a_b2 < 1e-14 {
				return sortList[i].ID > sortList[j].ID
			} else {
				return sortList[i].SecondarySortValue > sortList[j].SecondarySortValue
			}
		}
		return float64(sortList[i].Score) > float64(sortList[j].Score)
	})
	return sortList
}

// FusionRelativeScore uses the relative differences in the scores from keyword and vector search to combine the
// results. This method retains more information than ranked fusion and should result in better results.
//
// The scores from each result are normalized between 0 and 1, e.g. the maximum score becomes 1 and the minimum 0 and the
// other scores are in between, keeping their relative distance to the other scores.
// Example:
//
//	Input score = [1, 8, 6, 11] => [0, 0.7, 0.5, 1]
//
// The normalized scores are then combined using their respective weight and the combined scores are sorted
func FusionRelativeScore(weights []float64, resultSets [][]*search.Result, names []string, descending bool) []*search.Result {
	if len(resultSets) == 0 || len(resultSets[0]) == 0 && (len(resultSets) == 1 || len(resultSets[1]) == 0) {
		return []*search.Result{}
	}

	var maximum []float32
	var minimum []float32

	for i := range resultSets {
		if len(resultSets[i]) > 0 {
			maximum = append(maximum, resultSets[i][0].SecondarySortValue)
			minimum = append(minimum, resultSets[i][0].SecondarySortValue)
		} else { // dummy values so the indices match
			maximum = append(maximum, 0)
			minimum = append(minimum, 0)
		}
		for _, res := range resultSets[i] {
			if res.SecondarySortValue > maximum[i] {
				maximum[i] = res.SecondarySortValue
			}

			if res.SecondarySortValue < minimum[i] {
				minimum[i] = res.SecondarySortValue
			}
		}
	}

	// normalize scores between 0 and 1 and sum up the normalized scores from different sources
	// pre-allocate map, at this stage we do not know how many total, combined results there are, but it is at least the
	// length of the longer input list
	numResults := len(resultSets[0])
	if len(resultSets) > 1 && len(resultSets[1]) > numResults {
		numResults = len(resultSets[1])
	}
	mapResults := make(map[strfmt.UUID]*search.Result, numResults)
	for i := range resultSets {
		weight := float32(weights[i])
		for _, res := range resultSets[i] {
			// If all scores are identical min and max are the same => just set score to the weight.
			score := weight
			if maximum[i] != minimum[i] {
				score *= (res.SecondarySortValue - minimum[i]) / (maximum[i] - minimum[i])
			}

			previousResult, ok := mapResults[res.ID]
			explainScore := fmt.Sprintf("Hybrid (Result Set %v) Document %v: original score %v, normalized score: %v", names[i], res.ID, res.SecondarySortValue, score)
			if ok {
				score += previousResult.Score
				explainScore += " - " + previousResult.ExplainScore
			}
			res.Score = score
			res.ExplainScore = res.ExplainScore + "\n" + explainScore

			mapResults[res.ID] = res
		}
	}

	concat := make([]*search.Result, 0, len(mapResults))
	for _, res := range mapResults {
		concat = append(concat, res)
	}
	if descending {
		sort.Slice(concat, func(i, j int) bool {
			a_b := float64(concat[j].Score - concat[i].Score)
			if a_b*a_b < 1e-14 {
				a_b2 := float64(concat[j].SecondarySortValue - concat[i].SecondarySortValue)
				if a_b2*a_b2 < 1e-14 {
					return concat[i].ID > concat[j].ID
				} else {
					return concat[i].SecondarySortValue > concat[j].SecondarySortValue
				}
			}
			return float64(concat[i].Score) > float64(concat[j].Score)
		})
	} else {
		sort.Slice(concat, func(i, j int) bool {
			a_b := float64(concat[j].Score - concat[i].Score)
			if a_b*a_b < 1e-14 {
				a_b2 := float64(concat[j].SecondarySortValue - concat[i].SecondarySortValue)
				if a_b2*a_b2 < 1e-14 {
					return concat[i].ID > concat[j].ID
				} else {
					return concat[i].SecondarySortValue < concat[j].SecondarySortValue
				}
			}
			return float64(concat[i].Score) < float64(concat[j].Score)
		})
	}
	return concat
}
