//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hybrid

import (
	"fmt"
	"sort"

	"github.com/go-openapi/strfmt"
)

func FusionRanked(weights []float64, results [][]*Result) []*Result {
	mapResults := map[strfmt.UUID]*Result{}
	for resultSetIndex, result := range results {
		for i, res := range result {
			tempResult := res
			docId := tempResult.ID
			score := weights[resultSetIndex] / float64(i+60+1) // TODO replace 60 with a class configured variable

			if tempResult.AdditionalProperties == nil {
				tempResult.AdditionalProperties = map[string]interface{}{}
			}

			// Get previous results from the map, if any
			previousResult, ok := mapResults[docId]
			if ok {
				tempResult.AdditionalProperties["explainScore"] = fmt.Sprintf(
					"%v\n(hybrid) Document %v contributed %v to the score",
					previousResult.AdditionalProperties["explainScore"], tempResult.ID, score)
				score += float64(previousResult.Score)
			} else {
				tempResult.AdditionalProperties["explainScore"] = fmt.Sprintf(
					"%v\n(hybrid) Document %v contributed %v to the score",
					tempResult.ExplainScore, tempResult.ID, score)
			}
			tempResult.AdditionalProperties["rank_score"] = score
			tempResult.AdditionalProperties["score"] = score

			tempResult.Score = float32(score)
			mapResults[docId] = tempResult
		}
	}

	// Sort the results
	var (
		concat = make([]*Result, len(mapResults))
		i      = 0
	)
	for _, res := range mapResults {
		res.ExplainScore = res.AdditionalProperties["explainScore"].(string)
		concat[i] = res
		i++
	}

	sort.Slice(concat, func(i, j int) bool {
		if concat[j].Score == concat[i].Score {
			return concat[i].SecondarySortValue > concat[j].SecondarySortValue
		}
		return float64(concat[i].Score) > float64(concat[j].Score)
	})
	return concat
}

// FusionRelativeScore uses the relative differences in the scores from keyword and vector search to combine the
// results. This method retains more information than ranked fusion and should result in better results.
//
// The scores from each result are normalized between 0 and 1, e.g. the maximum score becomes 1 and the minimum 0 and the
// other scores are inbetween, keeping their relative distance to the other scores.
// Example:
//
//	Input score = [1, 8, 6, 11] => [0, 0.7, 0.5, 1]
//
// The normalized scores are then combined using their respective weight and the combined scores are sorted
func FusionRelativeScore(weights []float64, results [][]*Result) []*Result {
	if len(results[0]) == 0 {
		return []*Result{}
	}

	maximum := []float32{results[0][0].SecondarySortValue, results[1][0].SecondarySortValue}
	minimum := []float32{results[0][0].SecondarySortValue, results[1][0].SecondarySortValue}

	for i := range results {
		for _, res := range results[i] {
			if res.SecondarySortValue > maximum[i] {
				maximum[i] = res.SecondarySortValue
			}

			if res.SecondarySortValue < minimum[i] {
				minimum[i] = res.SecondarySortValue
			}
		}
	}

	// normalize scores between 0 and 1
	mapResults := make(map[strfmt.UUID]*Result)
	for i := range results {
		weight := float32(weights[i])
		for _, res := range results[i] {
			var score float32
			if maximum[i] != minimum[i] {
				score = weight * (res.SecondarySortValue - minimum[i]) / (maximum[i] - minimum[i])
			} else {
				score = 0
			}

			previousResult, ok := mapResults[res.ID]
			if ok {
				score += previousResult.Score
			}
			res.Score = score

			mapResults[res.ID] = res
		}
	}

	concat := make([]*Result, 0, len(mapResults))
	for _, res := range mapResults {
		concat = append(concat, res)
	}

	sort.Slice(concat, func(i, j int) bool {
		a_b := float64(concat[j].Score - concat[i].Score)
		if a_b*a_b < 1e-14 {
			return concat[i].SecondarySortValue > concat[j].SecondarySortValue
		}
		return float64(concat[i].Score) > float64(concat[j].Score)
	})
	return concat
}
