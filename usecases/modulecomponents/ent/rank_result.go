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

package ent

import "fmt"

type RankResult struct {
	Query          string
	DocumentScores []DocumentScore
}

type DocumentScore struct {
	Document string
	Score    float64
}

// DocumentScoresByIndex places each provider result at the position of the
// document it scored. It fails when the provider did not return exactly one
// result per document, which the rerank contract requires.
func DocumentScoresByIndex[R any](documents []string, results []R, at func(R) (index int, score float64)) ([]DocumentScore, error) {
	if len(results) != len(documents) {
		return nil, fmt.Errorf("reranker returned %d results for %d documents", len(results), len(documents))
	}
	scores := make([]DocumentScore, len(documents))
	seen := make([]bool, len(documents))
	for _, result := range results {
		index, score := at(result)
		if index < 0 || index >= len(documents) {
			return nil, fmt.Errorf("reranker returned index %d for %d documents", index, len(documents))
		}
		if seen[index] {
			return nil, fmt.Errorf("reranker returned index %d twice", index)
		}
		seen[index] = true
		scores[index] = DocumentScore{Document: documents[index], Score: score}
	}
	return scores, nil
}
