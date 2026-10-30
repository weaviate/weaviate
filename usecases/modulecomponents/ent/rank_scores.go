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

// IndexedScore is one result of a reranker API: the position of a document
// in the request and its score.
type IndexedScore struct {
	Index int
	Score float64
}

// DocumentScoresByIndex returns one score per document, in the order of
// documents, which is what the rank provider expects. It rejects an index
// outside documents and a repeated index. With complete set, it also rejects
// a response that does not score every document. Without it, a document the
// response leaves out (for example below a top_n cut) gets the score 0.
func DocumentScoresByIndex(documents []string, results []IndexedScore, complete bool) ([]DocumentScore, error) {
	if len(results) > len(documents) || (complete && len(results) != len(documents)) {
		return nil, fmt.Errorf("reranker response has %d results for %d documents", len(results), len(documents))
	}
	scores := make([]DocumentScore, len(documents))
	for i := range documents {
		scores[i].Document = documents[i]
	}
	seen := make([]bool, len(documents))
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(documents) || seen[result.Index] {
			return nil, fmt.Errorf("reranker response has an invalid or repeated index %d for %d documents",
				result.Index, len(documents))
		}
		seen[result.Index] = true
		scores[result.Index].Score = result.Score
	}
	return scores, nil
}
