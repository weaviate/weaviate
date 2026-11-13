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

// RankResult is a reranker's answer for one query. DocumentScores has exactly
// one entry per input document, in input order.
type RankResult struct {
	Query          string
	DocumentScores []DocumentScore
}

type DocumentScore struct {
	Document string
	Score    float64
	// Unscored is set when the provider returned no score for the document,
	// e.g. because a top-N setting cut it off. Such documents are dropped
	// from the reranked results. The zero value means scored, so producers
	// that always score every document need not set it.
	Unscored bool
}

// ToDocumentScores maps provider results back to the order of documents.
// Providers return results sorted by relevance, each carrying the position of
// its document in the request; indexAndScore extracts that position and the
// score. Documents with no result are marked Unscored. An out-of-range or
// duplicate index is an error rather than a panic or a silently misattributed
// score.
func ToDocumentScores[T any](documents []string, results []T,
	indexAndScore func(T) (int, float64),
) ([]DocumentScore, error) {
	documentScores := make([]DocumentScore, len(documents))
	for i := range documents {
		documentScores[i] = DocumentScore{Document: documents[i], Unscored: true}
	}
	for _, result := range results {
		index, score := indexAndScore(result)
		if index < 0 || index >= len(documents) {
			return nil, fmt.Errorf("reranker result index %d out of range for %d documents", index, len(documents))
		}
		if !documentScores[index].Unscored {
			return nil, fmt.Errorf("reranker returned duplicate result index %d", index)
		}
		documentScores[index].Score = score
		documentScores[index].Unscored = false
	}
	return documentScores, nil
}
