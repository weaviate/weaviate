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

// EstimateTokens is a rough upper bound of the tokens of a text, for the
// request budgets below: three bytes per token overstates English prose, so
// a request built with it stays under the provider's own count.
func EstimateTokens(text string) int {
	return len(text)/3 + 1
}

// ChunkDocuments splits documents into consecutive requests of at most
// maxDocuments each and at most maxTokens estimated tokens, counting the
// query once per document, which is how the providers bill and bound a
// request. A single document over the budget is sent on its own.
func ChunkDocuments(query string, documents []string, maxDocuments, maxTokens int) [][]string {
	queryTokens := EstimateTokens(query)
	var requests [][]string
	var current []string
	tokens := 0
	for _, document := range documents {
		cost := EstimateTokens(document) + queryTokens
		if len(current) > 0 && (len(current) >= maxDocuments || tokens+cost > maxTokens) {
			requests = append(requests, current)
			current, tokens = nil, 0
		}
		current = append(current, document)
		tokens += cost
	}
	if len(current) > 0 {
		requests = append(requests, current)
	}
	return requests
}
