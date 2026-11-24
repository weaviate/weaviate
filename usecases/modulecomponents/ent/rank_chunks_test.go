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

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkDocuments(t *testing.T) {
	short := strings.Repeat("a", 29) // 10 tokens
	long := strings.Repeat("a", 299) // 100 tokens
	tests := []struct {
		name         string
		query        string
		documents    []string
		maxDocuments int
		maxTokens    int
		want         [][]int // sizes of the requests
	}{
		{name: "nothing", documents: nil, maxDocuments: 10, maxTokens: 1000, want: nil},
		{name: "by count", documents: []string{short, short, short}, maxDocuments: 2, maxTokens: 1000, want: [][]int{{2}, {1}}},
		{name: "by tokens", documents: []string{long, long, long}, maxDocuments: 10, maxTokens: 250, want: [][]int{{2}, {1}}},
		{
			// 10 + 90 query tokens per document: 100 each.
			name: "the query counts once per document", query: strings.Repeat("q", 267),
			documents: []string{short, short, short}, maxDocuments: 10, maxTokens: 250, want: [][]int{{2}, {1}},
		},
		{name: "a document over the budget goes alone", documents: []string{long, short, long}, maxDocuments: 10, maxTokens: 50, want: [][]int{{1}, {1}, {1}}},
		{name: "everything fits", documents: []string{short, long, short}, maxDocuments: 10, maxTokens: 1000, want: [][]int{{3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ChunkDocuments(tt.query, tt.documents, tt.maxDocuments, tt.maxTokens)
			var sizes [][]int
			var flat []string
			for _, request := range got {
				sizes = append(sizes, []int{len(request)})
				flat = append(flat, request...)
			}
			assert.Equal(t, tt.want, sizes)
			assert.Equal(t, tt.documents, flat, "every document once, in order")
		})
	}
}
