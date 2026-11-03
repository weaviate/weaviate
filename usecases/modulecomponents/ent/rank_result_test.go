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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentScoresByIndex(t *testing.T) {
	type result struct {
		index int
		score float64
	}
	at := func(r result) (int, float64) { return r.index, r.score }
	threeDocs := []string{"doc0", "doc1", "doc2"}

	tests := []struct {
		name      string
		documents []string
		results   []result
		expected  []DocumentScore
		expectErr string
	}{
		{
			name:      "shuffled indices are placed in document order",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {0, 0.8}, {1, 0.7}},
			expected: []DocumentScore{
				{Document: "doc0", Score: 0.8},
				{Document: "doc1", Score: 0.7},
				{Document: "doc2", Score: 0.9},
			},
		},
		{
			name:      "fewer results than documents",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {0, 0.8}},
			expectErr: "reranker returned 2 results for 3 documents",
		},
		{
			name:      "more results than documents",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {0, 0.8}, {1, 0.7}, {1, 0.6}},
			expectErr: "reranker returned 4 results for 3 documents",
		},
		{
			name:      "negative index",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {-1, 0.8}, {1, 0.7}},
			expectErr: "reranker returned index -1 for 3 documents",
		},
		{
			name:      "index equal to the number of documents",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {3, 0.8}, {1, 0.7}},
			expectErr: "reranker returned index 3 for 3 documents",
		},
		{
			name:      "duplicate index",
			documents: threeDocs,
			results:   []result{{2, 0.9}, {0, 0.8}, {2, 0.7}},
			expectErr: "reranker returned index 2 twice",
		},
		{
			name:      "zero documents and zero results",
			documents: []string{},
			results:   []result{},
			expected:  []DocumentScore{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scores, err := DocumentScoresByIndex(tt.documents, tt.results, at)
			if tt.expectErr != "" {
				require.EqualError(t, err, tt.expectErr)
				assert.Nil(t, scores)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, scores)
		})
	}
}
