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

func TestToDocumentScores(t *testing.T) {
	type result struct {
		index int
		score float64
	}
	documents := []string{"a", "b", "c"}

	tests := []struct {
		name    string
		results []result
		want    []DocumentScore
		wantErr string
	}{
		{
			name:    "results sorted by relevance are mapped back to input order",
			results: []result{{2, 0.9}, {0, 0.5}, {1, -1.5}},
			want: []DocumentScore{
				{Document: "a", Score: 0.5},
				{Document: "b", Score: -1.5},
				{Document: "c", Score: 0.9},
			},
		},
		{
			name:    "documents without a result are unscored",
			results: []result{{1, 0.7}},
			want: []DocumentScore{
				{Document: "a", Unscored: true},
				{Document: "b", Score: 0.7},
				{Document: "c", Unscored: true},
			},
		},
		{
			name:    "no results",
			results: nil,
			want: []DocumentScore{
				{Document: "a", Unscored: true},
				{Document: "b", Unscored: true},
				{Document: "c", Unscored: true},
			},
		},
		{
			name:    "a zero score is still scored",
			results: []result{{0, 0}, {1, 0}, {2, 0}},
			want: []DocumentScore{
				{Document: "a"},
				{Document: "b"},
				{Document: "c"},
			},
		},
		{
			name:    "index past the end",
			results: []result{{3, 0.1}},
			wantErr: "reranker result index 3 out of range for 3 documents",
		},
		{
			name:    "negative index",
			results: []result{{-1, 0.1}},
			wantErr: "reranker result index -1 out of range for 3 documents",
		},
		{
			name:    "duplicate index",
			results: []result{{1, 0.1}, {1, 0.2}},
			wantErr: "reranker returned duplicate result index 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToDocumentScores(documents, tt.results, func(r result) (int, float64) {
				return r.index, r.score
			})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
