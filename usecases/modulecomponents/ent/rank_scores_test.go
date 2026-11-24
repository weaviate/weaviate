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
	documents := []string{"a", "b", "c"}
	tests := []struct {
		name     string
		results  []IndexedScore
		complete bool
		want     []DocumentScore
		wantErr  string
	}{
		{
			name:     "results in another order",
			results:  []IndexedScore{{2, 0.9}, {0, 0.5}, {1, 0.1}},
			complete: true,
			want:     []DocumentScore{{"a", 0.5}, {"b", 0.1}, {"c", 0.9}},
		},
		{
			name:    "partial response allowed",
			results: []IndexedScore{{2, 0.9}},
			want:    []DocumentScore{{"a", 0}, {"b", 0}, {"c", 0.9}},
		},
		{
			name:    "no results, partial allowed",
			results: nil,
			want:    []DocumentScore{{"a", 0}, {"b", 0}, {"c", 0}},
		},
		{
			name:     "partial response rejected",
			results:  []IndexedScore{{2, 0.9}},
			complete: true,
			wantErr:  "1 results for 3 documents",
		},
		{
			name:    "more results than documents",
			results: []IndexedScore{{0, 1}, {1, 1}, {2, 1}, {3, 1}},
			wantErr: "4 results for 3 documents",
		},
		{
			name:     "index past the end",
			results:  []IndexedScore{{0, 1}, {1, 1}, {3, 1}},
			complete: true,
			wantErr:  "invalid or repeated index 3",
		},
		{
			name:    "index past the end of a partial response",
			results: []IndexedScore{{3, 1}},
			wantErr: "invalid or repeated index 3",
		},
		{
			name:     "negative index",
			results:  []IndexedScore{{0, 1}, {-1, 1}, {2, 1}},
			complete: true,
			wantErr:  "invalid or repeated index -1",
		},
		{
			name:     "repeated index",
			results:  []IndexedScore{{0, 1}, {0, 1}, {2, 1}},
			complete: true,
			wantErr:  "invalid or repeated index 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DocumentScoresByIndex(documents, tt.results, tt.complete)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
