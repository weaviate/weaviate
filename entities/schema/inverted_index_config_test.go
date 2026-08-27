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

package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// b=0 (no length normalization) and k1=0 (IDF-only scoring) are valid settings
// that Weaviate honors, so they must survive serialization rather than being
// dropped as Go zero values.
func TestBM25ConfigSerializesZeroValues(t *testing.T) {
	tests := []struct {
		name     string
		bm25     BM25Config
		expected string
	}{
		{
			name:     "both zero",
			bm25:     BM25Config{B: 0, K1: 0},
			expected: `{"b":0,"k1":0}`,
		},
		{
			name:     "b zero, k1 default",
			bm25:     BM25Config{B: 0, K1: 1.2},
			expected: `{"b":0,"k1":1.2}`,
		},
		{
			name:     "k1 zero, b default",
			bm25:     BM25Config{B: 0.75, K1: 0},
			expected: `{"b":0.75,"k1":0}`,
		},
		{
			name:     "defaults",
			bm25:     BM25Config{B: 0.75, K1: 1.2},
			expected: `{"b":0.75,"k1":1.2}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := InvertedIndexConfigToModel(InvertedIndexConfig{BM25: test.bm25})

			marshalled, err := json.Marshal(model.Bm25)
			require.Nil(t, err)
			require.Equal(t, test.expected, string(marshalled))

			var roundTripped models.BM25Config
			require.Nil(t, json.Unmarshal(marshalled, &roundTripped))
			require.Equal(t, *model.Bm25, roundTripped)
		})
	}
}
