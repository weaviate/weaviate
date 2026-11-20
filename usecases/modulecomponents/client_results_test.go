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

package modulecomponents

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsEmbeddingEmpty(t *testing.T) {
	vectorTests := []struct {
		name      string
		embedding []float32
		want      bool
	}{
		{name: "nil vector", embedding: nil, want: true},
		{name: "vector without dimensions", embedding: []float32{}, want: true},
		{name: "vector", embedding: []float32{0.1, 0.2}, want: false},
	}
	for _, tt := range vectorTests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsEmbeddingEmpty(tt.embedding))
		})
	}

	multiVectorTests := []struct {
		name      string
		embedding [][]float32
		want      bool
	}{
		{name: "nil multi-vector", embedding: nil, want: true},
		{name: "multi-vector without token vectors", embedding: [][]float32{}, want: true},
		{name: "multi-vector with a nil token vector", embedding: [][]float32{nil}, want: true},
		{name: "multi-vector with an empty token vector", embedding: [][]float32{{}}, want: true},
		{name: "multi-vector with an empty token vector after a valid one", embedding: [][]float32{{0.1, 0.2}, {}}, want: true},
		{name: "multi-vector", embedding: [][]float32{{0.1, 0.2}, {0.3, 0.4}}, want: false},
	}
	for _, tt := range multiVectorTests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsEmbeddingEmpty(tt.embedding))
		})
	}
}
