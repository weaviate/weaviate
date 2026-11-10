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

package postingedge

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopKKeepsTheBest(t *testing.T) {
	nan := float32(math.NaN())
	tests := []struct {
		name   string
		limit  int
		pushes []scored
		want   []scored
	}{
		{
			name:   "fewer entries than the limit",
			limit:  4,
			pushes: []scored{{1, 0.5}, {2, 0.1}},
			want:   []scored{{2, 0.1}, {1, 0.5}},
		},
		{
			name:   "keeps the smallest distances",
			limit:  2,
			pushes: []scored{{1, 0.9}, {2, 0.1}, {3, 0.5}, {4, 0.3}, {5, 0.7}},
			want:   []scored{{2, 0.1}, {4, 0.3}},
		},
		{
			name:   "ties are broken by the smaller id whatever the push order",
			limit:  2,
			pushes: []scored{{9, 0.5}, {3, 0.5}, {7, 0.5}, {1, 0.5}},
			want:   []scored{{1, 0.5}, {3, 0.5}},
		},
		{
			name:   "nan distances are never kept",
			limit:  3,
			pushes: []scored{{1, nan}, {2, 0.4}, {3, nan}},
			want:   []scored{{2, 0.4}},
		},
		{
			name:   "a zero limit keeps nothing",
			limit:  0,
			pushes: []scored{{1, 0.1}},
			want:   []scored{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var heap topK
			heap.reset(tt.limit)
			for _, p := range tt.pushes {
				heap.push(p.id, p.dist)
			}
			assert.Equal(t, tt.want, append([]scored{}, heap.sorted()...))
		})
	}
}

func TestTopKWorstIsTheThreshold(t *testing.T) {
	var heap topK
	heap.reset(3)
	assert.True(t, math.IsInf(float64(heap.worst()), 1), "an empty heap accepts anything")

	heap.push(1, 0.3)
	heap.push(2, 0.9)
	assert.True(t, math.IsInf(float64(heap.worst()), 1), "a heap with room accepts anything")

	heap.push(3, 0.6)
	assert.Equal(t, float32(0.9), heap.worst())

	heap.push(4, 0.1)
	assert.Equal(t, float32(0.6), heap.worst())

	heap.reset(1)
	assert.Equal(t, 0, heap.len())
	assert.True(t, math.IsInf(float64(heap.worst()), 1), "reset empties the heap")
}

func TestTopKMatchesSorting(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	for round := 0; round < 50; round++ {
		n, limit := 1+rng.IntN(300), 1+rng.IntN(40)
		all := make([]scored, n)
		var heap topK
		heap.reset(limit)
		for i := range all {
			// few distinct distances, so ties are common
			all[i] = scored{id: uint64(i), dist: float32(rng.IntN(20))}
			heap.push(all[i].id, all[i].dist)
		}
		slices.SortFunc(all, func(a, b scored) int {
			switch {
			case worse(b, a):
				return -1
			case worse(a, b):
				return 1
			}
			return 0
		})
		require.Equal(t, all[:min(limit, n)], heap.sorted())
	}
}
