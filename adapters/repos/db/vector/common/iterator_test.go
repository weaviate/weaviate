//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import "testing"

func Test_SparseFisherYatesIterator(t *testing.T) {
	// Test the SparseFisherYatesSampler
	size := 10
	sampler := NewSparseFisherYatesIterator(size)
	sampledIndices := make(map[int]bool)

	for i := 0; i < size; i++ {
		index := sampler.Next()
		if index == nil {
			t.Fatalf("expected index, got nil at iteration %d", i)
		}
		if *index < 0 || *index >= size {
			t.Fatalf("index out of bounds: got %d, expected between 0 and %d", *index, size-1)
		}
		if _, exists := sampledIndices[*index]; exists {
			t.Fatalf("duplicate index: %d", *index)
		}
		sampledIndices[*index] = true
	}

	if !sampler.IsDone() {
		t.Fatalf("expected sampler to be done after %d iterations", size)
	}

	// Ensure that all indices were sampled
	if len(sampledIndices) != size {
		t.Fatalf("expected %d unique indices, got %d", size, len(sampledIndices))
	}
}
