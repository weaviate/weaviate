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

import (
	"math/rand"
)

// Iterator interface defines the methods for sampling elements.
type Iterator interface {
	Next() (int, error)
	IsDone() bool
}

// SparseFisherYatesIterator implements the Iterator interface using the Sparse Fisher-Yates algorithm.
type SparseFisherYatesIterator struct {
	size            int
	swapHistory     map[int]int
	currentPosition int
}

// NewSparseFisherYatesIterator creates a new SparseFisherYatesIterator with the given size.
func NewSparseFisherYatesIterator(size int) *SparseFisherYatesIterator {
	return &SparseFisherYatesIterator{
		size:            size,
		swapHistory:     make(map[int]int),
		currentPosition: 0,
	}
}

// Next returns the next sampled index using the Sparse Fisher-Yates algorithm.
func (s *SparseFisherYatesIterator) Next() *int {
	// Sparse Fisher Yates sampling algorithm to choose random element
	if s.currentPosition >= s.size {
		return nil
	}
	randIndex := rand.Intn(s.size - s.currentPosition)
	chosenIndex, ok := s.swapHistory[randIndex]
	if !ok {
		chosenIndex = randIndex
	}
	currentIndex, ok := s.swapHistory[s.size-s.currentPosition-1]
	if !ok {
		currentIndex = s.size - s.currentPosition - 1
	}
	s.swapHistory[randIndex] = currentIndex
	delete(s.swapHistory, s.size-s.currentPosition-1)
	s.currentPosition++
	return &chosenIndex
}

// IsDone checks if all elements have been sampled.
func (s *SparseFisherYatesIterator) IsDone() bool {
	return s.currentPosition >= s.size
}
