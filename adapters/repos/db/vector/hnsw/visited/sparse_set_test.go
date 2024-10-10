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

package visited

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparseVisitedList(t *testing.T) {
	t.Run("reusing a list it is not affected by past entries", func(t *testing.T) {
		s := NewSparseSet(totalSize, collisionRate)
		s.Visit(uint64(500067347))
	})

	t.Run("creating a new list, filling it and checking against it", func(t *testing.T) {
		l := NewSparseSet(1000, 16)

		l.Visit(7)
		l.Visit(38)
		l.Visit(999)

		assert.True(t, l.Visited(7), "visited node should be marked visited")
		assert.True(t, l.Visited(38), "visited node should be marked visited")
		assert.True(t, l.Visited(999), "visited node should be marked visited")
		assert.False(t, l.Visited(6), "unvisited node should NOT be marked visited")
		assert.False(t, l.Visited(37), "unvisited node should NOT be marked visited")
		assert.False(t, l.Visited(998), "unvisited node should NOT be marked visited")
	})

	t.Run("creating a new list, filling it with collisions and checking against it", func(t *testing.T) {
		l := NewSparseSet(1000, 32)

		l.Visit(7)
		l.Visit(5)
		l.Visit(1)

		assert.True(t, l.Visited(7), "visited node should be marked visited")
		assert.True(t, l.Visited(5), "visited node should be marked visited")
		assert.True(t, l.Visited(1), "visited node should be marked visited")
		assert.False(t, l.Visited(6), "unvisited node should NOT be marked visited")
		assert.False(t, l.Visited(31), "unvisited node should NOT be marked visited")
		assert.False(t, l.Visited(998), "unvisited node should NOT be marked visited")
	})

	t.Run("reusing a list it is not affected by past entries", func(t *testing.T) {
		l := NewSparseSet(1000, 16)

		l.Visit(7)
		l.Visit(38)
		l.Visit(999)

		l.Reset()

		l.Visit(6)
		l.Visit(37)
		l.Visit(998)

		assert.False(t, l.Visited(7), "an entry before the reset has no influence")
		assert.False(t, l.Visited(38), "an entry before the reset has no influence")
		assert.False(t, l.Visited(999), "an entry before the reset has no influence")
		assert.False(t, l.Visited(20), "an entry never visited is not visited")
		assert.True(t, l.Visited(6), "a node visited in this round is marked as such")
		assert.True(t, l.Visited(37), "a node visited in this round is marked as such")
		assert.True(t, l.Visited(998), "a node visited in this round is marked as such")
	})
}
