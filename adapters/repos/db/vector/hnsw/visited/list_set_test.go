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

func TestVisitedList(t *testing.T) {
	t.Run("creating a new list, filling it and checking against it", func(t *testing.T) {
		l := NewList(1000)

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

	t.Run("reusing a list it is not affected by past entries", func(t *testing.T) {
		l := NewList(1000)

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

	t.Run("it creates no false positives after a version overflow (v=1)", func(t *testing.T) {
		l := NewList(1000)

		for i := 0; i < 255; i++ {
			l.Reset()
		}

		// verify the test is correct and we are indeed at the version we think we are
		assert.Equal(t, uint8(1), l.set[0])

		// verify there are zero visited nodes
		for i := uint64(0); i < 1000; i++ {
			assert.False(t, l.Visited(i), "node should not be visited")
		}
	})

	t.Run("it creates no false positives after a version overflow (v=1)", func(t *testing.T) {
		l := NewList(1000)

		// mark every node as visited in version==1
		for i := uint64(0); i < 1000; i++ {
			l.Visit(i)
		}

		// v==0 does not exist, so we only need 255 runs to be at version==1 again
		for i := 0; i < 255; i++ {
			l.Reset()
		}

		// verify the test is correct and we are indeed at the version we think we are
		assert.Equal(t, l.set[0], uint8(1))

		// verify there are zero visited nodes
		for i := uint64(0); i < 1000; i++ {
			assert.False(t, l.Visited(i), "node should not be visited")
		}
	})
}

func TestListSetResize(t *testing.T) {
	l := NewList(2)
	assert.Equal(t, []uint8{1, 0, 0}, l.set)
	assert.Equal(t, l.Len(), 2)
	l.Visit(1)
	assert.Equal(t, []uint8{1, 0, 1}, l.set)
	assert.Equal(t, l.Len(), 2)
	l.Reset()
	assert.Equal(t, []uint8{2, 0, 1}, l.set)
	assert.Equal(t, l.Len(), 2)
	l.Visit(1)
	assert.Equal(t, []uint8{2, 0, 2}, l.set)
	assert.Equal(t, l.Len(), (2))
	l.Visit(3)
	assert.Equal(t, []uint8{2, 0, 2, 0, 2}, l.set[0:5])
	assert.Equal(t, (2 + 1024), l.Len())
	l.free()
	assert.Equal(t, []uint8(nil), l.set)
}

func TestGrowth(t *testing.T) {
	MaxInt := 1<<63 - 1 // math.MaxInt needs go >= 1.17
	tests := []struct {
		old  int
		new  int
		want int
	}{
		{512, 1000, 1024},
		{1024, 1048, 2048},
		{2000, 3500, 4000},
		{3500, 4500, 4887},
		{threshold, threshold + 32, threshold + threshold/2},
		{2097152, 4194304, 5122952},
		{threshold, MaxInt, MaxInt},
		{MaxInt / 2, MaxInt - 1, MaxInt - 1},
	}
	for _, tc := range tests {
		got := growth(tc.old, tc.new)
		if got != tc.want {
			t.Errorf("growth(%d,%d) got:%d want:%d", tc.old, tc.new, got, tc.want)
		}
	}
}

func insertItems() {
	list := NewList(1024)
	for i := uint64(1); i < 8000000; i++ {
		list.Visit(i)
	}
}

func BenchmarkListInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		insertItems()
	}
}
