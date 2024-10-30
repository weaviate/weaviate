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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolWithoutLimit(t *testing.T) {
	// pool with two liss
	pool := NewPool(2, 2, -1)
	assert.Equal(t, 2, pool.Len())

	// get first list
	l1 := pool.Borrow()
	assert.Equal(t, 2, l1.Len())
	assert.Equal(t, 1, pool.Len())

	// get second list
	l2 := pool.Borrow()
	assert.Equal(t, 0, pool.Len())

	// get third list from empty pool and return it back
	l3 := pool.Borrow()
	assert.Equal(t, 0, pool.Len())
	pool.Return(l3)
	assert.Equal(t, 1, pool.Len())

	// get same list again and modify its size
	// so that it is not accepted when returned to the pool
	l3 = pool.Borrow()
	l3.Visit(2)
	pool.Return(l3)
	assert.Equal(t, 0, pool.Len())

	// add two list and destroy the pool
	pool.Return(l1)
	pool.Return(l2)
	assert.Equal(t, 2, pool.Len())
	pool.Destroy()
	assert.Equal(t, 0, pool.Len())
}

func TestPoolWithLimit(t *testing.T) {
	type test struct {
		initialSize int
		maxStorage  int
		borrowCount int
		expected    int
	}

	tests := []test{
		// limited
		{initialSize: 2, maxStorage: 10, borrowCount: 30, expected: 10},
		{initialSize: 10, maxStorage: 10, borrowCount: 10, expected: 10},
		{initialSize: 10, maxStorage: 10, borrowCount: 50, expected: 10},
		{initialSize: 10, maxStorage: 15, borrowCount: 50, expected: 15},

		// unlimited
		{initialSize: 2, maxStorage: -1, borrowCount: 30, expected: 30},
		{initialSize: 100, maxStorage: -1, borrowCount: 30, expected: 100},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("initialSize=%d maxStorage=%d borrowCount=%d expected=%d",
			tt.initialSize, tt.maxStorage, tt.borrowCount, tt.expected)
		t.Run(name, func(t *testing.T) {
			// Test a journey where the pool starts with a small initial size and a
			// fairly generous limit. Then validate that when all lists are returned, the
			// pool does not hold more than the limit.

			pool := NewPool(tt.initialSize, 100, tt.maxStorage)

			if tt.maxStorage == -1 {
				tt.maxStorage = tt.borrowCount
			}

			lists := make([]ListSet, 0, 30)

			// Borrow all lists up to the limit
			for i := 0; i < tt.maxStorage; i++ {
				lists = append(lists, pool.Borrow())
			}

			// pool should now be max(initialSize-tt.maxStorage, 0)
			expected := max(0, tt.initialSize-tt.maxStorage)
			assert.Equal(t, expected, pool.Len())

			// Borrow the remaining lists
			for i := 0; i < tt.borrowCount-tt.maxStorage; i++ {
				lists = append(lists, pool.Borrow())
			}

			// pool should now be max(initialSize-tt.borrowCount, 0)
			expected = max(0, tt.initialSize-tt.borrowCount)
			assert.Equal(t, expected, pool.Len())

			// we should now have borrowCount lists
			assert.Len(t, lists, tt.borrowCount)

			// try to return all lists
			for _, l := range lists {
				pool.Return(l)
			}

			assert.Equal(t, tt.expected, pool.Len())
		})
	}
}
