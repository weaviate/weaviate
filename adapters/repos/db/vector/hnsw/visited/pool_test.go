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

package visited

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

			lists := make([]*SparseSet, 0, 30)

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
