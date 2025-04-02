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

package priorityqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityQueueMin(t *testing.T) {
	values := map[uint64]float32{
		0: 0.0,
		1: 0.23,
		2: 0.8,
		3: 0.222,
		4: 0.88,
		5: 1,
	}
	populateMinPq := func() *Queue[any] {
		pq := NewMin[any](6)
		for id, dist := range values {
			pq.Insert(id, dist)
		}
		return pq
	}

	t.Run("insert", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0, ID: 0},
			{Dist: 0.222, ID: 3},
			{Dist: 0.23, ID: 1},
			{Dist: 0.8, ID: 2},
			{Dist: 0.88, ID: 4},
			{Dist: 1, ID: 5},
		}

		pq := populateMinPq()

		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 1", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0, ID: 0},
			{Dist: 0.222, ID: 3},
			{Dist: 0.23, ID: 1},
			{Dist: 0.88, ID: 4},
			{Dist: 1, ID: 5},
		}

		pq := populateMinPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 2 && item.Dist == 0.8
		})
		notDeleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 100
		})

		assert.True(t, deleted1)
		assert.False(t, notDeleted1)
		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 2", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0, ID: 0},
			{Dist: 0.23, ID: 1},
			{Dist: 0.88, ID: 4},
			{Dist: 1, ID: 5},
		}

		pq := populateMinPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 2 && item.Dist == 0.8
		})
		deleted2 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 3 && item.Dist == 0.222
		})

		assert.True(t, deleted1)
		assert.True(t, deleted2)
		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 3", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0.23, ID: 1},
			{Dist: 0.88, ID: 4},
			{Dist: 1, ID: 5},
		}

		pq := populateMinPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 2 && item.Dist == 0.8)
		})
		deleted2 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 3 && item.Dist == 0.222)
		})
		deleted3 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 0 && item.Dist == 0)
		})

		assert.True(t, deleted1)
		assert.True(t, deleted2)
		assert.True(t, deleted3)
		assertPqElementsMatch(t, expectedResults, pq)
	})
}

func TestPriorityQueueMax(t *testing.T) {
	values := map[uint64]float32{
		0: 0.0,
		1: 0.23,
		2: 0.8,
		3: 0.222,
		4: 0.88,
		5: 1,
	}
	populateMaxPq := func() *Queue[any] {
		pq := NewMax[any](6)
		for id, dist := range values {
			pq.Insert(id, dist)
		}
		return pq
	}

	t.Run("insert", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 1, ID: 5},
			{Dist: 0.88, ID: 4},
			{Dist: 0.8, ID: 2},
			{Dist: 0.23, ID: 1},
			{Dist: 0.222, ID: 3},
			{Dist: 0, ID: 0},
		}

		pq := populateMaxPq()

		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 1", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0.88, ID: 4},
			{Dist: 0.8, ID: 2},
			{Dist: 0.23, ID: 1},
			{Dist: 0.222, ID: 3},
			{Dist: 0, ID: 0},
		}

		pq := populateMaxPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 5 && item.Dist == 1
		})
		notDeleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 100
		})

		assert.True(t, deleted1)
		assert.False(t, notDeleted1)
		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 2", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0.88, ID: 4},
			{Dist: 0.8, ID: 2},
			{Dist: 0.23, ID: 1},
			{Dist: 0, ID: 0},
		}

		pq := populateMaxPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 5 && item.Dist == 1
		})
		deleted2 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 3 && item.Dist == 0.222)
		})

		assert.True(t, deleted1)
		assert.True(t, deleted2)
		assertPqElementsMatch(t, expectedResults, pq)
	})

	t.Run("delete 3", func(t *testing.T) {
		expectedResults := []Item[any]{
			{Dist: 0.88, ID: 4},
			{Dist: 0.23, ID: 1},
			{Dist: 0, ID: 0},
		}

		pq := populateMaxPq()
		deleted1 := pq.DeleteItem(func(item Item[any]) bool {
			return item.ID == 5 && item.Dist == 1
		})
		deleted2 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 3 && item.Dist == 0.222)
		})
		deleted3 := pq.DeleteItem(func(item Item[any]) bool {
			return (item.ID == 2 && item.Dist == 0.8)
		})

		assert.True(t, deleted1)
		assert.True(t, deleted2)
		assert.True(t, deleted3)
		assertPqElementsMatch(t, expectedResults, pq)
	})
}

func assertPqElementsMatch(t *testing.T, expectedResults []Item[any], pq *Queue[any]) {
	var results []Item[any]
	for pq.Len() > 0 {
		results = append(results, pq.Pop())
	}

	assert.ElementsMatch(t, expectedResults, results)
}
