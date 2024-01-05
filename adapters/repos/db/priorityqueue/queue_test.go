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
	expectedResults := []Item[any]{
		{Dist: 0, ID: 0},
		{Dist: 0.222, ID: 3},
		{Dist: 0.23, ID: 1},
		{Dist: 0.8, ID: 2},
		{Dist: 0.88, ID: 4},
		{Dist: 1, ID: 5},
	}

	pq := NewMin[any](6)

	for id, dist := range values {
		pq.Insert(id, dist)
	}

	var results []Item[any]
	for pq.Len() > 0 {
		results = append(results, pq.Pop())
	}

	assert.Equal(t, expectedResults, results)
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
	expectedResults := []Item[any]{
		{Dist: 1, ID: 5},
		{Dist: 0.88, ID: 4},
		{Dist: 0.8, ID: 2},
		{Dist: 0.23, ID: 1},
		{Dist: 0.222, ID: 3},
		{Dist: 0, ID: 0},
	}

	pq := NewMax[any](6)

	for id, dist := range values {
		pq.Insert(id, dist)
	}

	var results []Item[any]
	for pq.Len() > 0 {
		results = append(results, pq.Pop())
	}

	assert.Equal(t, expectedResults, results)
}
