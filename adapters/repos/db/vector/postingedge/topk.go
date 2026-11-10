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
	"slices"
)

// scored is a vector (or centroid) id with its distance to a query.
type scored struct {
	id   uint64
	dist float32
}

// worse orders two entries by distance and then by id, so that selections are
// deterministic when distances tie.
func worse(a, b scored) bool {
	if a.dist != b.dist {
		return a.dist > b.dist
	}
	return a.id > b.id
}

// topK keeps the limit best entries pushed into it. It is a bounded max-heap:
// the root is the worst entry kept, which is the only one a push has to beat.
type topK struct {
	items []scored
	limit int
}

// reset empties the heap and sets how many entries it keeps.
func (t *topK) reset(limit int) {
	t.items = t.items[:0]
	t.limit = limit
}

func (t *topK) len() int {
	return len(t.items)
}

// worst is the distance a new entry has to beat to be kept: +Inf while the
// heap still has room.
func (t *topK) worst() float32 {
	if len(t.items) < t.limit {
		return float32(math.Inf(1))
	}
	return t.items[0].dist
}

// push offers an entry. A NaN distance is never kept: it compares false
// against everything and would break the heap order.
func (t *topK) push(id uint64, dist float32) {
	if dist != dist || t.limit <= 0 {
		return
	}
	e := scored{id: id, dist: dist}
	if len(t.items) < t.limit {
		t.items = append(t.items, e)
		t.siftUp(len(t.items) - 1)
		return
	}
	if !worse(t.items[0], e) {
		return
	}
	t.items[0] = e
	t.siftDown(0)
}

func (t *topK) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !worse(t.items[i], t.items[parent]) {
			return
		}
		t.items[i], t.items[parent] = t.items[parent], t.items[i]
		i = parent
	}
}

func (t *topK) siftDown(i int) {
	n := len(t.items)
	for {
		largest := i
		if l := 2*i + 1; l < n && worse(t.items[l], t.items[largest]) {
			largest = l
		}
		if r := 2*i + 2; r < n && worse(t.items[r], t.items[largest]) {
			largest = r
		}
		if largest == i {
			return
		}
		t.items[i], t.items[largest] = t.items[largest], t.items[i]
		i = largest
	}
}

// sorted orders the kept entries best first and returns them. The slice
// aliases the heap, which must be reset before it is used again.
func (t *topK) sorted() []scored {
	slices.SortFunc(t.items, func(a, b scored) int {
		switch {
		case worse(b, a):
			return -1
		case worse(a, b):
			return 1
		}
		return 0
	})
	return t.items
}
