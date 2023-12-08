//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package priorityqueue

type Rescored = bool

type Index = uint64

type valueType interface {
	Rescored | Index
}

type Item[T valueType] struct {
	ID    uint64
	Dist  float32
	Value T
}

type Queue[T valueType] struct {
	items []Item[T]
	less  func(items []Item[T], i, j int) bool
}

func NewMin[T valueType](capacity int) *Queue[T] {
	return &Queue[T]{
		items: make([]Item[T], 0, capacity),
		less: func(items []Item[T], i, j int) bool {
			return items[i].Dist < items[j].Dist
		},
	}
}

func NewMax[T valueType](capacity int) *Queue[T] {
	return &Queue[T]{
		items: make([]Item[T], 0, capacity),
		less: func(items []Item[T], i, j int) bool {
			return items[i].Dist > items[j].Dist
		},
	}
}

func (q *Queue[T]) Insert(id uint64, distance float32, val T) int {
	q.items = append(q.items, Item[T]{
		ID:    id,
		Dist:  distance,
		Value: val,
	})
	i := len(q.items) - 1
	for i != 0 && q.less(q.items, i, q.parent(i)) {
		q.swap(i, q.parent(i))
		i = q.parent(i)
	}
	return i
}

func (q *Queue[T]) Pop() Item[T] {
	out := q.items[0]
	q.items[0] = q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	q.heapify(0)
	return out
}

func (q *Queue[T]) Top() Item[T] {
	return q.items[0]
}

func (q *Queue[T]) Len() int {
	return len(q.items)
}

func (q *Queue[T]) Cap() int {
	return cap(q.items)
}

func (q *Queue[T]) Reset() {
	q.items = q.items[:0]
}

func (q *Queue[T]) ResetCap(capacity int) {
	q.items = make([]Item[T], 0, capacity)
}

func (q *Queue[T]) left(i int) int {
	return 2*i + 1
}

func (q *Queue[T]) right(i int) int {
	return 2*i + 2
}

func (q *Queue[T]) parent(i int) int {
	return (i - 1) / 2
}

func (q *Queue[T]) swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *Queue[T]) heapify(i int) {
	left := q.left(i)
	right := q.right(i)
	smallest := i
	if left < len(q.items) && q.less(q.items, left, i) {
		smallest = left
	}

	if right < len(q.items) && q.less(q.items, right, smallest) {
		smallest = right
	}

	if smallest != i {
		q.swap(i, smallest)
		q.heapify(smallest)
	}
}
