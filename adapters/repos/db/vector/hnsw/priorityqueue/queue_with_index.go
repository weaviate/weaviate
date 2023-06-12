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

// TODO: this is almost an exact copy of Queue just with a different payload.
// Using Go Generics in 1.18 we could probably simplify this a bit

type ItemWithIndex struct {
	ID    uint64
	Index uint64
	Dist  float32
}

type QueueWithIndex struct {
	items []ItemWithIndex
	less  func(items []ItemWithIndex, i, j int) bool
}

func NewMinWithIndex(capacity int) *QueueWithIndex {
	return &QueueWithIndex{
		items: make([]ItemWithIndex, 0, capacity),
		less: func(items []ItemWithIndex, i, j int) bool {
			return items[i].Dist < items[j].Dist
		},
	}
}

func NewMaxWithIndex(capacity int) *QueueWithIndex {
	return &QueueWithIndex{
		items: make([]ItemWithIndex, 0, capacity),
		less: func(items []ItemWithIndex, i, j int) bool {
			return items[i].Dist > items[j].Dist
		},
	}
}

func (l *QueueWithIndex) left(i int) int {
	return 2*i + 1
}

func (l *QueueWithIndex) right(i int) int {
	return 2*i + 2
}

func (l *QueueWithIndex) parent(i int) int {
	return (i - 1) / 2
}

func (l *QueueWithIndex) swap(i, j int) {
	l.items[i], l.items[j] = l.items[j], l.items[i]
}

func (l *QueueWithIndex) heapify(i int) {
	left := l.left(i)
	right := l.right(i)
	smallest := i
	if left < len(l.items) && l.less(l.items, left, i) {
		smallest = left
	}

	if right < len(l.items) && l.less(l.items, right, smallest) {
		smallest = right
	}

	if smallest != i {
		l.swap(i, smallest)
		l.heapify(smallest)
	}
}

func (l *QueueWithIndex) FirstUnRescored() int {
	return 0
}

func (l *QueueWithIndex) ReSort(id uint64, distance float32) {
	panic("Not implemented yet")
}

func (l *QueueWithIndex) Insert(id uint64, index uint64, distance float32) int {
	l.items = append(l.items, ItemWithIndex{id, index, distance})
	i := len(l.items) - 1
	for i != 0 && l.less(l.items, i, l.parent(i)) {
		l.swap(i, l.parent(i))
		i = l.parent(i)
	}
	return i
}

func (l *QueueWithIndex) Pop() ItemWithIndex {
	out := l.items[0]
	l.items[0] = l.items[len(l.items)-1]
	l.items = l.items[:len(l.items)-1]
	l.heapify(0)
	return out
}

func (l *QueueWithIndex) Top() ItemWithIndex {
	return l.items[0]
}

func (*QueueWithIndex) Last() (Item, bool) {
	panic("Not implemented yet")
}

func (l *QueueWithIndex) Items(k int) ([]uint64, []float32) {
	panic("Not implemented yet")
}

func (l *QueueWithIndex) Len() int {
	return len(l.items)
}

func (l *QueueWithIndex) Cap() int {
	return cap(l.items)
}

func (l *QueueWithIndex) Reset() {
	l.items = l.items[:0]
}

func (l *QueueWithIndex) ResetCap(capacity int) {
	l.items = make([]ItemWithIndex, 0, capacity)
}
