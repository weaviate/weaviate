//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package priorityqueue

type Item struct {
	ID   uint64
	Dist float32
}

type Queue struct {
	items []Item
	less  func(items []Item, i, j int) bool
}

func NewMin(capacity int) *Queue {
	return &Queue{
		items: make([]Item, 0, capacity),
		less: func(items []Item, i, j int) bool {
			return items[i].Dist < items[j].Dist
		},
	}
}

func NewMax(capacity int) *Queue {
	return &Queue{
		items: make([]Item, 0, capacity),
		less: func(items []Item, i, j int) bool {
			return items[i].Dist > items[j].Dist
		},
	}
}

func (l *Queue) left(i int) int {
	return 2*i + 1
}

func (l *Queue) right(i int) int {
	return 2*i + 2
}

func (l *Queue) parent(i int) int {
	return (i - 1) / 2
}

func (l *Queue) swap(i, j int) {
	l.items[i], l.items[j] = l.items[j], l.items[i]
}

func (l *Queue) heapify(i int) {
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

func (l *Queue) Insert(id uint64, dist float32) {
	l.items = append(l.items, Item{id, dist})
	i := len(l.items) - 1
	for i != 0 && l.less(l.items, i, l.parent(i)) {
		l.swap(i, l.parent(i))
		i = l.parent(i)
	}
}

func (l *Queue) Pop() Item {
	out := l.items[0]
	l.items[0] = l.items[len(l.items)-1]
	l.items = l.items[:len(l.items)-1]
	l.heapify(0)
	return out
}

func (l *Queue) Top() Item {
	return l.items[0]
}

func (l *Queue) Len() int {
	return len(l.items)
}
