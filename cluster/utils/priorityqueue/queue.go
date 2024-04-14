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

// StringPriorityQueue is a max-heap priority queue for string items.
type StringPriorityQueue struct {
	items []string
}

// NewMaxStringPriorityQueue constructs a priority queue with the specified initial capacity (initial length is always 0).
func NewMaxStringPriorityQueue(capacity int) *StringPriorityQueue {
	return &StringPriorityQueue{
		items: make([]string, 0, capacity),
	}
}

// Pop removes the next max string in the queue and returns it.
func (q *StringPriorityQueue) Pop() string {
	if len(q.items) == 0 {
		panic("string priority queue is empty")
	}
	out := q.items[0]
	q.items[0] = q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	q.heapify(0)
	return out
}

// Top peeks at the next item in the queue.
func (q *StringPriorityQueue) Top() string {
	return q.items[0]
}

// Len returns the length of the queue.
func (q *StringPriorityQueue) Len() int {
	return len(q.items)
}

// Cap returns the remaining capacity of the queue.
func (q *StringPriorityQueue) Cap() int {
	return cap(q.items)
}

// Reset clears all strings from the queue.
func (q *StringPriorityQueue) Reset() {
	q.items = q.items[:0]
}

// ResetCap drops existing queue strings, and allocates a new queue with the given capacity.
func (q *StringPriorityQueue) ResetCap(capacity int) {
	q.items = make([]string, 0, capacity)
}

// Insert adds the provided string to the queue.
func (q *StringPriorityQueue) Insert(item string) int {
	return q.insert(item)
}

func (q *StringPriorityQueue) insert(item string) int {
	q.items = append(q.items, item)
	i := len(q.items) - 1
	for i != 0 && q.items[i] > q.items[q.parent(i)] {
		q.swap(i, q.parent(i))
		i = q.parent(i)
	}
	return i
}

func (q *StringPriorityQueue) left(i int) int { return 2*i + 1 }

func (q *StringPriorityQueue) right(i int) int { return 2*i + 2 }

func (q *StringPriorityQueue) parent(i int) int { return (i - 1) / 2 }

func (q *StringPriorityQueue) swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

// heapify maintains the max-heap property.
func (q *StringPriorityQueue) heapify(i int) {
	left := q.left(i)
	right := q.right(i)
	largest := i
	if left < len(q.items) && q.items[left] > q.items[i] {
		largest = left
	}

	if right < len(q.items) && q.items[right] > q.items[largest] {
		largest = right
	}

	if largest != i {
		q.swap(i, largest)
		q.heapify(largest)
	}
}
