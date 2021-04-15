package hnsw

import "container/heap"

// An pqItem is something we manage in a priority queue.
type pqItem struct {
	hnswIndex uint64  // The value of the item; arbitrary.
	dist      float32 // The priority of the item in the queue.
	// The heapIndex is needed by update and is maintained by the heap.Interface methods.
	heapIndex int // The index of the item in the heap.
}

// A priorityQueue implements heap.Interface and holds Items.
type priorityQueue []*pqItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].dist > pq[j].dist
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].heapIndex = i
	pq[j].heapIndex = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)
	item.heapIndex = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil      // avoid memory leak
	item.heapIndex = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *priorityQueue) update(item *pqItem, hnswIndex uint64, priority float32) {
	item.hnswIndex = hnswIndex
	item.dist = priority
	heap.Fix(pq, item.heapIndex)
}
