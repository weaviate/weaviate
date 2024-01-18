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

// ListSet is a reusable list with very efficient resets. Inspired by the C++
// implementation in hnswlib it can be reset with zero memory writes in the
// array by moving the match target instead of altering the list. Only after a
// version overflow do we need to actually reset
//
// The new implementation uses a slice where the first element is reserved for the marker.
// This allow us to use ListSet as a value (i.e. no pointer is required)
// The marker (i.e. set[0]) allows for reusing the same list without having to zero all elements on each list reset.
// Resetting the list takes place once the marker (i.e. set[0]) overflows
type ListSet struct {
	set []uint8 // set[0] is reserved for the marker (version)
}

// Len returns the number of elements in the list.
func (l ListSet) Len() int { return len(l.set) - 1 }

// free allocated slice. This list should not be reusable after this call.
func (l *ListSet) free() { l.set = nil }

// NewList creates a new list. It allocates memory for elements and marker
func NewList(size int) ListSet {
	set := make([]uint8, size+1)
	set[0] = 1 // the marker starts always by 1 since on reset all element are set to 0
	return ListSet{set: set}
}

// Visit sets element at node to the marker value
func (l *ListSet) Visit(node uint64) {
	if int(node) >= l.Len() { // resize
		newset := make([]uint8, growth(len(l.set), int(node)+1024))
		copy(newset, l.set)
		l.set = newset
	}
	l.set[node+1] = l.set[0]
}

// Visited checks if l contains the specified node
func (l *ListSet) Visited(node uint64) bool {
	return int(node) < l.Len() && l.set[node+1] == l.set[0]
}

// Reset list only in case of an overflow.
func (l *ListSet) Reset() {
	l.set[0]++
	if l.set[0] == 0 { // if overflowed
		for i := range l.set {
			l.set[i] = 0
		}
		l.set[0] = 1 // restart counting
	}
}

// threshold let us double the size if the old size is below it
const threshold = 2048

// growth calculates the amount a list should grow in a smooth way.
//
// Inspired by the go standard implementation
func growth(oldsize, size int) int {
	doublesize := oldsize << 1
	if size > doublesize {
		return size
	}
	if oldsize < threshold {
		return doublesize // grow by 2x for small slices
	}
	// detect overflow newsize > 0
	// and prevent an infinite loop.
	newsize := oldsize
	for newsize > 0 && newsize < size {
		// grow by 1.25x for large slices
		// This formula allows for smothly growing
		newsize += (newsize + threshold) / 4
	}
	// return requested size in case of overflow
	if newsize <= 0 {
		newsize = size
	}
	return newsize
}
