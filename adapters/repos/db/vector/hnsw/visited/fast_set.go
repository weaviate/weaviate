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

// FastSet is a high-performance visited set using open addressing with linear probing.
// It's optimized for uint64 keys and provides better cache locality than Go's built-in map.
//
// Key optimizations:
// - Open addressing with linear probing (no pointer chasing)
// - Power-of-2 sizing for fast modulo (bitwise AND)
// - Simple bit mixing hash (fast for sequential/clustered IDs)
// - Marker-based O(1) reset (same trick as ListSet)
type FastSet struct {
	keys     []uint64
	markers  []uint8
	marker   uint8
	size     int // number of valid entries
	capacity int // always power of 2
	mask     uint64
}

// NewFastSet creates a new fast visited set.
// capacity is rounded up to the next power of 2.
func NewFastSet(capacity int) FastSet {
	if capacity < 16 {
		capacity = 16
	}
	// Round up to power of 2
	capacity = nextPowerOf2(capacity)

	return FastSet{
		keys:     make([]uint64, capacity),
		markers:  make([]uint8, capacity),
		marker:   1,
		size:     0,
		capacity: capacity,
		mask:     uint64(capacity - 1),
	}
}

func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

// hash uses a fast bit-mixing function optimized for uint64.
// This is faster than Go's built-in map hash for integer keys.
//
//	 This uses  Fibonacci Hashing:
//	- Multiplies the key by the golden ratio constant (2⁶⁴ / φ)
//	- This "scrambles" the bits so sequential IDs (0, 1, 2, 3...) spread across the table
//	- & f.mask is a fast modulo (works because capacity is power-of-2)
func (f *FastSet) hash(key uint64) uint64 {
	// Fibonacci hashing - good distribution, very fast
	// Golden ratio: 2^64 / phi = 11400714819323198485
	return (key * 11400714819323198485) & f.mask
}

// Visit marks a node as visited.
func (f *FastSet) Visit(node uint64) {
	idx := f.hash(node)
	// Cache struct fields in locals so the compiler can keep them in registers
	// across loop iterations, avoiding repeated loads through the f pointer.
	markers := f.markers
	keys := f.keys
	marker := f.marker
	mask := f.mask

	// Linear probing
	for {
		if markers[idx] != marker {
			// Empty slot - insert here
			keys[idx] = node
			markers[idx] = marker
			f.size++

			// Check load factor (75%)
			if f.size*4 > f.capacity*3 {
				f.grow()
			}
			return
		}
		if keys[idx] == node {
			// Already visited
			return
		}
		// Collision - probe next
		idx = (idx + 1) & mask
	}
}

// Visited checks if a node was visited.
func (f *FastSet) Visited(node uint64) bool {
	idx := f.hash(node)
	// Cache struct fields in locals so the compiler can keep them in registers
	// across loop iterations, avoiding repeated loads through the f pointer.
	markers := f.markers
	keys := f.keys
	marker := f.marker
	mask := f.mask

	// Linear probing
	for {
		if markers[idx] != marker {
			// Empty slot - not found
			return false
		}
		if keys[idx] == node {
			return true
		}
		// Collision - probe next
		idx = (idx + 1) & mask
	}
}

// Reset clears the set for reuse. O(1) in most cases.
func (f *FastSet) Reset() {
	f.marker++
	f.size = 0
	if f.marker == 0 {
		// Overflow - must clear markers
		for i := range f.markers {
			f.markers[i] = 0
		}
		f.marker = 1
	}
}

// grow doubles the capacity and rehashes all entries.
func (f *FastSet) grow() {
	oldKeys := f.keys
	oldMarkers := f.markers
	oldMarker := f.marker

	newCap := f.capacity * 2
	f.keys = make([]uint64, newCap)
	f.markers = make([]uint8, newCap)
	f.capacity = newCap
	f.mask = uint64(newCap - 1)
	f.size = 0

	// Rehash all valid entries
	for i, m := range oldMarkers {
		if m == oldMarker {
			f.Visit(oldKeys[i])
		}
	}
}

// Len returns the number of visited nodes.
func (f *FastSet) Len() int {
	return f.size
}

// Cap returns the current capacity.
func (f *FastSet) Cap() int {
	return f.capacity
}
