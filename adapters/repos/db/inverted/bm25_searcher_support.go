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

package inverted

// Functions and data structures to support different methods in caclulating BM25 scores

// The current wand implementation is a hybrid approach, this file supports using heaps or maps for storing and processing the results

import (
	"arena"

	"golang.org/x/exp/constraints"
)

// Heap implementation, using types rather than interfaces to avoid the overhead of interface calls

type PropertyHeaps struct {
	MapPairs *Heap[*docPointerWithScore]
	Propname string
}

type termHeap struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer   uint64
	posPointer  uint64
	data        *Heap[*docPointerWithScore]
	exhausted   bool
	queryTerm   string
	propLengths map[string]int
	propCounts  map[string]int
	UniqueCount int
}

// Using generics or types

// First version wraps go's built in map type, as a comparison
// Second version uses a custom map types and the new allocators, which are very effective

type termMap struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer   uint64
	posPointer  uint64
	data        *CustomMap
	exhausted   bool
	queryTerm   string
	propLengths map[string]int
	propCounts  map[string]int
	UniqueCount int
}

type hashable interface {
	constraints.Integer | constraints.Float | constraints.Complex | ~string | uintptr
}

type GoMap[K hashable, V any] struct {
	m map[K]V
}

// New returns a new HashMap instance with an optional specific initialization size
func NewGoMap[K hashable, V any](size uintptr) *GoMap[K, V] {
	m := &GoMap[K, V]{m: make(map[K]V, size)}
	return m
}

// ForEach iterates over key-value pairs and executes the lambda provided for each such pair
// lambda must return `true` to continue iteration and `false` to break iteration
func (m *GoMap[K, V]) ForEach(lambda func(K, V) bool) {
	for k, v := range m.m {
		if !lambda(k, v) {
			break
		}
	}
}

// Set tries to update an element if key is present else it inserts a new element
func (m *GoMap[K, V]) Set(key K, value V) {
	m.m[key] = value
}

// Get returns the value for the given key
func (m *GoMap[K, V]) Get(key K) (V, bool) {
	v, ok := m.m[key]
	return v, ok
}

// Len returns the number of elements in the map
func (m *GoMap[K, V]) Len() int {
	return len(m.m)
}

// Let's make our own map to compare
type docIdPointer struct {
	id      uint64
	pointer *docPointerWithScore
}

type Pair struct {
	Car *docIdPointer
	Cdr *Pair
}

func cons(arr *arena.Arena, car *docIdPointer, cdr *Pair) *Pair {
	p := arena.New[Pair](arr)
	p.Car = car
	p.Cdr = cdr
	return p
}

type CustomMap struct {
	Data       []*Pair
	length     int
	numBuckets int
	arena      *arena.Arena
}

// New returns a new HashMap instance with an optional specific initialization size
func NewCustomMap(size uintptr) *CustomMap {
	m := &CustomMap{}
	m.arena = arena.NewArena()
	m.Data = arena.MakeSlice[*Pair](m.arena, int(size), int(size))
	m.numBuckets = int(size)

	return m
}

// New returns a new HashMap instance with an optional specific initialization size
func NewCustomMapWithArena(ar *arena.Arena, size uintptr) *CustomMap {
	m := &CustomMap{}
	m.arena = ar
	m.Data = arena.MakeSlice[*Pair](m.arena, int(size), int(size))
	m.numBuckets = int(size)

	return m
}

func (m *CustomMap) Free() {
	m.arena.Free()
}

// ForEach iterates over key-value pairs and executes the lambda provided for each such pair
// lambda must return `true` to continue iteration and `false` to break iteration
func (m *CustomMap) ForEach(lambda func(uint64, *docPointerWithScore) bool) {
	for _, bucket := range m.Data {
		if bucket != nil {
			for v := bucket; v != nil; v = v.Cdr {
				if !lambda(v.Car.id, v.Car.pointer) {
					break
				}
			}
		}
	}
}

// Set tries to update an element if key is present else it inserts a new element
func (m *CustomMap) Set(key uint64, value *docPointerWithScore) {
	index := key % uint64(m.numBuckets)

	// Attempt to find the key in the bucket, and overwrite it if it exists.  If it doesn't exist, append it to the bucket
	bucket := m.Data[index]
	item := arena.New[docIdPointer](m.arena)
	item.id = key
	item.pointer = value

	if bucket != nil {
		for v := bucket; v != nil; v = v.Cdr {
			if v.Car.id == key {
				v.Car = item
				return
			}
		}
	} else {

		m.Data[index] = cons(m.arena, item, nil)
		m.length++
	}
}

// Get returns the value for the given key
func (m *CustomMap) Get(key uint64) (*docPointerWithScore, bool) {
	index := key % uint64(m.numBuckets)
	bucket := m.Data[index]
	if bucket != nil {
		for v := bucket; v != nil; v = v.Cdr {
			if v.Car.id == key {
				return v.Car.pointer, true
			}
		}
	}
	return nil, false
}

// Len returns the number of elements in the map
func (m *CustomMap) Len() int {
	return m.length
}
