//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package visited

// List is a reusable list with very efficient resets. Inspired by the C++
// implementation in hnswlib it can be reset with zero memrory writes in the
// array by moving the match target instead of altering the list. Only after a
// version overflow do we need to actually reset
type List struct {
	store   []uint8
	version uint8
	size    uint64
}

func NewList(size int) *List {
	return &List{
		// start at 1 since the initial value of the list is already 0, so we need
		// something to differentiate from that
		version: 1,
		store:   make([]uint8, size),
		size:    uint64(size),
	}
}

func (l *List) Visit(node uint64) {
	if node >= l.size {
		l.resize(node + 1000)
	}

	l.store[node] = l.version
}

func (l *List) Visited(node uint64) bool {
	if node >= l.size {
		l.resize(node + 1000)
	}

	return l.store[node] == l.version
}

func (l *List) resize(target uint64) {
	newStore := make([]uint8, target)
	copy(newStore, l.store)
	l.store = newStore
	l.size = target
}

func (l *List) Reset() {
	l.version++

	if l.version == 0 {
		// 0 is not a valid version because it conflicts with the initial value of
		// the array
		l.version = 1
	}

	// we have overflowed and need an actual reset
	if l.version == 1 {
		for i := range l.store {
			l.store[i] = 0
		}
	}
}
