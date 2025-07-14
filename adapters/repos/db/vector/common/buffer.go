//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

const (
	flatDefaultInitCapacity = 1000       // Default initial capacity for buffers
	flatMaxReusableCapacity = 10_000_000 // Maximum reusable capacity for flat buffers
	flatMinimumGrowthDelta  = 2000       // Minimum growth delta for flat buffers
	pagedDefaultPageSize    = 512        // Default page size for paged buffers
	pagedDefaultInitPages   = 10         // Default initial number of pages for paged buffers
	pagedMinimumGrowthDelta = 10         // Minimum growth delta for paged buffers
)

// FlatBuffer is a thread-safe buffer that stores elements in a contiguous slice.
// It is optimized for cases where random access speed is more important than memory usage.
// The buffer will grow as needed.
type FlatBuffer[T any] struct {
	locks *ShardedRWLocks
	buf   []T
}

func NewFlatBuffer[T any](initSize uint64) *FlatBuffer[T] {
	if initSize == 0 {
		initSize = flatDefaultInitCapacity
	}

	return &FlatBuffer[T]{
		locks: NewShardedRWLocks(512),
		buf:   make([]T, initSize),
	}
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (f *FlatBuffer[T]) Get(id uint64) T {
	f.locks.RLock(id)
	if int(id) >= len(f.buf) {
		f.locks.RUnlock(id)
		var zero T
		return zero
	}
	v := f.buf[id]
	f.locks.RUnlock(id)
	return v
}

// Set sets the element at the given index.
// If the index is out of bounds, it will grow the array.
func (f *FlatBuffer[T]) Set(id uint64, value T) {
	f.locks.Lock(id)
	if int(id) >= len(f.buf) {
		f.locks.Unlock(id)
		f.Grow(id + flatMinimumGrowthDelta)
		f.locks.Lock(id) // Re-lock after growing
	}
	f.buf[id] = value
	f.locks.Unlock(id)
}

// Delete sets the element at the given index to zero value of T.
func (f *FlatBuffer[T]) Delete(index uint64) {
	f.locks.Lock(index)

	if int(index) >= len(f.buf) {
		f.locks.Unlock(index)
		return
	}

	var zero T
	f.buf[index] = zero
	f.locks.Unlock(index)
}

func (f *FlatBuffer[T]) Grow(newSize uint64) {
	f.locks.RLock(0)
	if newSize <= uint64(len(f.buf)) {
		f.locks.RUnlock(0)
		return // No need to grow
	}
	f.locks.RUnlock(0)
	f.locks.LockAll()
	f.grow(newSize)
	f.locks.UnlockAll()
}

func (f *FlatBuffer[T]) grow(newSize uint64) {
	if newSize < uint64(len(f.buf)) {
		return
	}

	if int(newSize) > cap(f.buf) {
		newArray := make([]T, newSize)
		copy(newArray, f.buf)
		f.buf = newArray
		return
	}
	f.buf = f.buf[:newSize]
}

// Reset clears the underlying slice and reuses the existing memory.
// This is useful to avoid memory allocations when the buffer is reused frequently.
func (f *FlatBuffer[T]) Reset() {
	f.locks.LockAll()
	// If the buffer is too large, we reset it to the default initial capacity
	if cap(f.buf) > flatMaxReusableCapacity {
		f.buf = make([]T, flatDefaultInitCapacity)
	} else {
		f.buf = f.buf[:flatDefaultInitCapacity]
		clear(f.buf)
	}
	f.locks.UnlockAll()
}

// Cap returns the current capacity of the underlying slice.
func (f *FlatBuffer[T]) Cap() int {
	f.locks.RLock(0)
	c := cap(f.buf)
	f.locks.RUnlock(0)
	return c
}

// Len returns the size of the buffer.
func (f *FlatBuffer[T]) Len() int {
	f.locks.RLock(0)
	l := len(f.buf)
	f.locks.RUnlock(0)
	return l
}

// PagedBuffer is a thread-safe buffer thatstores elements in pages of a fixed size.
// It is optimized for cases where the buffer is sparse and the number of elements is not known in advance.
// The buffer will grow as needed.
type PagedBuffer[T any] struct {
	locks    *ShardedRWLocks
	buf      [][]T
	pageSize uint64
}

func NewPagedBuffer[T any](pageSize uint64) *PagedBuffer[T] {
	if pageSize == 0 {
		pageSize = pagedDefaultPageSize
	}

	return &PagedBuffer[T]{
		locks:    NewShardedRWLocks(32),
		pageSize: pageSize,
	}
}

// Get returns the element at the given index.
// If the element is not in the buffer, it will return nil.
func (p *PagedBuffer[T]) Get(id uint64) T {
	pageID := id / p.pageSize

	p.locks.RLock(pageID)
	if len(p.buf) <= int(pageID) || p.buf[pageID] == nil {
		p.locks.RUnlock(pageID)
		var zero T
		return zero
	}

	slotID := id % p.pageSize

	v := p.buf[pageID][slotID]
	p.locks.RUnlock(pageID)
	return v
}

// Set sets the element at the given index.
// If the page does not exist, it will be created.
func (p *PagedBuffer[T]) Set(id uint64, value T) {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	// get the current size of the buffer
	p.locks.RLock(0)
	size := uint64(len(p.buf))
	p.locks.RUnlock(0)

	if pageID >= size {
		newSize := max(pageID+pagedMinimumGrowthDelta, size*2)
		p.Grow(newSize)
	}

	p.locks.Lock(pageID)
	if p.buf[pageID] == nil {
		p.buf[pageID] = make([]T, p.pageSize)
	}

	p.buf[pageID][slotID] = value
	p.locks.Unlock(pageID)
}

// Delete sets the element at the given index to zero value of T.
func (p *PagedBuffer[T]) Delete(id uint64) {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	p.locks.Lock(pageID)
	if len(p.buf) <= int(pageID) || p.buf[pageID] == nil {
		p.locks.Unlock(pageID)
		return // No page to delete from
	}

	var zero T
	p.buf[pageID][slotID] = zero
	p.locks.Unlock(pageID)
}

// Grow increases the size of the buffer to accommodate at least `page` pages.
func (p *PagedBuffer[T]) Grow(newSize uint64) {
	p.locks.LockAll()
	if newSize <= uint64(len(p.buf)) {
		p.locks.UnlockAll()
		return // No need to grow
	}

	newBuf := make([][]T, newSize)
	copy(newBuf, p.buf)
	p.buf = newBuf
	p.locks.UnlockAll()
}

// Reset clears all pages of the array but does not change its capacity.
func (p *PagedBuffer[T]) Reset() {
	p.locks.LockAll()
	for i := range p.buf {
		clear(p.buf[i]) // optional: zero memory
		p.buf[i] = nil
	}
	p.locks.UnlockAll()
}

// Cap returns the current capacity of the array.
func (p *PagedBuffer[T]) Cap() int {
	p.locks.RLock(0)
	defer p.locks.RUnlock(0)

	return len(p.buf) * int(p.pageSize)
}
