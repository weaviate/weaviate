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

import (
	"math/bits"
)

const (
	flatDefaultInitCapacity = 1000       // Default initial capacity for buffers
	flatMaxReusableCapacity = 10_000_000 // Maximum reusable capacity for flat buffers
	flatMinimumGrowthDelta  = 2000       // Minimum growth delta for flat buffers
)

// FlatArray is an array that stores elements in a contiguous slice.
// It is optimized for cases where random access speed is more important than memory usage.
// This type is not thread-safe and should be used in combination with ShardedLocks or similar.
type FlatArray[T any] struct {
	buf []T
}

func NewFlatBuffer[T any](initSize uint64) *FlatArray[T] {
	if initSize == 0 {
		initSize = flatDefaultInitCapacity
	}

	return &FlatArray[T]{
		buf: make([]T, initSize),
	}
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (f *FlatArray[T]) Get(id uint64) T {
	if int(id) >= len(f.buf) {
		var zero T
		return zero
	}
	v := f.buf[id]
	return v
}

// Set sets the element at the given index.
// Panics if the index is out of bounds.
func (f *FlatArray[T]) Set(id uint64, value T) {
	f.buf[id] = value
}

// Delete sets the element at the given index to zero value of T.
func (f *FlatArray[T]) Delete(index uint64) {
	if int(index) >= len(f.buf) {
		return
	}

	var zero T
	f.buf[index] = zero
}

func (f *FlatArray[T]) Grow(newSize uint64) {
	if newSize <= uint64(len(f.buf)) {
		return // No need to grow
	}
	f.grow(newSize)
}

func (f *FlatArray[T]) grow(newSize uint64) {
	if newSize < uint64(len(f.buf)) {
		return
	}

	if int(newSize) > cap(f.buf) {
		newArray := make([]T, newSize+flatMinimumGrowthDelta)
		copy(newArray, f.buf)
		f.buf = newArray
		return
	}
	f.buf = f.buf[:newSize]
}

// Reset clears the underlying slice and reuses the existing memory.
// This is useful to avoid memory allocations when the buffer is reused frequently.
func (f *FlatArray[T]) Reset() {
	// If the buffer is too large, we reset it to the default initial capacity
	if cap(f.buf) > flatMaxReusableCapacity {
		f.buf = make([]T, flatDefaultInitCapacity)
	} else {
		f.buf = f.buf[:flatDefaultInitCapacity]
		clear(f.buf)
	}
}

// Cap returns the current capacity of the underlying slice.
func (f *FlatArray[T]) Cap() int {
	c := cap(f.buf)
	return c
}

// Len returns the size of the buffer.
func (f *FlatArray[T]) Len() int {
	l := len(f.buf)
	return l
}

// PagedArray is a array that stores elements in pages of a fixed size.
// It is optimized for cases where the array is sparse and the number of elements is not known in advance.
type PagedArray[T any] struct {
	buf      [][]T
	pageSize uint64 // Size of each page
	pageBits uint8  // log2(pageSize)
	pageMask uint64 // pageSize - 1
}

// NewPagedArray creates a new PagedArray with the given page size.
// It will round up to the next power of 2 and enforce a minimum size of 64.
func NewPagedArray[T any](pages, pageSize uint64) *PagedArray[T] {
	if pageSize < 64 {
		pageSize = 64
	}
	pageSize = nextPow2(pageSize)

	return &PagedArray[T]{
		pageSize: pageSize,
		pageBits: uint8(bits.TrailingZeros64(pageSize)),
		pageMask: pageSize - 1,
		buf:      make([][]T, pages),
	}
}

func nextPow2(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	if (v & (v - 1)) == 0 {
		return v
	}
	return 1 << bits.Len64(v)
}

// Get returns the element at the given index.
// If the page does not exist, it returns both zero value and false.
func (p *PagedArray[T]) Get(id uint64) T {
	pageID := id >> p.pageBits
	slotID := id & p.pageMask

	if int(pageID) >= len(p.buf) || p.buf[pageID] == nil {
		var zero T
		return zero
	}

	return p.buf[pageID][slotID]
}

// GetPageFor takes an ID and returns the associated page and its index.
// If the page does not exist, it returns nil.
// It doesn't return a copy of the page, so modifications to the returned slice will affect the original data.
func (p *PagedArray[T]) GetPageFor(id uint64) ([]T, int) {
	pageID := id >> p.pageBits
	if int(pageID) >= len(p.buf) || p.buf[pageID] == nil {
		return nil, -1
	}
	slotID := id & p.pageMask

	return p.buf[pageID], int(slotID)
}

// Set stores the element at the given index, assuming the page exists.
// Callers need to ensure the page is allocated before calling this method.
func (p *PagedArray[T]) Set(id uint64, value T) {
	pageID := id >> p.pageBits
	slotID := id & p.pageMask

	p.buf[pageID][slotID] = value
}

// Delete sets the element to zero value.
// If the page does not exist, it does nothing and returns false.
func (p *PagedArray[T]) Delete(id uint64) bool {
	pageID := id >> p.pageBits
	slotID := id & p.pageMask

	if int(pageID) >= len(p.buf) || p.buf[pageID] == nil {
		return false
	}

	var zero T
	p.buf[pageID][slotID] = zero
	return true
}

// AllocPageFor allocates a page for the given ID if it does not already exist.
func (p *PagedArray[T]) AllocPageFor(id uint64) {
	pageID := id >> p.pageBits
	if p.buf[pageID] == nil {
		p.buf[pageID] = make([]T, p.pageSize)
	}
}

// Grow ensures the buffer has space for `newPageCount` pages.
// It does not zero or allocate the individual pages unless needed.
func (p *PagedArray[T]) Grow(newPageCount uint64) {
	if int(newPageCount) <= len(p.buf) {
		return
	}
	newBuf := make([][]T, newPageCount)
	copy(newBuf, p.buf)
	p.buf = newBuf
}

// Cap returns the total capacity across all allocated pages.
func (p *PagedArray[T]) Cap() int {
	total := 0
	for _, page := range p.buf {
		if page != nil {
			total += len(page)
		}
	}
	return total
}

// Len returns the number of pages allocated.
func (p *PagedArray[T]) Len() int {
	return len(p.buf)
}

// Reset clears all pages but retains the allocated memory.
func (p *PagedArray[T]) Reset() {
	for i := range p.buf {
		if p.buf[i] != nil {
			clear(p.buf[i])
		}
	}
}
