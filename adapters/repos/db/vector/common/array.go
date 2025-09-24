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
	"sync/atomic"
	"unsafe"
)

// PagedArray is a array that stores elements in pages of a fixed size.
// It is optimized for concurrent access patterns where multiple goroutines may read and write to different pages simultaneously.
// The thread-safety is delegated to the caller, a typical pattern is to use an exclusive lock when allocating pages
// and atomic operations for reading and writing individual elements within a page.
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

	if int(pageID) >= len(p.buf) {
		var zero T
		return zero
	}

	ptr := unsafe.Pointer(&p.buf[pageID])
	loadedPtr := (*[]T)(atomic.LoadPointer((*unsafe.Pointer)(ptr)))
	if loadedPtr == nil {
		var zero T
		return zero
	}

	return (*loadedPtr)[slotID]
}

// GetPageFor takes an ID and returns the associated page and its index.
// If the page does not exist, it returns nil.
// It doesn't return a copy of the page, so modifications to the returned slice will affect the original data.
func (p *PagedArray[T]) GetPageFor(id uint64) ([]T, int) {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return nil, -1
	}

	ptr := unsafe.Pointer(&p.buf[pageID])
	loadedPtr := (*[]T)(atomic.LoadPointer((*unsafe.Pointer)(ptr)))
	if loadedPtr == nil {
		return nil, -1
	}

	slotID := id & p.pageMask

	return (*loadedPtr), int(slotID)
}

// Set stores the element at the given index, assuming the page exists.
// Callers need to ensure the page is allocated before calling this method.
func (p *PagedArray[T]) Set(id uint64, value T) {
	pageID := id >> p.pageBits
	slotID := id & p.pageMask

	ptr := unsafe.Pointer(&p.buf[pageID])
	loadedPtr := (*[]T)(atomic.LoadPointer((*unsafe.Pointer)(ptr)))

	(*loadedPtr)[slotID] = value
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
	ptr := unsafe.Pointer(&p.buf[pageID])
	loadedPtr := (*[]T)(atomic.LoadPointer((*unsafe.Pointer)(ptr)))

	if loadedPtr == nil {
		newPage := make([]T, p.pageSize)
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(ptr), nil, unsafe.Pointer(&newPage))
	}
}

// Grow ensures the buffer has space for `newPageCount` pages.
// It does not zero or allocate the individual pages unless needed.
func (p *PagedArray[T]) Grow(newPageCount int) {
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
