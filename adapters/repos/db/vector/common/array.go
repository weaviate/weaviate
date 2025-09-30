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
)

// PagedArray is an array that stores elements in pages of a fixed size.
// It is optimized for concurrent access patterns where multiple goroutines may read and write to different pages simultaneously.
// The thread-safety is delegated to the caller, a typical pattern is to use an exclusive lock when allocating pages
// and atomic operations for reading and writing individual elements within a page.
type PagedArray[T any] struct {
	buf      []atomic.Pointer[[]T]
	pageSize uint64
	pageBits uint8
	pageMask uint64
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
		buf:      make([]atomic.Pointer[[]T], pages),
	}
}

func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	if (v & (v - 1)) == 0 {
		return v
	}
	l := bits.Len64(v - 1)
	// avoid 1<<64 overflow
	if l >= 63 {
		return 1 << 63
	}
	return 1 << l
}

// Get returns the element at the given index.
// If the page does not exist, it returns the zero value of T.
func (p *PagedArray[T]) Get(id uint64) T {
	pageID := id >> p.pageBits
	var zero T

	if int(pageID) >= len(p.buf) {
		return zero
	}

	pg := p.buf[pageID].Load()
	if pg == nil {
		return zero
	}

	return (*pg)[int(id&p.pageMask)]
}

// GetPageFor takes an ID and returns the associated page and its index.
// If the page does not exist, it returns nil.
// It doesn't return a copy of the page, so modifications to the returned slice will affect the original data.
func (p *PagedArray[T]) GetPageFor(id uint64) ([]T, int) {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return nil, -1
	}

	pg := p.buf[pageID].Load()
	if pg == nil {
		return nil, -1
	}

	return (*pg), int(id & p.pageMask)
}

func (p *PagedArray[T]) Set(id uint64, v T) bool {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return false
	}

	pg := p.buf[pageID].Load()
	if pg == nil {
		return false
	}

	(*pg)[int(id&p.pageMask)] = v
	return true
}

// Delete sets the element to zero value.
// If the page does not exist, it does nothing and returns false.
func (p *PagedArray[T]) Delete(id uint64) bool {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return false
	}

	pg := p.buf[pageID].Load()
	if pg == nil {
		return false
	}

	var zero T
	(*pg)[int(id&p.pageMask)] = zero
	return true
}

// AllocPageFor allocates a page for the given ID if it does not already exist.
func (p *PagedArray[T]) AllocPageFor(id uint64) bool {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return false
	}

	if p.buf[pageID].Load() != nil {
		return true
	}

	pg := make([]T, p.pageSize)
	return p.buf[pageID].CompareAndSwap(nil, &pg)
}

// Grow ensures the buffer has space for `newPageCount` pages.
// It does not zero or allocate the individual pages unless needed.
func (p *PagedArray[T]) Grow(newPageCount int) {
	if newPageCount <= len(p.buf) {
		return
	}

	newBuf := make([]atomic.Pointer[[]T], newPageCount)
	for i := range p.buf {
		if pg := p.buf[i].Load(); pg != nil {
			newBuf[i].Store(pg)
		}
	}
	p.buf = newBuf
}

// Cap returns the total capacity across all allocated pages (sum of lengths).
func (p *PagedArray[T]) Cap() int {
	total := 0
	for i := range p.buf {
		if pg := p.buf[i].Load(); pg != nil {
			total += len(*pg)
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
		if pg := p.buf[i].Load(); pg != nil {
			clear(*pg)
		}
	}
}
