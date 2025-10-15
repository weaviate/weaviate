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
	"sync"
	"sync/atomic"
)

// PagedArray is an array that stores elements in pages of a fixed size.
// It is optimized for concurrent access patterns where multiple goroutines may read and write to different pages simultaneously.
// The API is thread-safe and returns direct references to the pages, allowing for efficient concurrent access.
// Caller is responsible for ensuring that reads and writes to the returned pages are performed safely,
// using either atomic operations or mutexes.
type PagedArray[T any] struct {
	buf      []atomic.Pointer[[]T]
	pageSize uint64
	pageBits uint8
	pageMask uint64

	// mu protects the allocation of new pages
	mu sync.Mutex
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

// GetPageFor takes an ID and returns the associated page and its index.
// If the page does not exist, it returns nil.
// It doesn't return a copy of the page, so modifications to the returned slice will affect the original data.
// Caller is responsible for ensuring that reads and writes to the returned page are performed safely,
// using either atomic operations or mutexes.
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

// EnsurePageFor makes sure that a page exists for the given ID.
// If the page already exists, it does nothing.
// It returns the page and its index within the page for the given ID.
func (p *PagedArray[T]) EnsurePageFor(id uint64) ([]T, int) {
	// first do a quick check without locking
	page, slot := p.GetPageFor(id)
	if page != nil {
		return page, slot
	}

	// double-checked locking
	p.mu.Lock()
	page, slot = p.GetPageFor(id)
	if page != nil {
		p.mu.Unlock()
		return page, slot
	}

	// allocate a new page
	pageID := id >> p.pageBits
	pg := make([]T, p.pageSize)
	p.buf[pageID].Store(&pg)

	p.mu.Unlock()

	return pg, int(id & p.pageMask)
}
