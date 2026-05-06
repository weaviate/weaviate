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

package common

import (
	"fmt"
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

const (
	pagesPerGroup     = 128
	pagesPerGroupBits = 7 // log2(128)
	pagesPerGroupMask = pagesPerGroup - 1
)

// group is a fixed-size bucket of page pointers, allocated on demand.
// Each group holds 128 pages, costing ~1KB when allocated (128 × 8-byte pointers).
type group[T any] struct {
	pages [pagesPerGroup]atomic.Pointer[[]T]
}

// GroupedPagedArray is a two-level paged array optimized for concurrent access
//
// Level 1: a slice of atomic pointers to groups (allocated upfront, small).
// Level 2: each group holds 128 atomic pointers to pages (allocated on demand).
// Pages: fixed-size slices of T (allocated on demand).
//
// Read path is lock-free (two atomic loads). Write path uses a mutex only
// for allocating new groups/pages via double-checked locking.
//
// Caller is responsible for ensuring that reads and writes to elements within
// returned pages are performed safely using atomic operations or mutexes.
type GroupedPagedArray[T any] struct {
	groups   []atomic.Pointer[group[T]]
	pageSize uint64
	pageBits uint8
	pageMask uint64

	// mu protects allocation of new groups and pages
	mu sync.Mutex
}

// NewGroupedPagedArray creates a new GroupedPagedArray.
//
// maxPages is the maximum number of pages that can be addressed.
// pageSize is the number of elements per page (rounded up to a power of 2, minimum 64).
//
// Total addressable elements = maxPages × pageSize.
//
// The upfront cost is len(groups) × 8 bytes, where len(groups) = ceil(maxPages / 128).
// For example, 16K max pages costs only 1KB upfront.
func NewGroupedPagedArray[T any](maxPages, pageSize uint64) *GroupedPagedArray[T] {
	if pageSize < 64 {
		pageSize = 64
	}
	pageSize = nextPow2(pageSize)

	numGroups := (maxPages + pagesPerGroup - 1) / pagesPerGroup

	return &GroupedPagedArray[T]{
		groups:   make([]atomic.Pointer[group[T]], numGroups),
		pageSize: pageSize,
		pageBits: uint8(bits.TrailingZeros64(pageSize)),
		pageMask: pageSize - 1,
	}
}

// GetPageFor returns the page containing the given ID and the index within that page.
// Returns (nil, -1) if the page has not been allocated.
//
// Lock-free: performs two atomic loads.
func (p *GroupedPagedArray[T]) GetPageFor(id uint64) ([]T, int) {
	pageID := id >> p.pageBits
	grpID := pageID >> pagesPerGroupBits
	subID := pageID & pagesPerGroupMask

	if grpID >= uint64(len(p.groups)) {
		return nil, -1
	}

	grp := p.groups[grpID].Load()
	if grp == nil {
		return nil, -1
	}

	pg := grp.pages[subID].Load()
	if pg == nil {
		return nil, -1
	}

	return *pg, int(id & p.pageMask)
}

// EnsurePageFor ensures that a page exists for the given ID, allocating
// the group and/or page if necessary. Returns the page and the index
// within the page.
//
// Panics if the ID exceeds the maximum addressable capacity.
func (p *GroupedPagedArray[T]) EnsurePageFor(id uint64) ([]T, int) {
	// Fast path: lock-free check
	page, slot := p.GetPageFor(id)
	if page != nil {
		return page, slot
	}

	// Slow path: allocate under lock with double-checked locking
	p.mu.Lock()
	defer p.mu.Unlock()

	page, slot = p.GetPageFor(id)
	if page != nil {
		return page, slot
	}

	pageID := id >> p.pageBits
	grpID := pageID >> pagesPerGroupBits
	subID := pageID & pagesPerGroupMask

	if grpID >= uint64(len(p.groups)) {
		panic(fmt.Sprintf(
			"GroupedPagedArray: id %d exceeds capacity (max group %d, got %d)",
			id, len(p.groups), grpID,
		))
	}

	// Ensure group exists
	grp := p.groups[grpID].Load()
	if grp == nil {
		grp = &group[T]{}
		p.groups[grpID].Store(grp)
	}

	// Ensure page exists
	pg := make([]T, p.pageSize)
	grp.pages[subID].Store(&pg)

	return pg, int(id & p.pageMask)
}
