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
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	pagedBitsetPageBits     = 16
	pagedBitsetPageSize     = 1 << pagedBitsetPageBits
	pagedBitsetPageMask     = pagedBitsetPageSize - 1
	pagedBitsetWordBits     = 6
	pagedBitsetWordSize     = 1 << pagedBitsetWordBits
	pagedBitsetWordMask     = pagedBitsetWordSize - 1
	pagedBitsetWordsPerPage = pagedBitsetPageSize / pagedBitsetWordSize

	pagedBitsetCleanupBit = uint32(1 << 31)
	pagedBitsetCountMask  = pagedBitsetCleanupBit - 1
)

// PagedBitset is a concurrent bitset optimized for dense, monotonic IDs.
// Pages are allocated lazily and released when the last bit in a page is
// deleted. Each page covers 64K IDs and costs 8KiB while live.
type PagedBitset struct {
	pages []atomic.Pointer[pagedBitsetPage]
	live  atomic.Uint64

	// mu protects page allocation. Reads, bit operations, and page deletion use
	// atomics and do not take this lock.
	mu sync.Mutex
}

type pagedBitsetPage struct {
	count atomic.Uint32
	words [pagedBitsetWordsPerPage]atomic.Uint64
}

// NewPagedBitset creates a PagedBitset that can address maxPages*64K IDs.
func NewPagedBitset(maxPages uint64) *PagedBitset {
	return &PagedBitset{
		pages: make([]atomic.Pointer[pagedBitsetPage], maxPages),
	}
}

// TryAdd sets id if it is not already present.
// It returns true if id was added, and false if it was already present.
func (b *PagedBitset) TryAdd(id uint64) bool {
	pageID, wordID, mask := pagedBitsetLocation(id)
	page := b.ensureReservedPage(pageID)

	for {
		old := page.words[wordID].Load()
		if old&mask != 0 {
			b.release(pageID, page, 1)
			return false
		}
		if page.words[wordID].CompareAndSwap(old, old|mask) {
			return true
		}
	}
}

// Delete clears id if present.
// It returns true if id was present and cleared.
func (b *PagedBitset) Delete(id uint64) bool {
	pageID, wordID, mask := pagedBitsetLocation(id)
	page := b.reserveExistingPage(pageID)
	if page == nil {
		return false
	}

	for {
		old := page.words[wordID].Load()
		if old&mask == 0 {
			b.release(pageID, page, 1)
			return false
		}
		if page.words[wordID].CompareAndSwap(old, old&^mask) {
			if b.release(pageID, page, 2) == 0 {
				b.cleanupPage(pageID, page)
			}
			return true
		}
	}
}

// Contains returns true if id is currently present.
func (b *PagedBitset) Contains(id uint64) bool {
	pageID, wordID, mask := pagedBitsetLocation(id)
	page := b.reserveExistingPage(pageID)
	if page == nil {
		return false
	}

	exists := page.words[wordID].Load()&mask != 0
	if b.release(pageID, page, 1) == 0 {
		b.cleanupPage(pageID, page)
	}
	return exists
}

// LivePages returns the number of currently allocated bitset pages.
func (b *PagedBitset) LivePages() uint64 {
	return b.live.Load()
}

func (b *PagedBitset) ensureReservedPage(pageID uint64) *pagedBitsetPage {
	for {
		page := b.page(pageID)
		if page != nil {
			if b.reserve(page) {
				return page
			}
			continue
		}

		b.mu.Lock()
		page = b.page(pageID)
		if page == nil {
			page = &pagedBitsetPage{}
			page.count.Store(1)
			b.pageCell(pageID).Store(page)
			b.live.Add(1)
			b.mu.Unlock()
			return page
		}
		b.mu.Unlock()
	}
}

func (b *PagedBitset) reserveExistingPage(pageID uint64) *pagedBitsetPage {
	for {
		page := b.page(pageID)
		if page == nil {
			return nil
		}
		if b.reserve(page) {
			return page
		}
	}
}

func (b *PagedBitset) reserve(page *pagedBitsetPage) bool {
	for {
		current := page.count.Load()
		if current&pagedBitsetCleanupBit != 0 {
			runtime.Gosched()
			return false
		}
		if current&pagedBitsetCountMask == pagedBitsetCountMask {
			panic("paged bitset page count overflow")
		}
		if page.count.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (b *PagedBitset) release(pageID uint64, page *pagedBitsetPage, n uint32) uint32 {
	for {
		current := page.count.Load()
		if current&pagedBitsetCleanupBit != 0 || current&pagedBitsetCountMask < n {
			panic("paged bitset page count underflow")
		}
		next := current - n
		if page.count.CompareAndSwap(current, next) {
			return next
		}
	}
}

func (b *PagedBitset) cleanupPage(pageID uint64, page *pagedBitsetPage) {
	if !page.count.CompareAndSwap(0, pagedBitsetCleanupBit) {
		return
	}

	if b.pageCell(pageID).CompareAndSwap(page, nil) {
		b.live.Add(^uint64(0))
		return
	}

	page.count.Store(0)
}

func (b *PagedBitset) page(pageID uint64) *pagedBitsetPage {
	return b.pageCell(pageID).Load()
}

func (b *PagedBitset) pageCell(pageID uint64) *atomic.Pointer[pagedBitsetPage] {
	if pageID >= uint64(len(b.pages)) {
		panic("paged bitset capacity exceeded")
	}
	return &b.pages[pageID]
}

func pagedBitsetLocation(id uint64) (pageID uint64, wordID uint64, mask uint64) {
	pageID = id >> pagedBitsetPageBits
	localID := id & pagedBitsetPageMask
	wordID = localID >> pagedBitsetWordBits
	mask = uint64(1) << (localID & pagedBitsetWordMask)
	return pageID, wordID, mask
}
