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
	pagedBitsetPagesPerGroup     = 128
	pagedBitsetPagesPerGroupBits = 7
	pagedBitsetPagesPerGroupMask = pagedBitsetPagesPerGroup - 1

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
	maxPages uint64
	groups   []atomic.Pointer[pagedBitsetGroup]
	live     atomic.Uint64

	// mu protects group and page allocation. Reads, bit operations, and page
	// deletion use atomics and do not take this lock.
	mu sync.Mutex
}

type pagedBitsetGroup struct {
	pages [pagedBitsetPagesPerGroup]atomic.Pointer[pagedBitsetPage]
}

type pagedBitsetPage struct {
	count atomic.Uint32
	words [pagedBitsetWordsPerPage]atomic.Uint64
}

// NewPagedBitset creates a PagedBitset that can address maxPages*64K IDs.
func NewPagedBitset(maxPages uint64) *PagedBitset {
	groupCount := (maxPages + pagedBitsetPagesPerGroup - 1) >> pagedBitsetPagesPerGroupBits
	return &PagedBitset{
		maxPages: maxPages,
		groups:   make([]atomic.Pointer[pagedBitsetGroup], groupCount),
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
		cell := b.ensurePageCell(pageID)
		page = cell.Load()
		if page == nil {
			page = &pagedBitsetPage{}
			page.count.Store(1)
			cell.Store(page)
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

	cell := b.pageCell(pageID)
	if cell != nil && cell.CompareAndSwap(page, nil) {
		b.live.Add(^uint64(0))
		return
	}

	page.count.Store(0)
}

func (b *PagedBitset) page(pageID uint64) *pagedBitsetPage {
	cell := b.pageCell(pageID)
	if cell == nil {
		return nil
	}
	return cell.Load()
}

func (b *PagedBitset) pageCell(pageID uint64) *atomic.Pointer[pagedBitsetPage] {
	if pageID >= b.maxPages {
		panic("paged bitset capacity exceeded")
	}

	groupID := pageID >> pagedBitsetPagesPerGroupBits
	group := b.groups[groupID].Load()
	if group == nil {
		return nil
	}

	return &group.pages[pageID&pagedBitsetPagesPerGroupMask]
}

func (b *PagedBitset) ensurePageCell(pageID uint64) *atomic.Pointer[pagedBitsetPage] {
	if pageID >= b.maxPages {
		panic("paged bitset capacity exceeded")
	}

	groupID := pageID >> pagedBitsetPagesPerGroupBits
	group := b.groups[groupID].Load()
	if group == nil {
		group = &pagedBitsetGroup{}
		b.groups[groupID].Store(group)
	}

	return &group.pages[pageID&pagedBitsetPagesPerGroupMask]
}

func pagedBitsetLocation(id uint64) (pageID uint64, wordID uint64, mask uint64) {
	pageID = id >> pagedBitsetPageBits
	localID := id & pagedBitsetPageMask
	wordID = localID >> pagedBitsetWordBits
	mask = uint64(1) << (localID & pagedBitsetWordMask)
	return pageID, wordID, mask
}
