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

// PagedCache is a cache that stores elements in pages of a fixed size.
// It is optimized for cases where the cache is sparse and the number of elements is not known in advance.
// The cache will grow as needed and will reuse pages that have been freed.
type PagedCache[T any] struct {
	cache     [][]*T
	pageSize  int
	freePages [][]*T
}

// NewPagedCache creates a new PagedCache with the given page size.
// The cache will start with 10 pages.
func NewPagedCache[T any](pageSize int) *PagedCache[T] {
	return NewPagedCacheWith[T](pageSize, 10)
}

// NewPagedCacheWith creates a new PagedCache with the given page size and initial number of pages.
func NewPagedCacheWith[T any](pageSize int, initialPages int) *PagedCache[T] {
	return &PagedCache[T]{
		pageSize: pageSize,
		cache:    make([][]*T, initialPages),
	}
}

// Get returns the element at the given index.
// If the element is not in the cache, it will return nil.
func (p *PagedCache[T]) Get(id int) *T {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if p.cache[pageID] == nil {
		return nil
	}

	return p.cache[pageID][slotID]
}

// Set sets the element at the given index.
// If the page does not exist, it will be created.
func (p *PagedCache[T]) Set(id int, value *T) {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if pageID >= len(p.cache) {
		p.grow(pageID)
	}

	if p.cache[pageID] == nil {
		p.cache[pageID] = p.getPage()
	}

	p.cache[pageID][slotID] = value
}

func (p *PagedCache[T]) grow(page int) {
	newSize := max(page+10, len(p.cache)*2)
	newCache := make([][]*T, newSize)
	copy(newCache, p.cache)
	p.cache = newCache
}

func (p *PagedCache[T]) getPage() []*T {
	if len(p.freePages) > 0 {
		lastIndex := len(p.freePages) - 1
		page := p.freePages[lastIndex]
		p.freePages = p.freePages[:lastIndex]
		return page
	}

	return make([]*T, p.pageSize)
}

// Reset clears the cache and frees all pages.
// Free pages are reused when new pages are needed.
func (p *PagedCache[T]) Reset() {
	for i := range p.cache {
		if p.cache[i] != nil {
			clear(p.cache[i])
			p.freePages = append(p.freePages, p.cache[i])
			p.cache[i] = nil
		}
	}
}

// Cap returns the current capacity of the cache.
func (p *PagedCache[T]) Cap() int {
	return len(p.cache) * p.pageSize
}

// FlatCache is a cache that stores elements in a contiguous slice.
// It is optimized for cases where random access speed is more important than memory usage.
// The cache will grow as needed.
type FlatCache[T any] struct {
	cache   []T
	initCap int // Initial capacity for the cache
}

// NewFlatCache creates a new FlatCache with the given initial capacity.
func NewFlatCache[T any](initialCapacity int) *FlatCache[T] {
	return &FlatCache[T]{
		cache:   make([]T, initialCapacity),
		initCap: initialCapacity,
	}
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (f *FlatCache[T]) Get(id int) T {
	if id < 0 || id >= len(f.cache) {
		var zero T
		return zero
	}

	return f.cache[id]
}

// Set sets the element at the given index.
// If the index is out of bounds, it will grow the cache.
func (f *FlatCache[T]) Set(id int, value T) {
	if id >= len(f.cache) {
		f.grow(id + 2000)
	}
	f.cache[id] = value
}

func (f *FlatCache[T]) grow(newSize int) {
	newCache := make([]T, newSize)
	copy(newCache, f.cache)
	f.cache = newCache
}

// Reset frees the cache and creates a new empty slice with the initial capacity.
func (f *FlatCache[T]) Reset() {
	f.cache = make([]T, f.initCap)
}
