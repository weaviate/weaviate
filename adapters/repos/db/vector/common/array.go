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

// FlatArray stores elements in a contiguous slice.
// It is optimized for cases where random access speed is more important than memory usage
// or write performance.
// The array will grow as needed.
type FlatArray[T any] struct {
	array   []T
	initCap int // Initial capacity for the array
}

// NewFlatArray creates a new FlatArray with the given initial capacity.
func NewFlatArray[T any](initialCapacity int) *FlatArray[T] {
	return &FlatArray[T]{
		array:   make([]T, initialCapacity),
		initCap: initialCapacity,
	}
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (f *FlatArray[T]) Get(id int) T {
	if id < 0 || id >= len(f.array) {
		var zero T
		return zero
	}

	return f.array[id]
}

// Set sets the element at the given index.
// If the index is out of bounds, it will grow the array.
func (f *FlatArray[T]) Set(id int, value T) {
	if id >= len(f.array) {
		f.grow(id + 2000)
	}
	f.array[id] = value
}

func (f *FlatArray[T]) grow(newSize int) {
	newArray := make([]T, newSize)
	copy(newArray, f.array)
	f.array = newArray
}

// Reset frees the array and creates a new empty slice with the initial capacity.
func (f *FlatArray[T]) Reset() {
	f.array = make([]T, f.initCap)
}

// PagedArray stores elements in pages of a fixed size.
// It is optimized for cases where the array is sparse and the number of elements is not known in advance.
// The array will grow as needed and will reuse pages that have been freed.
type PagedArray[T any] struct {
	array     [][]T
	pageSize  int
	freePages [][]T
}

// NewPagedArray creates a new PagedArray with the given page size.
// The array will start with 10 pages.
func NewPagedArray[T any](pageSize int) *PagedArray[T] {
	return NewPagedArrayWith[T](pageSize, 10)
}

// NewPagedArrayWith creates a new PagedArray with the given page size and initial number of pages.
func NewPagedArrayWith[T any](pageSize int, initialPages int) *PagedArray[T] {
	return &PagedArray[T]{
		pageSize: pageSize,
		array:    make([][]T, initialPages),
	}
}

// Get returns the element at the given index.
// If the element is not in the array, it will return nil.
func (p *PagedArray[T]) Get(id int) T {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if p.array[pageID] == nil {
		var zero T
		return zero
	}

	return p.array[pageID][slotID]
}

// Set sets the element at the given index.
// If the page does not exist, it will be created.
func (p *PagedArray[T]) Set(id int, value T) {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if pageID >= len(p.array) {
		p.grow(pageID)
	}

	if p.array[pageID] == nil {
		p.array[pageID] = p.getPage()
	}

	p.array[pageID][slotID] = value
}

func (p *PagedArray[T]) grow(page int) {
	newSize := max(page+10, len(p.array)*2)
	newArray := make([][]T, newSize)
	copy(newArray, p.array)
	p.array = newArray
}

func (p *PagedArray[T]) getPage() []T {
	if len(p.freePages) > 0 {
		lastIndex := len(p.freePages) - 1
		page := p.freePages[lastIndex]
		p.freePages = p.freePages[:lastIndex]
		return page
	}

	return make([]T, p.pageSize)
}

// Reset clears the array and frees all pages.
// Free pages are reused when new pages are needed.
func (p *PagedArray[T]) Reset() {
	for i := range p.array {
		if p.array[i] != nil {
			clear(p.array[i])
			p.freePages = append(p.freePages, p.array[i])
			p.array[i] = nil
		}
	}
}

// Cap returns the current capacity of the array.
func (p *PagedArray[T]) Cap() int {
	return len(p.array) * p.pageSize
}
