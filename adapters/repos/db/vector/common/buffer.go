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
	flatDefaultInitCapacity = 1000 // Default initial capacity for buffers
	flatMinimumGrowthDelta  = 2000 // Minimum growth delta for flat buffers
	pagedDefaultPageSize    = 512  // Default page size for paged buffers
	pagedDefaultInitPages   = 10   // Default initial number of pages for paged buffers
)

// FlatBuffer stores elements in a contiguous slice.
// It is optimized for cases where random access speed is more important than memory usage.
// The buffer will grow as needed.
// It is not thread-safe.
type FlatBuffer[T any] struct {
	buf []T
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (f *FlatBuffer[T]) Get(id uint64) T {
	if int(id) >= len(f.buf) {
		var zero T
		return zero
	}

	return f.buf[id]
}

// Set sets the element at the given index.
// If the index is out of bounds, it will grow the array.
func (f *FlatBuffer[T]) Set(id uint64, value T) {
	if int(id) >= len(f.buf) {
		f.Grow(id + flatMinimumGrowthDelta)
	}
	f.buf[id] = value
}

func (f *FlatBuffer[T]) Grow(newSize uint64) {
	if newSize < uint64(len(f.buf)) {
		return
	}

	newArray := make([]T, newSize)
	copy(newArray, f.buf)
	f.buf = newArray
}

// Reset frees the array and creates a new empty slice with the initial capacity.
func (f *FlatBuffer[T]) Reset() {
	f.buf = make([]T, flatDefaultInitCapacity)
}

// Cap returns the current capacity of the array.
func (f *FlatBuffer[T]) Cap() int {
	return len(f.buf)
}

// PagedBuffer stores elements in pages of a fixed size.
// It is optimized for cases where the buffer is sparse and the number of elements is not known in advance.
// The buffer will grow as needed and will reuse pages that have been freed.
// It is not thread-safe.
type PagedBuffer[T any] struct {
	buf       [][]T
	pageSize  uint64
	freePages [][]T
}

// Get returns the element at the given index.
// If the element is not in the buffer, it will return nil.
func (p *PagedBuffer[T]) Get(id uint64) T {
	if p.pageSize == 0 {
		var zero T
		return zero
	}

	pageID := id / p.pageSize

	if p.buf[pageID] == nil {
		var zero T
		return zero
	}

	slotID := id % p.pageSize

	return p.buf[pageID][slotID]
}

// Set sets the element at the given index.
// If the page does not exist, it will be created.
func (p *PagedBuffer[T]) Set(id uint64, value T) {
	if p.pageSize == 0 {
		p.Grow(pagedDefaultInitPages)
	}

	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if int(pageID) >= len(p.buf) {
		p.Grow(pageID)
	}

	if p.buf[pageID] == nil {
		p.buf[pageID] = p.getPage()
	}

	p.buf[pageID][slotID] = value
}

func (p *PagedBuffer[T]) Grow(page uint64) {
	if p.pageSize == 0 {
		p.pageSize = pagedDefaultPageSize
	}

	newSize := max(int(page+10), len(p.buf)*2)
	newBuf := make([][]T, newSize)
	copy(newBuf, p.buf)
	p.buf = newBuf
}

func (p *PagedBuffer[T]) getPage() []T {
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
func (p *PagedBuffer[T]) Reset() {
	for i := range p.buf {
		if p.buf[i] != nil {
			clear(p.buf[i])
			p.freePages = append(p.freePages, p.buf[i])
			p.buf[i] = nil
		}
	}
}

// Cap returns the current capacity of the array.
func (p *PagedBuffer[T]) Cap() int {
	return len(p.buf) * int(p.pageSize)
}

type Buffer[T any] interface {
	Get(id uint64) T
	Set(id uint64, value T)
	Reset()
	Grow(newSize uint64)
	Cap() int
}

// SafeBuffer is a thread-safe array that grows as needed.
type SafeBuffer[T any] struct {
	locks  *ShardedRWLocks
	buffer Buffer[T]
}

// Get returns the element at the given index.
// If the index is out of bounds, it will return zero value of T.
func (s *SafeBuffer[T]) Get(id uint64) T {
	s.locks.Lock(id)
	v := s.buffer.Get(id)
	s.locks.Unlock(id)

	return v
}

// Set sets the element at the given index.
// If the index is out of bounds, it will grow the array.
func (s *SafeBuffer[T]) Set(id uint64, value T) {
	s.locks.Lock(id)
	s.buffer.Set(id, value)
	s.locks.Unlock(id)
}

func (s *SafeBuffer[T]) Grow(newSize uint64) {
	s.locks.LockAll()
	s.buffer.Grow(newSize)
	s.locks.UnlockAll()
}

// Reset frees the array and creates a new empty slice with the initial capacity.
func (s *SafeBuffer[T]) Reset() {
	s.locks.LockAll()
	s.buffer.Reset()
	s.locks.UnlockAll()
}

// NewFlatBuffer creates a thread-safe FlatBuffer with the given initial capacity.
func NewSafeFlatBuffer[T any](initialCapacity uint64) *SafeBuffer[T] {
	var fb FlatBuffer[T]
	fb.Grow(initialCapacity)

	return &SafeBuffer[T]{
		buffer: &fb,
		locks:  NewShardedRWLocks(16),
	}
}

// NewPagedBuffer creates a new PagedBuffer with the given page size.
// The array will start with 10 pages.
func NewSafePagedBuffer[T any](pageSize uint64) *PagedBuffer[T] {
	return NewSafePagedBufferWith[T](pageSize, 10)
}

// NewPagedBufferWith creates a new PagedBuffer with the given page size and initial number of pages.
func NewSafePagedBufferWith[T any](pageSize, initialPages uint64) *PagedBuffer[T] {
	return &PagedBuffer[T]{
		pageSize: pageSize,
		buf:      make([][]T, initialPages),
	}
}
