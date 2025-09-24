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

package spfresh

import (
	"iter"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Centroid struct {
	Vector  Vector
	Deleted bool
}

type BruteForceSPTAG struct {
	quantizer *compressionhelpers.RotationalQuantizer
	distancer *Distancer
	metrics   *Metrics

	centroidsLock sync.Mutex
	centroids     *PagedArray[atomic.Pointer[Centroid]]
	idLock        sync.RWMutex
	ids           []uint64
	counter       atomic.Int32
}

func NewBruteForceSPTAG(metrics *Metrics, pages, pageSize uint64) *BruteForceSPTAG {
	return &BruteForceSPTAG{
		metrics:   metrics,
		centroids: NewPagedArray[atomic.Pointer[Centroid]](pages, pageSize),
	}
}

func (s *BruteForceSPTAG) Init(dims int32, distancer distancer.Provider) {
	// TODO: seed
	seed := uint64(42)
	s.quantizer = compressionhelpers.NewRotationalQuantizer(int(dims), seed, 8, distancer)
	s.distancer = &Distancer{
		quantizer: s.quantizer,
		distancer: distancer,
	}
}

func (s *BruteForceSPTAG) Get(id uint64) *Centroid {
	page, _, slot := s.centroids.GetPageFor(id)
	if page == nil {
		return nil
	}

	return page[slot].Load()
}

func (s *BruteForceSPTAG) Insert(id uint64, vector Vector) error {
	page, _, slot := s.centroids.GetPageFor(id)
	if page == nil {
		s.centroidsLock.Lock()
		page, _, slot = s.centroids.GetPageFor(id)
		if page == nil {
			s.centroids.AllocPageFor(id)
			page, _, slot = s.centroids.GetPageFor(id)
		}
		s.centroidsLock.Unlock()
	}

	page[slot].Store(&Centroid{
		Vector: vector,
	})

	s.idLock.Lock()
	s.ids = append(s.ids, id)
	s.idLock.Unlock()

	s.metrics.SetPostings(int(s.counter.Add(1)))

	return nil
}

func (s *BruteForceSPTAG) MarkAsDeleted(id uint64) error {
	for {
		page, _, slot := s.centroids.GetPageFor(id)
		if page == nil {
			return nil
		}
		centroid := page[slot].Load()
		if centroid == nil {
			return errors.New("centroid not found")
		}

		if centroid.Deleted {
			return errors.New("centroid already marked as deleted")
		}

		newCentroid := Centroid{
			Vector:  centroid.Vector,
			Deleted: true,
		}

		if page[slot].CompareAndSwap(centroid, &newCentroid) {
			s.metrics.SetPostings(int(s.counter.Add(-1)))
			break
		}
	}

	return nil
}

func (s *BruteForceSPTAG) Exists(id uint64) bool {
	centroid := s.Get(id)
	if centroid == nil {
		return false
	}

	return !centroid.Deleted
}

func (s *BruteForceSPTAG) Quantizer() *compressionhelpers.RotationalQuantizer {
	return s.quantizer
}

var idsPool = sync.Pool{
	New: func() any {
		return make([]uint64, 0, 1024)
	},
}

func (s *BruteForceSPTAG) Search(query Vector, k int) (*ResultSet, error) {
	start := time.Now()
	defer s.metrics.CentroidSearchDuration(start)

	ids := idsPool.Get().([]uint64)
	ids = ids[:0]
	defer idsPool.Put(ids)

	s.idLock.RLock()
	ids = append(ids, s.ids...) // copy to avoid races
	s.idLock.RUnlock()

	max := uint64(len(ids))

	q := NewResultSet(k)

	for i := range max {
		c := s.Get(ids[i])
		if c == nil || c.Deleted {
			continue
		}

		dist, err := c.Vector.Distance(s.distancer, query)
		if err != nil {
			return nil, err
		}

		q.Insert(ids[i], dist)
	}

	return q, nil
}

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
func (p *PagedArray[T]) GetPageFor(id uint64) ([]T, uint64, int) {
	pageID := id >> p.pageBits

	if int(pageID) >= len(p.buf) {
		return nil, 0, -1
	}

	ptr := unsafe.Pointer(&p.buf[pageID])
	loadedPtr := (*[]T)(atomic.LoadPointer((*unsafe.Pointer)(ptr)))
	if loadedPtr == nil {
		return nil, 0, -1
	}

	slotID := id & p.pageMask

	return (*loadedPtr), pageID, int(slotID)
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

// Len returns the number of pages allocated.
func (p *PagedArray[T]) Len() int {
	return len(p.buf)
}

type Result struct {
	ID       uint64
	Distance float32
}

// ResultSet maintains the k smallest elements by distance in a sorted array.
type ResultSet struct {
	data []Result
	k    int
}

var qPool = sync.Pool{
	New: func() any {
		return &ResultSet{
			data: make([]Result, 0, 64),
		}
	},
}

func NewResultSet(k int) *ResultSet {
	ks := qPool.Get().(*ResultSet)
	ks.Reset(k)

	return ks
}

func (ks *ResultSet) Release() {
	qPool.Put(ks)
}

// Insert adds a new element, maintaining only k smallest elements by distance
func (ks *ResultSet) Insert(id uint64, dist float32) {
	item := Result{ID: id, Distance: dist}

	// If array isn't full yet, just insert in sorted position
	if len(ks.data) < ks.k {
		pos := ks.searchByDistance(dist)
		ks.data = append(ks.data, Result{})
		copy(ks.data[pos+1:], ks.data[pos:])
		ks.data[pos] = item
		return
	}

	// If array is full, only insert if distance is smaller than max (last element)
	if dist < ks.data[ks.k-1].Distance {
		pos := ks.searchByDistance(dist)
		// Shift elements to the right and insert
		copy(ks.data[pos+1:], ks.data[pos:ks.k-1])
		ks.data[pos] = item
	}
}

// searchByDistance finds the insertion position for a given distance
func (ks *ResultSet) searchByDistance(dist float32) int {
	left, right := 0, len(ks.data)
	for left < right {
		mid := (left + right) / 2
		if ks.data[mid].Distance < dist {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func (ks *ResultSet) Len() int {
	return len(ks.data)
}

func (ks *ResultSet) Iter() iter.Seq2[uint64, float32] {
	return func(yield func(uint64, float32) bool) {
		for _, item := range ks.data {
			if !yield(item.ID, item.Distance) {
				break
			}
		}
	}
}

func (ks *ResultSet) Reset(k int) {
	ks.data = ks.data[:0]
	if cap(ks.data) < k {
		ks.data = make([]Result, 0, k)
	}
	ks.k = k
}
