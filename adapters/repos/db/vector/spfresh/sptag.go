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
)

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
}

// type BruteForceSPTAG struct {
// 	quantizer *compressionhelpers.RotationalQuantizer
// 	distancer *Distancer
// 	metrics   *Metrics

// 	centroids *common.PagedArray[atomic.Pointer[Centroid]]
// 	idLock    sync.RWMutex
// 	ids       []uint64
// 	counter   atomic.Int32
// }

// func NewBruteForceSPTAG(metrics *Metrics, pages, pageSize uint64) *BruteForceSPTAG {
// 	return &BruteForceSPTAG{
// 		metrics:   metrics,
// 		centroids: common.NewPagedArray[atomic.Pointer[Centroid]](pages, pageSize),
// 	}
// }

// func (s *BruteForceSPTAG) Init(dims int32, distancer distancer.Provider) {
// 	// TODO: seed
// 	seed := uint64(42)
// 	s.quantizer = compressionhelpers.NewRotationalQuantizer(int(dims), seed, 8, distancer)
// 	s.distancer = &Distancer{
// 		quantizer: s.quantizer,
// 		distancer: distancer,
// 	}
// }

// func (s *BruteForceSPTAG) Get(id uint64) *Centroid {
// 	page, slot := s.centroids.GetPageFor(id)
// 	if page == nil {
// 		return nil
// 	}

// 	return page[slot].Load()
// }

// func (s *BruteForceSPTAG) Insert(id uint64, vector Vector) error {
// 	page, slot := s.centroids.EnsurePageFor(id)
// 	if page == nil {
// 		return errors.New("failed to allocate page")
// 	}

// 	page[slot].Store(&Centroid{
// 		Vector: vector,
// 	})

// 	s.idLock.Lock()
// 	s.ids = append(s.ids, id)
// 	s.idLock.Unlock()

// 	s.metrics.SetPostings(int(s.counter.Add(1)))

// 	return nil
// }

// func (s *BruteForceSPTAG) MarkAsDeleted(id uint64) error {
// 	for {
// 		page, slot := s.centroids.GetPageFor(id)
// 		if page == nil {
// 			return nil
// 		}
// 		centroid := page[slot].Load()
// 		if centroid == nil {
// 			return errors.New("centroid not found")
// 		}

// 		if centroid.Deleted {
// 			return errors.New("centroid already marked as deleted")
// 		}

// 		newCentroid := Centroid{
// 			Uncompressed: centroid.Uncompressed,
// 			Compressed:   centroid.Compressed,
// 			Deleted:      true,
// 		}

// 		if page[slot].CompareAndSwap(centroid, &newCentroid) {
// 			s.metrics.SetPostings(int(s.counter.Add(-1)))
// 			break
// 		}
// 	}

// 	return nil
// }

// func (s *BruteForceSPTAG) Exists(id uint64) bool {
// 	centroid := s.Get(id)
// 	if centroid == nil {
// 		return false
// 	}

// 	return !centroid.Deleted
// }

// func (s *BruteForceSPTAG) Quantizer() *compressionhelpers.RotationalQuantizer {
// 	return s.quantizer
// }

// var idsPool = sync.Pool{
// 	New: func() any {
// 		buf := make([]uint64, 0, 1024)
// 		return &buf
// 	},
// }

// func (s *BruteForceSPTAG) Search(query Vector, k int) (*ResultSet, error) {
// 	start := time.Now()
// 	defer s.metrics.CentroidSearchDuration(start)

// 	if k == 0 {
// 		return nil, nil
// 	}

// 	ids := *(idsPool.Get().(*[]uint64))
// 	ids = ids[:0]
// 	defer idsPool.Put(&ids)

// 	s.idLock.RLock()
// 	ids = append(ids, s.ids...) // copy to avoid races
// 	s.idLock.RUnlock()

// 	max := uint64(len(ids))

// 	q := NewResultSet(k)

// 	for i := range max {
// 		c := s.Get(ids[i])
// 		if c == nil || c.Deleted {
// 			continue
// 		}

// 		dist, err := c.Vector.Distance(s.distancer, query)
// 		if err != nil {
// 			return nil, err
// 		}

// 		q.Insert(ids[i], dist)
// 	}

// 	return q, nil
// }

type Result struct {
	ID       uint64
	Distance float32
}

// ResultSet maintains the k smallest elements by distance in a sorted array.
// It creates a fixed-size array of length k and inserts new elements in sorted order.
// It performs about 3x faster than the priority queue approach, as it avoids
// the overhead of heap operations and memory allocations.
type ResultSet struct {
	data []Result
	k    int
}

func NewResultSet(k int) *ResultSet {
	return &ResultSet{
		data: make([]Result, 0, k),
		k:    k,
	}
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
