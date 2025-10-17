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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Centroid struct {
	Uncompressed []float32
	Compressed   []byte
	Deleted      bool
}

func (c *Centroid) Distance(distancer *Distancer, v Vector) (float32, error) {
	switch v := v.(type) {
	case *RawVector:
		return distancer.DistanceBetweenVectors(c.Uncompressed, v.Data())
	case CompressedVector:
		return v.DistanceWithRaw(distancer, c.Compressed)
	default:
		return 0, errors.Errorf("unknown vector type: %T", v)
	}
}

type CentroidIndex interface {
	Insert(id uint64, centroid *Centroid) error
	Get(id uint64) *Centroid
	MarkAsDeleted(id uint64) error
	Exists(id uint64) bool
	Search(query []float32, k int) (*ResultSet, error)
}

var _ CentroidIndex = (*BruteForceIndex)(nil)

type BruteForceIndex struct {
	distancer distancer.Provider
	metrics   *Metrics

	centroids *common.PagedArray[atomic.Pointer[Centroid]]
	idLock    sync.RWMutex
	ids       []uint64
	counter   atomic.Int32
}

func NewBruteForceSPTAG(metrics *Metrics, distancer distancer.Provider, pages, pageSize uint64) *BruteForceIndex {
	return &BruteForceIndex{
		metrics:   metrics,
		centroids: common.NewPagedArray[atomic.Pointer[Centroid]](pages, pageSize),
		distancer: distancer,
	}
}

func (s *BruteForceIndex) Get(id uint64) *Centroid {
	page, slot := s.centroids.GetPageFor(id)
	if page == nil {
		return nil
	}

	return page[slot].Load()
}

func (s *BruteForceIndex) Insert(id uint64, centroid *Centroid) error {
	page, slot := s.centroids.EnsurePageFor(id)
	if page == nil {
		return errors.New("failed to allocate page")
	}

	page[slot].Store(centroid)

	s.idLock.Lock()
	s.ids = append(s.ids, id)
	s.idLock.Unlock()

	s.metrics.SetPostings(int(s.counter.Add(1)))

	return nil
}

func (s *BruteForceIndex) MarkAsDeleted(id uint64) error {
	for {
		page, slot := s.centroids.GetPageFor(id)
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
			Uncompressed: centroid.Uncompressed,
			Compressed:   centroid.Compressed,
			Deleted:      true,
		}

		if page[slot].CompareAndSwap(centroid, &newCentroid) {
			s.metrics.SetPostings(int(s.counter.Add(-1)))
			break
		}
	}

	return nil
}

func (s *BruteForceIndex) Exists(id uint64) bool {
	centroid := s.Get(id)
	if centroid == nil {
		return false
	}

	return !centroid.Deleted
}

var idsPool = sync.Pool{
	New: func() any {
		buf := make([]uint64, 0, 1024)
		return &buf
	},
}

func (s *BruteForceIndex) Search(query []float32, k int) (*ResultSet, error) {
	start := time.Now()
	defer s.metrics.CentroidSearchDuration(start)

	if k == 0 {
		return nil, nil
	}

	ids := *(idsPool.Get().(*[]uint64))
	ids = ids[:0]
	defer idsPool.Put(&ids)

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

		dist, err := s.distancer.SingleDist(c.Uncompressed, query)
		if err != nil {
			return nil, err
		}

		q.Insert(ids[i], dist)
	}

	return q, nil
}
