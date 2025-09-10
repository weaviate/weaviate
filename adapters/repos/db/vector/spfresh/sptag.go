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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var _ SPTAG = (*BruteForceSPTAG)(nil)

type SPTAG interface {
	Init(dims int32, distancer distancer.Provider)
	// Get returns the centroid for the given ID or nil if not found.
	// The centroid may have been marked as deleted.
	Get(id uint64) *Centroid
	Exists(id uint64) bool
	Upsert(id uint64, centroid *Centroid) error
	IsEmpty() bool
	IsMarkedAsDeleted(id uint64) bool
	MarkAsDeleted(id uint64) error
	Search(query Vector, k int) ([]SearchResult, error)
	Quantizer() *compressionhelpers.RotationalQuantizer
}

type SearchResult struct {
	ID       uint64
	Distance float32
}

type Centroid struct {
	Vector Vector
	Radius float32
}

type BruteForceSPTAG struct {
	m          sync.RWMutex
	centroids  map[uint64]Centroid
	tombstones map[uint64]struct{}
	quantizer  *compressionhelpers.RotationalQuantizer
	distancer  *Distancer
}

func NewBruteForceSPTAG() *BruteForceSPTAG {
	return &BruteForceSPTAG{
		centroids:  make(map[uint64]Centroid),
		tombstones: make(map[uint64]struct{}),
	}
}

func (s *BruteForceSPTAG) Init(dims int32, distancer distancer.Provider) {
	s.m.Lock()
	defer s.m.Unlock()

	// TODO: seed
	seed := uint64(42)
	s.quantizer = compressionhelpers.NewRotationalQuantizer(int(dims), seed, 8, distancer)
	s.distancer = &Distancer{
		quantizer: s.quantizer,
		distancer: distancer,
	}
}

func (s *BruteForceSPTAG) Get(id uint64) *Centroid {
	s.m.RLock()
	defer s.m.RUnlock()

	centroid, exists := s.centroids[id]
	if !exists {
		return nil
	}

	return &centroid
}

func (s *BruteForceSPTAG) Upsert(id uint64, centroid *Centroid) error {
	s.m.Lock()
	defer s.m.Unlock()

	if _, deleted := s.tombstones[id]; deleted {
		return errors.New("cannot upsert a centroid that is marked as deleted")
	}

	s.centroids[id] = *centroid
	return nil
}

func (s *BruteForceSPTAG) MarkAsDeleted(id uint64) error {
	s.m.Lock()
	defer s.m.Unlock()

	if _, deleted := s.tombstones[id]; deleted {
		return errors.New("centroid already marked as deleted")
	}

	s.tombstones[id] = struct{}{}
	return nil
}

func (s *BruteForceSPTAG) IsMarkedAsDeleted(id uint64) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	_, deleted := s.tombstones[id]
	return deleted
}

func (s *BruteForceSPTAG) Quantizer() *compressionhelpers.RotationalQuantizer {
	return s.quantizer
}

func (s *BruteForceSPTAG) IsEmpty() bool {
	s.m.RLock()
	defer s.m.RUnlock()

	return len(s.centroids) == 0
}

func (s *BruteForceSPTAG) Len() (int, int) {
	s.m.RLock()
	defer s.m.RUnlock()

	var count int
	var deleted int
	for id := range s.centroids {
		count++
		if _, del := s.tombstones[id]; del {
			deleted++
			continue
		}
	}

	return count, deleted
}

func (s *BruteForceSPTAG) Search(query Vector, k int) ([]SearchResult, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	// if quantizer is null, the index is empty
	if s.quantizer == nil {
		return nil, nil
	}

	q := priorityqueue.NewMax[uint64](k)
	for id, centroid := range s.centroids {
		if _, deleted := s.tombstones[id]; deleted {
			continue
		}

		dist, err := centroid.Vector.Distance(s.distancer, query)
		if err != nil {
			return nil, err
		}

		q.Insert(id, dist)
		if q.Len() > k {
			q.Pop()
		}
	}

	results := make([]SearchResult, q.Len())
	i := len(results) - 1
	for q.Len() > 0 {
		element := q.Pop()
		results[i] = SearchResult{ID: element.ID, Distance: element.Dist}
		i--
	}

	return results, nil
}

func (s *BruteForceSPTAG) Exists(id uint64) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	if _, deleted := s.tombstones[id]; deleted {
		return false
	}
	_, exists := s.centroids[id]
	return exists
}
