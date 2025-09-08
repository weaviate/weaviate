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

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var _ SPTAG = (*BruteForceSPTAG)(nil)

type SPTAG interface {
	Init(dims int32, distancer distancer.Provider)
	Get(id uint64) *Centroid
	Exists(id uint64) bool
	Upsert(id uint64, centroid *Centroid) error
	Delete(id uint64) error
	Search(query []byte, k int) ([]SearchResult, error)
}

type SearchResult struct {
	ID       uint64
	Distance float32
}

type Centroid struct {
	Vector []byte
	Radius float32
}

type BruteForceSPTAG struct {
	m         sync.RWMutex
	Centroids map[uint64]Centroid
	quantizer *compressionhelpers.RotationalQuantizer
}

func NewBruteForceSPTAG() *BruteForceSPTAG {
	return &BruteForceSPTAG{
		Centroids: make(map[uint64]Centroid),
	}
}

func (s *BruteForceSPTAG) Init(dims int32, distancer distancer.Provider) {
	s.m.Lock()
	defer s.m.Unlock()

	// TODO: seed
	seed := uint64(42)
	s.quantizer = compressionhelpers.NewRotationalQuantizer(int(dims), seed, 8, distancer)
}

func (s *BruteForceSPTAG) Get(id uint64) *Centroid {
	s.m.RLock()
	defer s.m.RUnlock()

	centroid, exists := s.Centroids[id]
	if !exists {
		return nil
	}

	return &centroid
}

func (s *BruteForceSPTAG) Upsert(id uint64, centroid *Centroid) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.Centroids[id] = *centroid
	return nil
}

func (s *BruteForceSPTAG) Delete(id uint64) error {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.Centroids, id)
	return nil
}

func (s *BruteForceSPTAG) Search(query []byte, k int) ([]SearchResult, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	// if quantizer is null, the index is empty
	if s.quantizer == nil {
		return nil, nil
	}

	q := priorityqueue.NewMin[uint64](k)
	for id, centroid := range s.Centroids {
		dist, err := s.quantizer.DistanceBetweenCompressedVectors(query, centroid.Vector)
		if err != nil {
			return nil, err
		}

		q.Insert(id, dist)
		if q.Len() > k {
			q.Pop()
		}
	}

	results := make([]SearchResult, 0, q.Len())
	for q.Len() > 0 {
		item := q.Pop()
		results = append(results, SearchResult{ID: item.ID, Distance: item.Dist})
	}

	return results, nil
}

func (s *BruteForceSPTAG) Exists(id uint64) bool {
	s.m.RLock()
	defer s.m.RUnlock()

	_, exists := s.Centroids[id]
	return exists
}
