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
)

type SPTAG interface {
	Get(id uint64) []byte
	Exists(id uint64) bool
	Upsert(id uint64, centroid []byte) error
	Delete(id uint64) error
	Search(query []byte, k int) ([]SearchResult, error)
	Split(oldID uint64, newID1, newID2 uint64, c1, c2 []byte) error
	Merge(oldID1, oldID2, newID uint64, newCentroid []byte) error
	ComputeDistance(a, b []byte) (float64, error)
}

type SearchResult struct {
	ID       uint64
	Distance float64
}

type BruteForceSPTAG struct {
	m         sync.RWMutex
	Centroids map[uint64][]byte
	quantizer *compressionhelpers.RotationalQuantizer
}

func NewBruteForceSPTAG(quantizer *compressionhelpers.RotationalQuantizer) *BruteForceSPTAG {
	return &BruteForceSPTAG{
		Centroids: make(map[uint64][]byte),
		quantizer: quantizer,
	}
}

func (b *BruteForceSPTAG) Get(id uint64) []byte {
	b.m.RLock()
	defer b.m.RUnlock()

	return b.Centroids[id]
}

func (b *BruteForceSPTAG) Upsert(id uint64, centroid []byte) error {
	b.m.Lock()
	defer b.m.Unlock()

	b.Centroids[id] = centroid
	return nil
}

func (b *BruteForceSPTAG) Delete(id uint64) error {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.Centroids, id)
	return nil
}

func (b *BruteForceSPTAG) Search(query []byte, k int) ([]uint64, error) {
	b.m.RLock()
	defer b.m.RUnlock()

	q := priorityqueue.NewMinWithId[byte](k)
	for id, centroid := range b.Centroids {
		dist, err := b.quantizer.DistanceBetweenCompressedVectors(query, centroid)
		if err != nil {
			return nil, err
		}

		q.Insert(id, dist)
		if q.Len() > k {
			q.Pop()
		}
	}

	results := make([]uint64, 0, q.Len())
	for q.Len() > 0 {
		item := q.Pop()
		results = append(results, item.ID)
	}

	return results, nil
}

func (b *BruteForceSPTAG) Split(oldID uint64, newID1, newID2 uint64, c1, c2 []byte) error {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.Centroids, oldID)
	b.Centroids[newID1] = c1
	b.Centroids[newID2] = c2
	return nil
}

func (b *BruteForceSPTAG) Merge(oldID1, oldID2, newID uint64, newCentroid []byte) error {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.Centroids, oldID1)
	delete(b.Centroids, oldID2)
	b.Centroids[newID] = newCentroid
	return nil
}
