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
	Search(query []byte, k int) ([]uint64, error)
	Split(oldID uint64, newID1, newID2 uint64, c1, c2 []byte) error
	Merge(oldID1, oldID2, newID uint64, newCentroid []byte) error
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

func (d *BruteForceSPTAG) Get(id uint64) []byte {
	d.m.RLock()
	defer d.m.RUnlock()

	return d.Centroids[id]
}

func (d *BruteForceSPTAG) Upsert(id uint64, centroid []byte) error {
	d.m.Lock()
	defer d.m.Unlock()

	d.Centroids[id] = centroid
	return nil
}

func (d *BruteForceSPTAG) Delete(id uint64) error {
	d.m.Lock()
	defer d.m.Unlock()

	delete(d.Centroids, id)
	return nil
}

func (d *BruteForceSPTAG) Search(query []byte, k int) ([]uint64, error) {
	d.m.RLock()
	defer d.m.RUnlock()

	q := priorityqueue.NewMinWithId[byte](k)
	for id, centroid := range d.Centroids {
		dist, err := d.quantizer.DistanceBetweenCompressedVectors(query, centroid)
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

func (d *BruteForceSPTAG) Split(oldID uint64, newID1, newID2 uint64, c1, c2 []byte) error {
	d.m.Lock()
	defer d.m.Unlock()

	delete(d.Centroids, oldID)
	d.Centroids[newID1] = c1
	d.Centroids[newID2] = c2
	return nil
}

func (d *BruteForceSPTAG) Merge(oldID1, oldID2, newID uint64, newCentroid []byte) error {
	d.m.Lock()
	defer d.m.Unlock()

	delete(d.Centroids, oldID1)
	delete(d.Centroids, oldID2)
	d.Centroids[newID] = newCentroid
	return nil
}
