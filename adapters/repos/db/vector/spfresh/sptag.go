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
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type SPTAG interface {
	Upsert(id uint64, centroid []float32) error
	Delete(id uint64) error
	Search(query []float32, k int) ([]uint64, error)
	Split(oldID uint64, newID1, newID2 uint64, c1, c2 []float32) error
	Merge(oldID1, oldID2, newID uint64, newCentroid []float32) error
}

type DummySPTAG struct {
	Centroids map[uint64][]float32
	distancer distancer.L2SquaredProvider
}

func NewDummySPTAG() *DummySPTAG {
	return &DummySPTAG{
		Centroids: make(map[uint64][]float32),
		distancer: distancer.NewL2SquaredProvider(),
	}
}

func (d *DummySPTAG) Upsert(id uint64, centroid []float32) error {
	d.Centroids[id] = centroid
	return nil
}

func (d *DummySPTAG) Delete(id uint64) error {
	delete(d.Centroids, id)
	return nil
}

func (d *DummySPTAG) Search(query []float32, k int) ([]uint64, error) {
	q := priorityqueue.NewMinWithId[float32](k)
	for id, centroid := range d.Centroids {
		dist, err := d.distancer.SingleDist(query, centroid)
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
