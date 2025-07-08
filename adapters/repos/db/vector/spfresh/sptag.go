package spfresh

import (
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type SPTAG interface {
	Upsert(id uint64, centroid []float32) error
	Delete(id uint64) error
	Search(query []float32, k int) ([]uint64, error)
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
