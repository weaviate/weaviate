package hnsw

import (
	"context"
	"sync"

	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
)

const (
	centroids = 256
)

func (h *hnsw) Compress() {
	vec, _ := h.vectorForID(context.Background(), h.nodes[0].id)
	dims := len(vec)
	h.pq = ssdhelpers.NewProductQuantizer(dims, centroids, ssdhelpers.NewDistanceProvider(h.distancerProvider), dims, ssdhelpers.UseKMeansEncoder)

	data := h.cache.all()
	h.compressedVectorsCache.grow(uint64(len(data)))
	h.pq.Fit(data)
	ssdhelpers.Concurrently(uint64(len(data)), func(_, index uint64, _ *sync.Mutex) {
		if data[index] == nil {
			return
		}
		h.Lock()

		err := h.growIndexToAccomodateNode(index, h.logger)
		if err != nil {
			h.Unlock()
			//ToDo: report error
			return
		}
		h.Unlock()
		h.compressedVectorsCache.preload(index, h.pq.Encode(data[index]))
	})

	h.compressed = true
	h.cache = nil
	//ToDo: clear cache
}

func (i *hnsw) encodedVector(id uint64) ([]byte, error) {
	return i.compressedVectorsCache.get(context.Background(), id)
}
