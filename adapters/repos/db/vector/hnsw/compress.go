package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
)

const (
	centroids = 256
)

func (h *hnsw) Compress(segments int) error {
	vec, _ := h.vectorForID(context.Background(), h.nodes[0].id)
	dims := len(vec)
	// segments == 0 (default value) means use as many sements as dimensions
	if segments <= 0 {
		segments = dims
	}
	h.pq = ssdhelpers.NewProductQuantizer(segments, centroids, ssdhelpers.NewDistanceProvider(h.distancerProvider), dims, ssdhelpers.UseKMeansEncoder)

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
		encoded := h.pq.Encode(data[index])
		h.storeCompressedVector(index, encoded)
		h.compressedVectorsCache.preload(index, encoded)
	})
	if err := h.commitLog.AddPQ(h.pq.ExposeFields()); err != nil {
		fmt.Println(err)
		return err
	}

	h.compressed = true
	h.cache.drop()
	//ToDo: clear cache
	return nil
}

func (h *hnsw) encodedVector(id uint64) ([]byte, error) {
	return h.compressedVectorsCache.get(context.Background(), id)
}

func (h *hnsw) storeCompressedVector(index uint64, vector []byte) {
	Id := make([]byte, 8)
	binary.LittleEndian.PutUint64(Id, index)
	h.compressedStore.Bucket(helpers.CompressedObjectsBucketLSM).Put(Id, vector)
}
