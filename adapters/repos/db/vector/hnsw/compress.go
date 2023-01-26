//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdhelpers"
)

const (
	centroids = 256
)

func (h *hnsw) Compress(segments int) error {
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	if h.nodes[0] == nil {
		return errors.New("Compress command cannot be executed before inserting some data. Please, insert your data first.")
	}
	store, err := lsmkv.New(fmt.Sprintf("%s/%s", h.rootPath, h.className), "", h.logger, nil)
	if err != nil {
		return errors.Wrapf(err, "init hnsw")
	}
	err = store.CreateOrLoadBucket(context.Background(), helpers.CompressedObjectsBucketLSM)
	if err != nil {
		return errors.Wrapf(err, "init hnsw")
	}
	h.compressedStore = store

	vec, _ := h.vectorForID(context.Background(), h.nodes[0].id)
	dims := len(vec)
	// segments == 0 (default value) means use as many sements as dimensions
	if segments <= 0 {
		segments = dims
	}
	h.pq = ssdhelpers.NewProductQuantizer(segments, centroids, h.distancerProvider, dims, ssdhelpers.UseTileEncoder)

	data := h.cache.all()
	cleanData := make([][]float32, 0, len(data))
	for _, point := range data {
		if point == nil {
			continue
		}
		cleanData = append(cleanData, point)
	}
	h.compressedVectorsCache.grow(uint64(len(cleanData)))
	h.pq.Fit(cleanData)
	ssdhelpers.Concurrently(uint64(len(cleanData)),
		func(_, index uint64, _ *sync.Mutex) {
			encoded := h.pq.Encode(cleanData[index])
			h.storeCompressedVector(index, encoded)
			h.compressedVectorsCache.preload(index, encoded)
		})
	if err := h.commitLog.AddPQ(h.pq.ExposeFields()); err != nil {
		fmt.Println(err)
		return err
	}

	h.compressed.Store(true)
	h.cache.drop()
	// ToDo: clear cache
	return nil
}

//nolint:unused
func (h *hnsw) encodedVector(id uint64) ([]byte, error) {
	return h.compressedVectorsCache.get(context.Background(), id)
}

func (h *hnsw) storeCompressedVector(index uint64, vector []byte) {
	Id := make([]byte, 8)
	binary.LittleEndian.PutUint64(Id, index)
	h.compressedStore.Bucket(helpers.CompressedObjectsBucketLSM).Put(Id, vector)
}
