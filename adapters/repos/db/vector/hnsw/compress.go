//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

const (
	centroids = 256
)

func (h *hnsw) initCompressedStore() error {
	store, err := lsmkv.New(fmt.Sprintf("%s/%s/%s", h.rootPath, h.className, h.shardName), "", h.logger, nil)
	if err != nil {
		return errors.Wrapf(err, "init hnsw")
	}
	err = store.CreateOrLoadBucket(context.Background(), helpers.CompressedObjectsBucketLSM)
	if err != nil {
		return errors.Wrapf(err, "init hnsw")
	}
	h.compressedStore = store
	return nil
}

func (h *hnsw) Compress(segments int, encoderType int, encoderDistribution int) error {
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	if h.nodes[0] == nil {
		return errors.New("Compress command cannot be executed before inserting some data. Please, insert your data first.")
	}
	err := h.initCompressedStore()
	if err != nil {
		return err
	}

	vec, _ := h.vectorForID(context.Background(), h.nodes[0].id)
	dims := len(vec)
	// segments == 0 (default value) means use as many sements as dimensions
	if segments <= 0 {
		segments = dims
	}
	h.pq = ssdhelpers.NewProductQuantizer(segments, centroids, h.distancerProvider, dims, ssdhelpers.Encoder(encoderType), ssdhelpers.EncoderDistribution(encoderDistribution))

	data := h.cache.all()
	cleanData := make([][]float32, 0, len(data))
	for _, point := range data {
		if point == nil {
			continue
		}
		cleanData = append(cleanData, point)
	}
	h.compressedVectorsCache.grow(uint64(len(data)))
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
