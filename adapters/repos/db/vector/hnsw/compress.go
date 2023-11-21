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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (h *hnsw) initCompressedBucket() error {
	err := h.store.CreateOrLoadBucket(context.Background(), helpers.VectorsHNSWPQBucketLSM)
	if err != nil {
		return fmt.Errorf("create or load bucket (compressed vectors store): %w", err)
	}
	h.compressedBucket = h.store.Bucket(helpers.VectorsHNSWPQBucketLSM)
	return nil
}

func (h *hnsw) Compress(cfg ent.PQConfig) error {
	if h.isEmpty() {
		return errors.New("Compress command cannot be executed before inserting some data. Please, insert your data first.")
	}
	err := h.initCompressedBucket()
	if err != nil {
		return fmt.Errorf("init compressed vector store: %w", err)
	}

	dims := int(h.dims)

	if cfg.Segments <= 0 {
		for i := 6; i > 0; i-- {
			if dims%i == 0 {
				cfg.Segments = dims / i
				h.pqConfig.Segments = cfg.Segments
				break
			}
		}
		h.logger.Error(dims, cfg.Segments)
	}

	h.pq, err = ssdhelpers.NewProductQuantizer(cfg, h.distancerProvider, dims)
	if err != nil {
		return fmt.Errorf("compress vectors: %w", err)
	}

	data := h.cache.All()
	cleanData := make([][]float32, 0, len(data))
	for _, point := range data {
		if point == nil {
			continue
		}
		cleanData = append(cleanData, point)
	}
	h.compressedVectorsCache.Grow(uint64(len(data)))
	h.pq.Fit(cleanData)

	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	ssdhelpers.Concurrently(uint64(len(data)),
		func(index uint64) {
			if data[index] == nil {
				return
			}

			encoded := h.pq.Encode(data[index])
			h.storeCompressedVector(index, encoded)
			h.compressedVectorsCache.Preload(index, encoded)
		})
	if err := h.commitLog.AddPQ(h.pq.ExposeFields()); err != nil {
		return fmt.Errorf("add PQ to commit logger: %w", err)
	}

	h.compressed.Store(true)
	h.cache.Drop()
	return nil
}

func (h *hnsw) storeCompressedVector(id uint64, vector []byte) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	h.compressedBucket.Put(key, vector)
}

func (h *hnsw) getCompressedVectorForID(ctx context.Context, id uint64) ([]byte, error) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)

	if vec, err := h.compressedBucket.Get(key); err == nil {
		return vec, nil
	} else if err != entlsmkv.NotFound {
		return nil, fmt.Errorf("getting vector '%d' from compressed store: %w", id, err)
	}

	// not found, fallback to uncompressed source
	h.logger.
		WithField("action", "compress").
		WithField("vector", id).
		Warnf("Vector not found in compressed store")

	vec, err := h.VectorForIDThunk(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting vector '%d' from store: %w", id, err)
	}

	return h.pq.Encode(h.normalizeVec(vec)), nil
}
