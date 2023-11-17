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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
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
	if h.nodes[0] == nil {
		return errors.New("data must be inserted before compress command can be executed")
	}
	err := h.initCompressedBucket()
	if err != nil {
		return fmt.Errorf("init compressed vector store: %w", err)
	}

	vec, err := h.vectorForID(context.Background(), h.nodes[0].id)
	if err != nil {
		return fmt.Errorf("infer vector dimensions: %w", err)
	}
	dims := len(vec)

	// segments == 0 (default value) means use as many segments as dimensions
	if cfg.Segments <= 0 {
		cfg.Segments = dims
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

func (h *hnsw) storeCompressedVector(index uint64, vector []byte) {
	Id := make([]byte, 8)
	binary.LittleEndian.PutUint64(Id, index)
	h.compressedBucket.Put(Id, vector)
}

func (h *hnsw) getCompressedVectorForID(ctx context.Context, id uint64) ([]byte, error) {
	vec, err := h.vectorForID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get vector for id: %w", err)
	}
	if h.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vec = distancer.Normalize(vec)
	}

	return h.pq.Encode(vec), nil
}
