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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (h *hnsw) Compress(cfg ent.UserConfig) error {
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	data := h.cache.All()
	if cfg.PQ.Enabled {
		h.shardedNodeLocks.RLock(0)
		node := h.nodes[0]
		h.shardedNodeLocks.RUnlock(0)

		if node == nil {
			return errors.New("data must be inserted before compress command can be executed")
		}

		vec, err := h.vectorForID(context.Background(), node.id)
		if err != nil {
			return fmt.Errorf("infer vector dimensions: %w", err)
		}
		dims := len(vec)

		// segments == 0 (default value) means use as many segments as dimensions
		if cfg.PQ.Segments <= 0 {
			cfg.PQ.Segments = dims
		}

		cleanData := make([][]float32, 0, len(data))
		for _, point := range data {
			if point == nil {
				continue
			}
			cleanData = append(cleanData, point)
		}

		h.compressor, err = compressionhelpers.NewPQCompressor(cfg.PQ, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store)
		if err != nil {
			return errors.Wrap(err, "Compressing vectors.")
		}
	} else {
		var err error
		h.compressor, err = compressionhelpers.NewBQCompressor(h.distancerProvider, 1e12, h.logger, h.store)
		if err != nil {
			return err
		}
	}
	compressionhelpers.Concurrently(uint64(len(data)),
		func(index uint64) {
			if data[index] == nil {
				return
			}
			h.compressor.Preload(index, data[index])
		})

	h.compressed.Store(true)
	h.cache.Drop()
	return nil
}
