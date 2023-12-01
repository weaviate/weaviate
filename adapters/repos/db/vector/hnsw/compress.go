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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (h *hnsw) Compress(cfg ent.PQConfig) error {
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
	if cfg.Segments <= 0 {
		cfg.Segments = dims
	}

	data := h.cache.All()
	cleanData := make([][]float32, 0, len(data))
	for _, point := range data {
		if point == nil {
			continue
		}
		cleanData = append(cleanData, point)
	}
	h.compressor, err = ssdhelpers.NewPQCompressor(cfg, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store)
	if err != nil {
		return errors.Wrap(err, "Compressing vectors.")
	}

	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	ssdhelpers.Concurrently(uint64(len(data)),
		func(index uint64) {
			if data[index] == nil {
				return
			}
			h.compressor.Preload(index, data[index])
		})
	/*if err := h.commitLog.AddPQ(h.pq.ExposeFields()); err != nil {
		return errors.Wrap(err, "Adding PQ to the commit logger")
	}*/

	h.compressed.Store(true)
	h.cache.Drop()
	return nil
}
