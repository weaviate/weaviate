//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"

	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (h *hnsw) calculateOptimalSegments(dims int) int {
	if dims >= 2048 && dims%8 == 0 {
		return dims / 8
	} else if dims >= 768 && dims%6 == 0 {
		return dims / 6
	} else if dims >= 256 && dims%4 == 0 {
		return dims / 4
	} else if dims%2 == 0 {
		return dims / 2
	}
	return dims
}

func (h *hnsw) compress(cfg ent.UserConfig) error {
	if !cfg.PQ.Enabled && !cfg.BQ.Enabled {
		return nil
	}

	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	data := h.cache.All()
	if cfg.PQ.Enabled {
		if h.isEmpty() {
			return errors.New("Compress command cannot be executed before inserting some data. Please, insert your data first.")
		}
		dims := int(h.dims)

		if cfg.PQ.Segments <= 0 {
			cfg.PQ.Segments = h.calculateOptimalSegments(dims)
			h.pqConfig.Segments = cfg.PQ.Segments
		}

		cleanData := make([][]float32, 0, len(data))
		for i := range data {
			// Rather than just taking the cache dump at face value, let's explicitly
			// request the vectors. Otherwise we would miss any vector that's currently
			// not in the cache, for example because the cache is not hot yet after a
			// restart.
			p, err := h.cache.Get(context.Background(), uint64(i))
			if err != nil {
				var e storobj.ErrNotFound
				if errors.As(err, &e) {
					// already deleted, ignore
					continue
				} else {
					return fmt.Errorf("unexpected error obtaining vectors for fitting: %w", err)
				}
			}

			if p == nil {
				// already deleted, ignore
				continue
			}

			cleanData = append(cleanData, p)
		}

		var err error
		h.compressor, err = compressionhelpers.NewPQCompressor(cfg.PQ, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store)
		if err != nil {
			return fmt.Errorf("Compressing vectors: %w", err)
		}
		h.commitLog.AddPQ(h.compressor.ExposeFields())
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
