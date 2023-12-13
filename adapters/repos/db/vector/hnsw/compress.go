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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
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

func (h *hnsw) Compress(cfg ent.PQConfig) error {
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	data := h.cache.All()
	if cfg.Enabled {
		if h.isEmpty() {
			return errors.New("Compress command cannot be executed before inserting some data. Please, insert your data first.")
		}
		dims := int(h.dims)

		if cfg.Segments <= 0 {
			cfg.Segments = h.calculateOptimalSegments(dims)
			h.pqConfig.Segments = cfg.Segments
		}

		cleanData := make([][]float32, 0, len(data))
		for _, point := range data {
			if point == nil {
				continue
			}
			cleanData = append(cleanData, point)
		}

		var err error
		h.compressor, err = compressionhelpers.NewPQCompressor(cfg, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store)
		if err != nil {
			return errors.Wrap(err, "Compressing vectors.")
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
