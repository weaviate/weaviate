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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func (h *hnsw) compress(cfg ent.UserConfig) error {
	if !cfg.PQ.Enabled && !cfg.BQ.Enabled && !cfg.SQ.Enabled {
		return nil
	}
	h.compressActionLock.Lock()
	defer h.compressActionLock.Unlock()
	data := h.cache.All()
	if cfg.PQ.Enabled || cfg.SQ.Enabled {
		if h.isEmpty() {
			return errors.New("compress command cannot be executed before inserting some data")
		}
		cleanData := make([][]float32, 0, len(data))
		sampler := common.NewSparseFisherYatesIterator(len(data))
		for !sampler.IsDone() {
			// Sparse Fisher Yates sampling algorithm to choose random element
			sampledIndex := sampler.Next()
			if sampledIndex == nil {
				break
			}
			// Rather than just taking the cache dump at face value, let's explicitly
			// request the vectors. Otherwise we would miss any vector that's currently
			// not in the cache, for example because the cache is not hot yet after a
			// restart.
			p, err := h.cache.Get(context.Background(), uint64(*sampledIndex))
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
			if len(cleanData) >= cfg.PQ.TrainingLimit {
				break
			}
		}
		if cfg.PQ.Enabled {
			dims := int(h.dims)

			if cfg.PQ.Segments <= 0 {
				cfg.PQ.Segments = common.CalculateOptimalSegments(dims)
				h.pqConfig.Segments = cfg.PQ.Segments
			}

			var err error
			if !h.multivector.Load() || h.muvera.Load() {
				h.compressor, err = compressionhelpers.NewHNSWPQCompressor(
					cfg.PQ, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store,
					h.allocChecker)
			} else {
				h.compressor, err = compressionhelpers.NewHNSWPQMultiCompressor(
					cfg.PQ, h.distancerProvider, dims, 1e12, h.logger, cleanData, h.store,
					h.allocChecker)
			}
			if err != nil {
				h.pqConfig.Enabled = false
				return fmt.Errorf("compressing vectors: %w", err)
			}
		} else if cfg.SQ.Enabled {
			var err error
			if !h.multivector.Load() || h.muvera.Load() {
				h.compressor, err = compressionhelpers.NewHNSWSQCompressor(
					h.distancerProvider, 1e12, h.logger, cleanData, h.store,
					h.allocChecker)
			} else {
				h.compressor, err = compressionhelpers.NewHNSWSQMultiCompressor(
					h.distancerProvider, 1e12, h.logger, cleanData, h.store,
					h.allocChecker)
			}
			if err != nil {
				h.sqConfig.Enabled = false
				return fmt.Errorf("compressing vectors: %w", err)
			}
		}
		h.compressor.PersistCompression(h.commitLog)
	} else {
		var err error
		if !h.multivector.Load() || h.muvera.Load() {
			h.compressor, err = compressionhelpers.NewBQCompressor(
				h.distancerProvider, 1e12, h.logger, h.store, h.allocChecker)
		} else {
			h.compressor, err = compressionhelpers.NewBQMultiCompressor(
				h.distancerProvider, 1e12, h.logger, h.store, h.allocChecker)
		}
		if err != nil {
			return err
		}
	}
	if !h.multivector.Load() || h.muvera.Load() {
		compressionhelpers.Concurrently(h.logger, uint64(len(data)),
			func(index uint64) {
				if data[index] == nil {
					return
				}
				h.compressor.Preload(index, data[index])
			})
	} else {
		compressionhelpers.Concurrently(h.logger, uint64(len(data)),
			func(index uint64) {
				if len(data[index]) == 0 {
					return
				}
				docID, relativeID := h.cache.GetKeys(index)
				h.compressor.PreloadPassage(index, docID, relativeID, data[index])
			})
	}

	h.compressed.Store(true)
	h.cache.Drop()
	return nil
}
