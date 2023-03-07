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
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func ValidateUserConfigUpdate(initial, updated schema.VectorIndexConfig) error {
	initialParsed, ok := initial.(ent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	updatedParsed, ok := updated.(ent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	immutableFields := []immutableInt{
		{
			name:     "efConstruction",
			accessor: func(c ent.UserConfig) int { return c.EFConstruction },
		},
		{
			name:     "maxConnections",
			accessor: func(c ent.UserConfig) int { return c.MaxConnections },
		},
		{
			// NOTE: There isn't a technical reason for this to be immutable, it
			// simply hasn't been implemented yet. It would require to stop the
			// current timer and start a new one. Certainly possible, but let's see
			// if anyone actually needs this before implementing it.
			name:     "cleanupIntervalSeconds",
			accessor: func(c ent.UserConfig) int { return c.CleanupIntervalSeconds },
		},
	}

	for _, u := range immutableFields {
		if err := validateImmutableIntField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}

	return nil
}

type immutableInt struct {
	accessor func(c ent.UserConfig) int
	name     string
}

func validateImmutableIntField(u immutableInt,
	previous, next ent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%d\" to \"%d\"",
			u.name, oldField, newField)
	}

	return nil
}

func (h *hnsw) UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	// Store atomatically as a lock here would be very expensive, this value is
	// read on every single user-facing search, which can be highly concurrent
	atomic.StoreInt64(&h.ef, int64(parsed.EF))
	atomic.StoreInt64(&h.efMin, int64(parsed.DynamicEFMin))
	atomic.StoreInt64(&h.efMax, int64(parsed.DynamicEFMax))
	atomic.StoreInt64(&h.efFactor, int64(parsed.DynamicEFFactor))
	atomic.StoreInt64(&h.flatSearchCutoff, int64(parsed.FlatSearchCutoff))

	if h.compressed.Load() {
		h.compressedVectorsCache.updateMaxSize(int64(parsed.VectorCacheMaxObjects))
	} else {
		h.cache.updateMaxSize(int64(parsed.VectorCacheMaxObjects))
	}
	// ToDo: check atomic operation
	if !h.compressed.Load() && parsed.PQ.Enabled {
		h.logger.WithField("action", "compress").Info("switching to compressed vectors")

		encoder, err := ent.ValidEncoder(parsed.PQ.Encoder.Type)
		if err != nil {
			callback()
			return err
		}
		encoderDistribution, err := ent.ValidEncoderDistribution(parsed.PQ.Encoder.Distribution)
		if err != nil {
			callback()
			return err
		}

		go func() {
			if err := h.Compress(parsed.PQ.Segments, parsed.PQ.Centroids, parsed.PQ.BitCompression, int(encoder), int(encoderDistribution)); err != nil {
				h.logger.Error(err)
				h.logger.Error(err)
				callback()
				return
			}
			h.logger.WithField("action", "compress").Info("vector compression complete")
			callback()
		}()
	}

	callback()
	return nil
}
