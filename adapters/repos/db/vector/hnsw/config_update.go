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
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
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

	immutableFields := []immutableParameter{
		{
			name:     "efConstruction",
			accessor: func(c ent.UserConfig) interface{} { return c.EFConstruction },
		},
		{
			name:     "maxConnections",
			accessor: func(c ent.UserConfig) interface{} { return c.MaxConnections },
		},
		{
			// NOTE: There isn't a technical reason for this to be immutable, it
			// simply hasn't been implemented yet. It would require to stop the
			// current timer and start a new one. Certainly possible, but let's see
			// if anyone actually needs this before implementing it.
			name:     "cleanupIntervalSeconds",
			accessor: func(c ent.UserConfig) interface{} { return c.CleanupIntervalSeconds },
		},
		{
			name:     "distance",
			accessor: func(c ent.UserConfig) interface{} { return c.Distance },
		},
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}

	return nil
}

type immutableParameter struct {
	accessor func(c ent.UserConfig) interface{}
	name     string
}

func validateImmutableField(u immutableParameter,
	previous, next ent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
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

	// Store automatically as a lock here would be very expensive, this value is
	// read on every single user-facing search, which can be highly concurrent
	atomic.StoreInt64(&h.ef, int64(parsed.EF))
	atomic.StoreInt64(&h.efMin, int64(parsed.DynamicEFMin))
	atomic.StoreInt64(&h.efMax, int64(parsed.DynamicEFMax))
	atomic.StoreInt64(&h.efFactor, int64(parsed.DynamicEFFactor))
	atomic.StoreInt64(&h.flatSearchCutoff, int64(parsed.FlatSearchCutoff))

	if !parsed.PQ.Enabled && !parsed.BQ.Enabled {
		callback()
		return nil
	}

	h.pqConfig = parsed.PQ
	if asyncEnabled() {
		callback()
		return nil
	}

	if !h.compressed.Load() {
		// the compression will fire the callback once it's complete
		return h.TurnOnCompression(callback)
	} else {
		h.compressor.SetCacheMaxSize(int64(parsed.VectorCacheMaxObjects))
		callback()
		return nil
	}
}

func asyncEnabled() bool {
	return config.Enabled(os.Getenv("ASYNC_INDEXING"))
}

func (h *hnsw) TurnOnCompression(callback func()) error {
	h.logger.WithField("action", "compress").Info("switching to compressed vectors")

	err := ent.ValidatePQConfig(h.pqConfig)
	if err != nil {
		callback()
		return err
	}

	go h.compressThenCallback(callback)

	return nil
}

func (h *hnsw) compressThenCallback(callback func()) {
	defer callback()

	uc := ent.UserConfig{
		PQ: h.pqConfig,
		BQ: ent.BQConfig{
			Enabled: !h.pqConfig.Enabled,
		},
	}
	if err := h.compress(uc); err != nil {
		h.logger.Error(err)
		return
	}
	h.logger.WithField("action", "compress").Info("vector compression complete")
}
