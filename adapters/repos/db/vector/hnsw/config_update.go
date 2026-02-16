//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func ValidateUserConfigUpdate(initial, updated config.VectorIndexConfig) error {
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
		{
			name:     "multivector enabled",
			accessor: func(c ent.UserConfig) interface{} { return c.Multivector.Enabled },
		},
		{
			name:     "muvera enabled",
			accessor: func(c ent.UserConfig) interface{} { return c.Multivector.MuveraConfig.Enabled },
		},
		{
			name:     "skipDefaultQuantization",
			accessor: func(c ent.UserConfig) interface{} { return c.SkipDefaultQuantization },
		},
		{
			name:     "trackDefaultQuantization",
			accessor: func(c ent.UserConfig) interface{} { return c.TrackDefaultQuantization },
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

func (h *hnsw) UpdateUserConfig(updated config.VectorIndexConfig, callback func()) error {
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

	h.acornSearch.Store(parsed.FilterStrategy == ent.FilterStrategyAcorn)

	// Handle adaptive EF config changes
	if parsed.AdaptiveEF.Enabled {
		// Recalibrate if not yet calibrated or if targetRecall changed
		needsCalibration := false
		existing := h.adaptiveEf.Load()
		if existing == nil {
			needsCalibration = true
		} else if existing.TargetRecall != parsed.AdaptiveEF.TargetRecall {
			needsCalibration = true
		}

		if needsCalibration {
			h.logger.WithFields(logrus.Fields{
				"action":       "calibrate_adaptive_ef",
				"shard":        h.shardName,
				"collection":   h.className,
				"targetVector": h.getTargetVector(),
				"targetRecall": parsed.AdaptiveEF.TargetRecall,
			}).Info("adaptive ef enabled, starting calibration")

			// Set immediately so stats endpoint reflects in-progress state
			// before the goroutine is scheduled.
			h.adaptiveEfCalibrating.Store(true)

			enterrors.GoWrapper(func() {
				defer callback()
				ctx := context.Background()
				if err := h.CalibrateAdaptiveEF(ctx, parsed.AdaptiveEF.TargetRecall); err != nil {
					h.logger.WithFields(logrus.Fields{
						"action":       "calibrate_adaptive_ef",
						"shard":        h.shardName,
						"collection":   h.className,
						"targetVector": h.getTargetVector(),
					}).WithError(err).Error("adaptive ef calibration failed")
				} else {
					h.logger.WithFields(logrus.Fields{
						"action":       "calibrate_adaptive_ef",
						"shard":        h.shardName,
						"collection":   h.className,
						"targetVector": h.getTargetVector(),
					}).Info("adaptive ef calibration complete")
				}
			}, h.logger)

			return nil
		}
	}

	if !parsed.PQ.Enabled && !parsed.BQ.Enabled && !parsed.SQ.Enabled && !parsed.RQ.Enabled {
		callback()
		return nil
	}

	// check if rq bits is immutable
	if h.rqConfig.Enabled && parsed.RQ.Enabled {
		if parsed.RQ.Bits != h.rqConfig.Bits {
			callback()
			return errors.Errorf("rq bits is immutable: attempted change from \"%v\" to \"%v\"",
				h.rqConfig.Bits, parsed.RQ.Bits)
		}
	}

	h.compressActionLock.Lock()
	h.pqConfig = parsed.PQ
	h.sqConfig = parsed.SQ
	h.bqConfig = parsed.BQ
	h.rqConfig = parsed.RQ
	h.compressActionLock.Unlock()
	if h.asyncIndexingEnabled {
		callback()
		return nil
	}

	if !h.compressed.Load() {
		// the compression will fire the callback once it's complete
		return h.Upgrade(callback)
	} else {
		h.compressor.SetCacheMaxSize(int64(parsed.VectorCacheMaxObjects))
		callback()
		return nil
	}
}

func (h *hnsw) Upgrade(callback func()) error {
	h.logger.WithFields(logrus.Fields{
		"action":       "compress",
		"shard":        h.shardName,
		"collection":   h.className,
		"targetVector": h.getTargetVector(),
	}).Info("switching to compressed vectors")

	err := ent.ValidatePQConfig(h.pqConfig)
	if err != nil {
		callback()
		return err
	}

	err = ent.ValidateRQConfig(h.rqConfig)
	if err != nil {
		callback()
		return err
	}

	enterrors.GoWrapper(func() { h.compressThenCallback(callback) }, h.logger)

	return nil
}

func (h *hnsw) compressThenCallback(callback func()) {
	defer callback()

	uc := ent.UserConfig{
		PQ: h.pqConfig,
		BQ: h.bqConfig,
		SQ: h.sqConfig,
		RQ: h.rqConfig,
	}
	if err := h.compress(uc); err != nil {
		h.logger.WithFields(logrus.Fields{
			"action":       "compress",
			"shard":        h.shardName,
			"collection":   h.className,
			"targetVector": h.getTargetVector(),
		}).WithError(err).Error("vector compression failed")
		return
	}
	h.logger.WithFields(logrus.Fields{
		"action":       "compress",
		"shard":        h.shardName,
		"collection":   h.className,
		"targetVector": h.getTargetVector(),
	}).Info("vector compression complete")
}
