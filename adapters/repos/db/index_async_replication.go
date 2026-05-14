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

package db

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
)

// maxDurationMillis is the largest int64 millisecond value that still fits in
// a time.Duration (nanoseconds) without wrapping. Used to reject schema inputs
// that would otherwise overflow during the *time.Millisecond conversion and
// silently clamp to the minimum.
const maxDurationMillis = int64(math.MaxInt64 / int64(time.Millisecond))

// asyncReplicationConfigFromModel builds an AsyncReplicationConfig from the
// class model and multitenancy flag.
//
// The returned config encodes the three-tier hierarchy:
//
//   - Code defaults: chosen based on multiTenancyEnabled (single vs multi-tenant).
//   - Per-class API / schema overrides: stored in classOverrides (pointer fields).
//     Non-nil means the user set a value via the collection API; nil means
//     "use the cluster default or code default".
//   - Global runtime-config DynamicValues: applied per hashbeat cycle via
//     Effective(globals) — NOT persisted here.
//
// This function intentionally never reads environment variables. Env-var
// values are surfaced through the GlobalConfig DynamicValues and applied by
// Effective(), so removing an env var restores the per-class schema value
// without any schema mutation.
//
// Frequency minimums (minFrequency, minFrequencyWhilePropagating) are enforced
// by clamping sub-minimum values up to the minimum and emitting a Warn log
// entry via the supplied logger. This is intentionally lenient so that
// collections created before a minimum was raised continue to load and update;
// the runtime defensive clamp in Effective() would otherwise be the only
// safety net and would silence the legacy values without operator visibility.
func asyncReplicationConfigFromModel(multiTenancyEnabled bool, cfg *models.ReplicationAsyncConfig, logger logrus.FieldLogger) (config AsyncReplicationConfig, err error) {
	// ---- Code defaults (tier 1) ----
	if multiTenancyEnabled {
		config.hashtreeHeight = defaultHashtreeHeightMultiTenant
	} else {
		config.hashtreeHeight = defaultHashtreeHeightSingleTenant
	}
	config.frequency = defaultFrequency
	config.frequencyWhilePropagating = defaultFrequencyWhilePropagating
	config.loggingFrequency = defaultLoggingFrequency
	config.diffBatchSize = defaultDiffBatchSize
	config.diffPerNodeTimeout = defaultDiffPerNodeTimeout
	config.prePropagationTimeout = defaultPrePropagationTimeout
	config.propagationTimeout = defaultPropagationTimeout
	config.propagationLimit = defaultPropagationLimit
	config.propagationConcurrency = defaultPropagationConcurrency
	config.propagationBatchSize = defaultPropagationBatchSize
	config.propagationDelay = defaultPropagationDelay
	if cfg == nil {
		return config, nil
	}

	// ---- Per-class API / schema overrides (tier 2) ----
	// Validate each API value and store it as a pointer in classOverrides.
	// Nil pointer means "not set by the user" — Effective() will skip it.

	if cfg.HashtreeHeight != nil {
		v, err := optParseInt("", int(*cfg.HashtreeHeight), minHashtreeHeight, maxHashtreeHeight)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("hashtreeHeight: %w", err)
		}
		config.classOverrides.hashtreeHeight = &v
	}

	if cfg.Frequency != nil {
		if *cfg.Frequency < 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("frequency must be >= 0")
		}
		if *cfg.Frequency > maxDurationMillis {
			return AsyncReplicationConfig{}, fmt.Errorf("frequency too large: %d ms exceeds max %d ms", *cfg.Frequency, maxDurationMillis)
		}
		v := time.Duration(*cfg.Frequency) * time.Millisecond
		if v < minFrequency {
			logger.WithFields(logrus.Fields{
				"field":     "frequency",
				"requested": v,
				"min":       minFrequency,
				"applied":   minFrequency,
			}).Warn("async-replication frequency below minimum; clamping to min")
			v = minFrequency
		}
		config.classOverrides.frequency = &v
	}

	if cfg.FrequencyWhilePropagating != nil {
		if *cfg.FrequencyWhilePropagating < 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("frequencyWhilePropagating must be >= 0")
		}
		if *cfg.FrequencyWhilePropagating > maxDurationMillis {
			return AsyncReplicationConfig{}, fmt.Errorf("frequencyWhilePropagating too large: %d ms exceeds max %d ms", *cfg.FrequencyWhilePropagating, maxDurationMillis)
		}
		v := time.Duration(*cfg.FrequencyWhilePropagating) * time.Millisecond
		if v < minFrequencyWhilePropagating {
			logger.WithFields(logrus.Fields{
				"field":     "frequencyWhilePropagating",
				"requested": v,
				"min":       minFrequencyWhilePropagating,
				"applied":   minFrequencyWhilePropagating,
			}).Warn("async-replication frequencyWhilePropagating below minimum; clamping to min")
			v = minFrequencyWhilePropagating
		}
		config.classOverrides.frequencyWhilePropagating = &v
	}

	if cfg.LoggingFrequency != nil {
		v := time.Duration(*cfg.LoggingFrequency) * time.Second
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("loggingFrequency must be > 0")
		}
		config.classOverrides.loggingFrequency = &v
	}

	if cfg.DiffBatchSize != nil {
		v, err := optParseInt("", int(*cfg.DiffBatchSize), minDiffBatchSize, maxDiffBatchSize)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("diffBatchSize: %w", err)
		}
		config.classOverrides.diffBatchSize = &v
	}

	if cfg.DiffPerNodeTimeout != nil {
		v := time.Duration(*cfg.DiffPerNodeTimeout) * time.Second
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("diffPerNodeTimeout must be > 0")
		}
		config.classOverrides.diffPerNodeTimeout = &v
	}

	if cfg.PrePropagationTimeout != nil {
		v := time.Duration(*cfg.PrePropagationTimeout) * time.Second
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("prePropagationTimeout must be > 0")
		}
		config.classOverrides.prePropagationTimeout = &v
	}

	if cfg.PropagationTimeout != nil {
		v := time.Duration(*cfg.PropagationTimeout) * time.Second
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("propagationTimeout must be > 0")
		}
		config.classOverrides.propagationTimeout = &v
	}

	if cfg.PropagationLimit != nil {
		v, err := optParseInt("", int(*cfg.PropagationLimit), minPropagationLimit, maxPropagationLimit)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("propagationLimit: %w", err)
		}
		config.classOverrides.propagationLimit = &v
	}

	if cfg.PropagationConcurrency != nil {
		v, err := optParseInt("", int(*cfg.PropagationConcurrency), minPropagationConcurrency, maxPropagationConcurrency)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("propagationConcurrency: %w", err)
		}
		config.classOverrides.propagationConcurrency = &v
	}

	if cfg.PropagationBatchSize != nil {
		v, err := optParseInt("", int(*cfg.PropagationBatchSize), minPropagationBatchSize, maxPropagationBatchSize)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("propagationBatchSize: %w", err)
		}
		config.classOverrides.propagationBatchSize = &v
	}

	if cfg.PropagationDelay != nil {
		v := time.Duration(*cfg.PropagationDelay) * time.Millisecond
		if v < 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("propagationDelay must be >= 0")
		}
		config.classOverrides.propagationDelay = &v
	}

	return config, nil
}

// optParseInt parses s as an integer when non-empty, otherwise uses defaultVal.
// Returns an error if the resolved value is outside [minVal, maxVal].
func optParseInt(s string, defaultVal, minVal, maxVal int) (val int, err error) {
	if s == "" {
		val = defaultVal
	} else {
		val, err = strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
	}

	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("value %d out of range: min %d, max %d", val, minVal, maxVal)
	}

	return val, nil
}
