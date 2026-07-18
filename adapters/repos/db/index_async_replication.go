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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	entreplication "github.com/weaviate/weaviate/entities/replication"
)

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
// Rejection is delegated to entreplication.ValidateAsyncConfig, shared with
// proposal-time schema validation so the reject-set cannot drift.
//
// Frequency minimums (minFrequency, minFrequencyWhilePropagating) are enforced
// by clamping sub-minimum values up to the minimum and emitting a Warn log
// entry via the supplied logger. This is intentionally lenient so that
// collections created before a minimum was raised continue to load and update;
// the runtime defensive clamp in Effective() would otherwise be the only
// safety net and would silence the legacy values without operator visibility.
func asyncReplicationConfigFromModel(multiTenancyEnabled bool, cfg *models.ReplicationAsyncConfig, logger logrus.FieldLogger) (config AsyncReplicationConfig, err error) {
	if err := entreplication.ValidateAsyncConfig(cfg); err != nil {
		return AsyncReplicationConfig{}, err
	}

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
	// Store each validated API value as a pointer in classOverrides. Nil
	// pointer means "not set by the user" — Effective() will skip it.

	if cfg.HashtreeHeight != nil {
		v := int(*cfg.HashtreeHeight)
		config.classOverrides.hashtreeHeight = &v
	}

	if cfg.Frequency != nil {
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
		config.classOverrides.loggingFrequency = &v
	}

	if cfg.DiffBatchSize != nil {
		v := int(*cfg.DiffBatchSize)
		config.classOverrides.diffBatchSize = &v
	}

	if cfg.DiffPerNodeTimeout != nil {
		v := time.Duration(*cfg.DiffPerNodeTimeout) * time.Second
		config.classOverrides.diffPerNodeTimeout = &v
	}

	if cfg.PrePropagationTimeout != nil {
		v := time.Duration(*cfg.PrePropagationTimeout) * time.Second
		config.classOverrides.prePropagationTimeout = &v
	}

	if cfg.PropagationTimeout != nil {
		v := time.Duration(*cfg.PropagationTimeout) * time.Second
		config.classOverrides.propagationTimeout = &v
	}

	if cfg.PropagationLimit != nil {
		v := int(*cfg.PropagationLimit)
		config.classOverrides.propagationLimit = &v
	}

	if cfg.PropagationConcurrency != nil {
		v := int(*cfg.PropagationConcurrency)
		config.classOverrides.propagationConcurrency = &v
	}

	if cfg.PropagationBatchSize != nil {
		v := int(*cfg.PropagationBatchSize)
		config.classOverrides.propagationBatchSize = &v
	}

	if cfg.PropagationDelay != nil {
		v := time.Duration(*cfg.PropagationDelay) * time.Millisecond
		config.classOverrides.propagationDelay = &v
	}

	return config, nil
}
