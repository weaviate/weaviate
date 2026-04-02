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
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/models"
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
func asyncReplicationConfigFromModel(multiTenancyEnabled bool, cfg *models.ReplicationAsyncConfig) (config AsyncReplicationConfig, err error) {
	// ---- Code defaults (tier 1) ----
	if multiTenancyEnabled {
		config.maxWorkers = defaultAsyncReplicationMaxWorkersMultiTenant
		config.hashtreeHeight = defaultHashtreeHeightMultiTenant
	} else {
		config.maxWorkers = defaultAsyncReplicationMaxWorkersSingleTenant
		config.hashtreeHeight = defaultHashtreeHeightSingleTenant
	}
	config.frequency = defaultFrequency
	config.frequencyWhilePropagating = defaultFrequencyWhilePropagating
	config.aliveNodesCheckingFrequency = defaultAliveNodesCheckingFrequency
	config.loggingFrequency = defaultLoggingFrequency
	config.diffBatchSize = defaultDiffBatchSize
	config.diffPerNodeTimeout = defaultDiffPerNodeTimeout
	config.prePropagationTimeout = defaultPrePropagationTimeout
	config.propagationTimeout = defaultPropagationTimeout
	config.propagationLimit = defaultPropagationLimit
	config.propagationConcurrency = defaultPropagationConcurrency
	config.propagationBatchSize = defaultPropagationBatchSize
	config.initShieldCPUEveryN = defaultInitShieldCPUEveryN

	if cfg == nil {
		return config, nil
	}

	// ---- Per-class API / schema overrides (tier 2) ----
	// Validate each API value and store it as a pointer in classOverrides.
	// Nil pointer means "not set by the user" — Effective() will skip it.

	if cfg.MaxWorkers != nil {
		v, err := optParseInt("", int(*cfg.MaxWorkers), minMaxWorkers, maxMaxWorkers)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("maxWorkers: %w", err)
		}
		config.classOverrides.maxWorkers = &v
	}

	if cfg.HashtreeHeight != nil {
		v, err := optParseInt("", int(*cfg.HashtreeHeight), minHashtreeHeight, maxHashtreeHeight)
		if err != nil {
			return AsyncReplicationConfig{}, fmt.Errorf("hashtreeHeight: %w", err)
		}
		config.classOverrides.hashtreeHeight = &v
	}

	if cfg.Frequency != nil {
		v := time.Duration(*cfg.Frequency) * time.Millisecond
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("frequency must be > 0")
		}
		config.classOverrides.frequency = &v
	}

	if cfg.FrequencyWhilePropagating != nil {
		v := time.Duration(*cfg.FrequencyWhilePropagating) * time.Millisecond
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("frequencyWhilePropagating must be > 0")
		}
		config.classOverrides.frequencyWhilePropagating = &v
	}

	if cfg.AliveNodesCheckingFrequency != nil {
		v := time.Duration(*cfg.AliveNodesCheckingFrequency) * time.Millisecond
		if v <= 0 {
			return AsyncReplicationConfig{}, fmt.Errorf("aliveNodesCheckingFrequency must be > 0")
		}
		// NOTE: aliveNodesCheckingFrequency is stored for API compatibility but
		// does NOT affect how often the scheduler checks for newly-alive nodes.
		// The topology-watcher interval is controlled globally via the
		// AsyncReplicationAliveNodesCheckingFrequency runtime-config knob on the
		// AsyncReplicationScheduler, which is shared across all collections.
		// Per-class overrides of this field are therefore silently ignored at
		// runtime.
		config.classOverrides.aliveNodesCheckingFrequency = &v
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

	if cfg.InitShieldCPUEveryN != nil {
		v := int(*cfg.InitShieldCPUEveryN)
		if v < minInitShieldCPUEveryN || v > maxInitShieldCPUEveryN {
			return AsyncReplicationConfig{}, fmt.Errorf(
				"initShieldCPUEveryN value %d out of range: min %d, max %d",
				v, minInitShieldCPUEveryN, maxInitShieldCPUEveryN)
		}
		config.classOverrides.initShieldCPUEveryN = &v
	}

	return config, nil
}

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
