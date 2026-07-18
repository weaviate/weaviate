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

package replication

import (
	"fmt"
	"math"
	"time"

	"github.com/weaviate/weaviate/entities/models"
)

// Bounds for per-class async-replication schema values; the db layer also
// uses them to clamp global runtime-config values.
const (
	MinHashtreeHeight = 0
	MaxHashtreeHeight = 20

	MinDiffBatchSize = 1
	MaxDiffBatchSize = 10_000

	MinPropagationLimit = 1
	MaxPropagationLimit = 100_000

	MinPropagationConcurrency = 1
	MaxPropagationConcurrency = 20

	MinPropagationBatchSize = 1
	MaxPropagationBatchSize = 1_000
)

// MaxDurationMillis is the largest millisecond value that fits in a
// time.Duration without wrapping.
const MaxDurationMillis = int64(math.MaxInt64 / int64(time.Millisecond))

// fieldCheck pairs a field's pointer with its validation; slice order fixes which error ValidateAsyncConfig reports first.
type fieldCheck struct {
	ptr   **int64
	check func(int64) error
}

func fieldChecks(cfg *models.ReplicationAsyncConfig) []fieldCheck {
	return []fieldCheck{
		{&cfg.HashtreeHeight, func(v int64) error {
			if err := validateIntRange(int(v), MinHashtreeHeight, MaxHashtreeHeight); err != nil {
				return fmt.Errorf("hashtreeHeight: %w", err)
			}
			return nil
		}},
		{&cfg.Frequency, func(v int64) error {
			if v < 0 {
				return fmt.Errorf("frequency must be >= 0")
			}
			if v > MaxDurationMillis {
				return fmt.Errorf("frequency too large: %d ms exceeds max %d ms", v, MaxDurationMillis)
			}
			return nil
		}},
		{&cfg.FrequencyWhilePropagating, func(v int64) error {
			if v < 0 {
				return fmt.Errorf("frequencyWhilePropagating must be >= 0")
			}
			if v > MaxDurationMillis {
				return fmt.Errorf("frequencyWhilePropagating too large: %d ms exceeds max %d ms", v, MaxDurationMillis)
			}
			return nil
		}},
		{&cfg.LoggingFrequency, func(v int64) error {
			if time.Duration(v)*time.Second <= 0 {
				return fmt.Errorf("loggingFrequency must be > 0")
			}
			return nil
		}},
		{&cfg.DiffBatchSize, func(v int64) error {
			if err := validateIntRange(int(v), MinDiffBatchSize, MaxDiffBatchSize); err != nil {
				return fmt.Errorf("diffBatchSize: %w", err)
			}
			return nil
		}},
		{&cfg.DiffPerNodeTimeout, func(v int64) error {
			if time.Duration(v)*time.Second <= 0 {
				return fmt.Errorf("diffPerNodeTimeout must be > 0")
			}
			return nil
		}},
		{&cfg.PrePropagationTimeout, func(v int64) error {
			if time.Duration(v)*time.Second <= 0 {
				return fmt.Errorf("prePropagationTimeout must be > 0")
			}
			return nil
		}},
		{&cfg.PropagationTimeout, func(v int64) error {
			if time.Duration(v)*time.Second <= 0 {
				return fmt.Errorf("propagationTimeout must be > 0")
			}
			return nil
		}},
		{&cfg.PropagationLimit, func(v int64) error {
			if err := validateIntRange(int(v), MinPropagationLimit, MaxPropagationLimit); err != nil {
				return fmt.Errorf("propagationLimit: %w", err)
			}
			return nil
		}},
		{&cfg.PropagationConcurrency, func(v int64) error {
			if err := validateIntRange(int(v), MinPropagationConcurrency, MaxPropagationConcurrency); err != nil {
				return fmt.Errorf("propagationConcurrency: %w", err)
			}
			return nil
		}},
		{&cfg.PropagationBatchSize, func(v int64) error {
			if err := validateIntRange(int(v), MinPropagationBatchSize, MaxPropagationBatchSize); err != nil {
				return fmt.Errorf("propagationBatchSize: %w", err)
			}
			return nil
		}},
		{&cfg.PropagationDelay, func(v int64) error {
			if time.Duration(v)*time.Millisecond < 0 {
				return fmt.Errorf("propagationDelay must be >= 0")
			}
			return nil
		}},
	}
}

// ValidateAsyncConfig is the single source of truth for the reject-set: it
// runs at schema proposal time and asyncReplicationConfigFromModel delegates
// to it, so the layers cannot drift. Sub-minimum frequencies are accepted
// (the db layer clamps them), and duration fields are checked after
// conversion so overflowed values stay rejected.
func ValidateAsyncConfig(cfg *models.ReplicationAsyncConfig) error {
	if cfg == nil {
		return nil
	}
	for _, fc := range fieldChecks(cfg) {
		if *fc.ptr == nil {
			continue
		}
		if err := fc.check(**fc.ptr); err != nil {
			return err
		}
	}
	return nil
}

// SanitizeAsyncConfig returns a copy of cfg with each invalid field cleared (falling back to its default), keeping valid siblings, plus the dropped fields' errors.
func SanitizeAsyncConfig(cfg *models.ReplicationAsyncConfig) (*models.ReplicationAsyncConfig, []error) {
	if cfg == nil {
		return nil, nil
	}
	sanitized := *cfg
	var dropped []error
	for _, fc := range fieldChecks(&sanitized) {
		if *fc.ptr == nil {
			continue
		}
		if err := fc.check(**fc.ptr); err != nil {
			dropped = append(dropped, err)
			*fc.ptr = nil
		}
	}
	return &sanitized, dropped
}

func validateIntRange(val, minVal, maxVal int) error {
	if val < minVal || val > maxVal {
		return fmt.Errorf("value %d out of range: min %d, max %d", val, minVal, maxVal)
	}
	return nil
}
