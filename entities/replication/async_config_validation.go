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

// ValidateAsyncConfig is the single source of truth for the reject-set: it
// runs at schema proposal time and asyncReplicationConfigFromModel delegates
// to it, so the layers cannot drift. Sub-minimum frequencies are accepted
// (the db layer clamps them), and duration fields are checked after
// conversion so overflowed values stay rejected.
func ValidateAsyncConfig(cfg *models.ReplicationAsyncConfig) error {
	if cfg == nil {
		return nil
	}

	if cfg.HashtreeHeight != nil {
		if err := validateIntRange(int(*cfg.HashtreeHeight), MinHashtreeHeight, MaxHashtreeHeight); err != nil {
			return fmt.Errorf("hashtreeHeight: %w", err)
		}
	}

	if cfg.Frequency != nil {
		if *cfg.Frequency < 0 {
			return fmt.Errorf("frequency must be >= 0")
		}
		if *cfg.Frequency > MaxDurationMillis {
			return fmt.Errorf("frequency too large: %d ms exceeds max %d ms", *cfg.Frequency, MaxDurationMillis)
		}
	}

	if cfg.FrequencyWhilePropagating != nil {
		if *cfg.FrequencyWhilePropagating < 0 {
			return fmt.Errorf("frequencyWhilePropagating must be >= 0")
		}
		if *cfg.FrequencyWhilePropagating > MaxDurationMillis {
			return fmt.Errorf("frequencyWhilePropagating too large: %d ms exceeds max %d ms", *cfg.FrequencyWhilePropagating, MaxDurationMillis)
		}
	}

	if cfg.LoggingFrequency != nil {
		if time.Duration(*cfg.LoggingFrequency)*time.Second <= 0 {
			return fmt.Errorf("loggingFrequency must be > 0")
		}
	}

	if cfg.DiffBatchSize != nil {
		if err := validateIntRange(int(*cfg.DiffBatchSize), MinDiffBatchSize, MaxDiffBatchSize); err != nil {
			return fmt.Errorf("diffBatchSize: %w", err)
		}
	}

	if cfg.DiffPerNodeTimeout != nil {
		if time.Duration(*cfg.DiffPerNodeTimeout)*time.Second <= 0 {
			return fmt.Errorf("diffPerNodeTimeout must be > 0")
		}
	}

	if cfg.PrePropagationTimeout != nil {
		if time.Duration(*cfg.PrePropagationTimeout)*time.Second <= 0 {
			return fmt.Errorf("prePropagationTimeout must be > 0")
		}
	}

	if cfg.PropagationTimeout != nil {
		if time.Duration(*cfg.PropagationTimeout)*time.Second <= 0 {
			return fmt.Errorf("propagationTimeout must be > 0")
		}
	}

	if cfg.PropagationLimit != nil {
		if err := validateIntRange(int(*cfg.PropagationLimit), MinPropagationLimit, MaxPropagationLimit); err != nil {
			return fmt.Errorf("propagationLimit: %w", err)
		}
	}

	if cfg.PropagationConcurrency != nil {
		if err := validateIntRange(int(*cfg.PropagationConcurrency), MinPropagationConcurrency, MaxPropagationConcurrency); err != nil {
			return fmt.Errorf("propagationConcurrency: %w", err)
		}
	}

	if cfg.PropagationBatchSize != nil {
		if err := validateIntRange(int(*cfg.PropagationBatchSize), MinPropagationBatchSize, MaxPropagationBatchSize); err != nil {
			return fmt.Errorf("propagationBatchSize: %w", err)
		}
	}

	if cfg.PropagationDelay != nil {
		if time.Duration(*cfg.PropagationDelay)*time.Millisecond < 0 {
			return fmt.Errorf("propagationDelay must be >= 0")
		}
	}

	return nil
}

func validateIntRange(val, minVal, maxVal int) error {
	if val < minVal || val > maxVal {
		return fmt.Errorf("value %d out of range: min %d, max %d", val, minVal, maxVal)
	}
	return nil
}
