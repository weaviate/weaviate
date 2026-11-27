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

package config

import (
	"fmt"
	"math"
	"time"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	DefaultIdleShardCollectorCheckInterval = 30 * time.Second
	DefaultIdleShardCollectorUnloadRate    = 300
	DefaultIdleShardCollectorUnloadWorkers = 4
	MaxIdleShardCollectorUnloadWorkers     = 64
)

// IdleShardCollector holds the runtime knobs of the licensed idle shard
// collector. Every knob is a runtime config with its default in code; there are
// no env vars. They parse and validate in every mode but act only on a licensed
// node with lazy loading on. Not read from the config file: FromEnv sets them.
type IdleShardCollector struct {
	IdleTTL       *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	CheckInterval *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	// UnloadRate caps the shards one check queues for unloading.
	UnloadRate    *runtime.DynamicValue[int]  `json:"-" yaml:"-"`
	UnloadWorkers *runtime.DynamicValue[int]  `json:"-" yaml:"-"`
	Paused        *runtime.DynamicValue[bool] `json:"-" yaml:"-"`
}

// ValidateIdleShardCollectorOptionalDuration accepts 0 (off) or at least 1s.
func ValidateIdleShardCollectorOptionalDuration(val time.Duration) error {
	if val == 0 || val >= time.Second {
		return nil
	}
	return fmt.Errorf("duration 0 (off) or at least 1s expected, got %v", val)
}

// ValidateIdleShardCollectorInterval accepts at least 1s.
func ValidateIdleShardCollectorInterval(val time.Duration) error {
	if val >= time.Second {
		return nil
	}
	return fmt.Errorf("duration of at least 1s expected, got %v", val)
}

func validateIntBetween(lo, hi int) func(int) error {
	return func(val int) error {
		if val < lo || val > hi {
			return fmt.Errorf("integer between %d and %d expected, got %d", lo, hi, val)
		}
		return nil
	}
}

// NewIdleShardCollector returns the knobs at their defaults, each validating
// the runtime overrides it receives; an invalid override keeps the previous
// value. There are no env vars: the knobs come only from the overrides file.
func NewIdleShardCollector() IdleShardCollector {
	return IdleShardCollector{
		IdleTTL:       mustDynamicValue(time.Duration(0), ValidateIdleShardCollectorOptionalDuration),
		CheckInterval: mustDynamicValue(DefaultIdleShardCollectorCheckInterval, ValidateIdleShardCollectorInterval),
		UnloadRate:    mustDynamicValue(DefaultIdleShardCollectorUnloadRate, validateIntBetween(0, math.MaxInt)),
		UnloadWorkers: mustDynamicValue(DefaultIdleShardCollectorUnloadWorkers, validateIntBetween(1, MaxIdleShardCollectorUnloadWorkers)),
		Paused:        runtime.NewDynamicValue(false),
	}
}

// mustDynamicValue panics on a default its own validator rejects, a programming error.
func mustDynamicValue[T runtime.DynamicType](def T, validate func(T) error) *runtime.DynamicValue[T] {
	v, err := runtime.NewDynamicValueWithValidation(def, validate)
	if err != nil {
		panic(fmt.Sprintf("idle shard collector default %v: %v", def, err))
	}
	return v
}
