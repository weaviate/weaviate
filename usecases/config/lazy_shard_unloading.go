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
	DefaultLazyShardUnloadingIdleCheckInterval  = 30 * time.Second
	DefaultLazyShardUnloadingMinResidency       = 30 * time.Second
	DefaultLazyShardUnloadingUnloadRate         = 50
	DefaultLazyShardUnloadingInterval           = 5 * time.Second
	DefaultLazyShardUnloadingMakeRoomCandidates = 16
	DefaultLazyShardUnloadingUnloadWorkers      = 4
	MaxLazyShardUnloadingMakeRoomCandidates     = 1024
	MaxLazyShardUnloadingUnloadWorkers          = 64
)

// LazyShardUnloading holds the runtime knobs of the licensed lazy shard
// unloading. Every knob is a runtime config with its default in code; there are
// no env vars. They parse and validate in every mode but act only on a licensed
// node with lazy loading on. Not read from the config file: FromEnv sets them.
type LazyShardUnloading struct {
	IdleTTL             *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	IdleCheckInterval   *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	MaxLoadedShards     *runtime.DynamicValue[int]           `json:"-" yaml:"-"`
	MemoryHighWatermark *runtime.DynamicValue[float64]       `json:"-" yaml:"-"`
	MemoryLowWatermark  *runtime.DynamicValue[float64]       `json:"-" yaml:"-"`
	MinResidency        *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	UnloadRate          *runtime.DynamicValue[int]           `json:"-" yaml:"-"`
	Interval            *runtime.DynamicValue[time.Duration] `json:"-" yaml:"-"`
	MakeRoomCandidates  *runtime.DynamicValue[int]           `json:"-" yaml:"-"`
	UnloadWorkers       *runtime.DynamicValue[int]           `json:"-" yaml:"-"`
	NamespaceMaxShare   *runtime.DynamicValue[float64]       `json:"-" yaml:"-"`
	Paused              *runtime.DynamicValue[bool]          `json:"-" yaml:"-"`
}

// ValidateLazyShardUnloadingOptionalDuration accepts 0 (off) or at least 1s.
func ValidateLazyShardUnloadingOptionalDuration(val time.Duration) error {
	if val == 0 || val >= time.Second {
		return nil
	}
	return fmt.Errorf("duration 0 (off) or at least 1s expected, got %v", val)
}

// ValidateLazyShardUnloadingInterval accepts at least 1s.
func ValidateLazyShardUnloadingInterval(val time.Duration) error {
	if val >= time.Second {
		return nil
	}
	return fmt.Errorf("duration of at least 1s expected, got %v", val)
}

// ValidateLazyShardUnloadingFraction accepts a number in [0, 1].
func ValidateLazyShardUnloadingFraction(val float64) error {
	if math.IsNaN(val) || val < 0 || val > 1 {
		return fmt.Errorf("number between 0 and 1 expected, got %v", val)
	}
	return nil
}

func validateIntBetween(lo, hi int) func(int) error {
	return func(val int) error {
		if val < lo || val > hi {
			return fmt.Errorf("integer between %d and %d expected, got %d", lo, hi, val)
		}
		return nil
	}
}

// NewLazyShardUnloading returns the knobs at their defaults, each validating
// the runtime overrides it receives; an invalid override keeps the previous
// value. There are no env vars: the knobs come only from the overrides file.
func NewLazyShardUnloading() LazyShardUnloading {
	return LazyShardUnloading{
		IdleTTL:             mustDynamicValue(time.Duration(0), ValidateLazyShardUnloadingOptionalDuration),
		IdleCheckInterval:   mustDynamicValue(DefaultLazyShardUnloadingIdleCheckInterval, ValidateLazyShardUnloadingInterval),
		MaxLoadedShards:     mustDynamicValue(0, validateIntBetween(0, math.MaxInt)),
		MemoryHighWatermark: mustDynamicValue(0.0, ValidateLazyShardUnloadingFraction),
		MemoryLowWatermark:  mustDynamicValue(0.0, ValidateLazyShardUnloadingFraction),
		MinResidency:        mustDynamicValue(DefaultLazyShardUnloadingMinResidency, ValidateLazyShardUnloadingOptionalDuration),
		UnloadRate:          mustDynamicValue(DefaultLazyShardUnloadingUnloadRate, validateIntBetween(0, math.MaxInt)),
		Interval:            mustDynamicValue(DefaultLazyShardUnloadingInterval, ValidateLazyShardUnloadingInterval),
		MakeRoomCandidates:  mustDynamicValue(DefaultLazyShardUnloadingMakeRoomCandidates, validateIntBetween(0, MaxLazyShardUnloadingMakeRoomCandidates)),
		UnloadWorkers:       mustDynamicValue(DefaultLazyShardUnloadingUnloadWorkers, validateIntBetween(1, MaxLazyShardUnloadingUnloadWorkers)),
		NamespaceMaxShare:   mustDynamicValue(0.0, ValidateLazyShardUnloadingFraction),
		Paused:              runtime.NewDynamicValue(false),
	}
}

// mustDynamicValue panics on a default its own validator rejects, a programming error.
func mustDynamicValue[T runtime.DynamicType](def T, validate func(T) error) *runtime.DynamicValue[T] {
	v, err := runtime.NewDynamicValueWithValidation(def, validate)
	if err != nil {
		panic(fmt.Sprintf("lazy shard unloading default %v: %v", def, err))
	}
	return v
}
