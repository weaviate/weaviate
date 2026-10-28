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
	"os"
	"time"

	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/usecases/config/parser"
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
// unloading. Every knob is a runtime config; the env vars only seed them. They
// parse and validate in every mode but act only on a licensed node with lazy
// loading on. Not read from the config file: FromEnv always sets them.
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

// ValidateLazyShardUnloadingCombination checks the cross-field rules the
// per-field validators cannot: 0 < low < high <= 1 or both 0, and idle_ttl 0
// or at least min_residency. At runtime the policy turns an invalid
// combination off instead, because two knobs may change in separate reloads.
func ValidateLazyShardUnloadingCombination(c LazyShardUnloading) error {
	high, low := c.MemoryHighWatermark.Get(), c.MemoryLowWatermark.Get()
	if !(high == 0 && low == 0) && !(low > 0 && low < high && high <= 1) {
		return fmt.Errorf("LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK and LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK must satisfy 0 < low < high <= 1 or both be 0, got high=%v low=%v", high, low)
	}
	if ttl, residency := c.IdleTTL.Get(), c.MinResidency.Get(); ttl != 0 && ttl < residency {
		return fmt.Errorf("LAZY_SHARD_UNLOADING_IDLE_TTL must be 0 or at least LAZY_SHARD_UNLOADING_MIN_RESIDENCY, got %v < %v", ttl, residency)
	}
	return nil
}

func parseLazyShardUnloading(config *Config) error {
	c := &config.LazyShardUnloading
	durations := []struct {
		env      string
		def      time.Duration
		validate func(time.Duration) error
		dst      **runtime.DynamicValue[time.Duration]
	}{
		{"LAZY_SHARD_UNLOADING_IDLE_TTL", 0, ValidateLazyShardUnloadingOptionalDuration, &c.IdleTTL},
		{"LAZY_SHARD_UNLOADING_IDLE_CHECK_INTERVAL", DefaultLazyShardUnloadingIdleCheckInterval, ValidateLazyShardUnloadingInterval, &c.IdleCheckInterval},
		{"LAZY_SHARD_UNLOADING_MIN_RESIDENCY", DefaultLazyShardUnloadingMinResidency, ValidateLazyShardUnloadingOptionalDuration, &c.MinResidency},
		{"LAZY_SHARD_UNLOADING_INTERVAL", DefaultLazyShardUnloadingInterval, ValidateLazyShardUnloadingInterval, &c.Interval},
	}
	for _, d := range durations {
		if err := parser.ParseDynamicDurationWithValidation(d.env, d.def, d.validate,
			func(v *runtime.DynamicValue[time.Duration]) { *d.dst = v }); err != nil {
			return err
		}
	}
	ints := []struct {
		env      string
		def      int
		validate func(int) error
		dst      **runtime.DynamicValue[int]
	}{
		{"LAZY_SHARD_UNLOADING_MAX_LOADED_SHARDS", 0, parser.ValidateIntGreaterThanEqual0, &c.MaxLoadedShards},
		{"LAZY_SHARD_UNLOADING_UNLOAD_RATE", DefaultLazyShardUnloadingUnloadRate, parser.ValidateIntGreaterThanEqual0, &c.UnloadRate},
		{"LAZY_SHARD_UNLOADING_MAKE_ROOM_CANDIDATES", DefaultLazyShardUnloadingMakeRoomCandidates, validateIntBetween(0, MaxLazyShardUnloadingMakeRoomCandidates), &c.MakeRoomCandidates},
		{"LAZY_SHARD_UNLOADING_UNLOAD_WORKERS", DefaultLazyShardUnloadingUnloadWorkers, validateIntBetween(1, MaxLazyShardUnloadingUnloadWorkers), &c.UnloadWorkers},
	}
	for _, i := range ints {
		if err := parser.ParseDynamicIntWithValidation(i.env, i.def, i.validate,
			func(v *runtime.DynamicValue[int]) { *i.dst = v }); err != nil {
			return err
		}
	}
	floats := []struct {
		env string
		dst **runtime.DynamicValue[float64]
	}{
		{"LAZY_SHARD_UNLOADING_MEMORY_HIGH_WATERMARK", &c.MemoryHighWatermark},
		{"LAZY_SHARD_UNLOADING_MEMORY_LOW_WATERMARK", &c.MemoryLowWatermark},
		{"LAZY_SHARD_UNLOADING_NAMESPACE_MAX_SHARE", &c.NamespaceMaxShare},
	}
	for _, f := range floats {
		if err := parser.ParseDynamicFloatWithValidation(f.env, 0, ValidateLazyShardUnloadingFraction,
			func(v *runtime.DynamicValue[float64]) { *f.dst = v }); err != nil {
			return err
		}
	}
	c.Paused = runtime.NewDynamicValue(entcfg.Enabled(os.Getenv("LAZY_SHARD_UNLOADING_PAUSED")))
	return ValidateLazyShardUnloadingCombination(*c)
}
