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

package idleshardcollector

import (
	"time"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// Defaults of the knobs; a nil knob takes its default.
const (
	DefaultCheckInterval = 30 * time.Second
	DefaultUnloadRate    = 300
	DefaultUnloadWorkers = 4
	MaxUnloadWorkers     = 64
)

// Knobs are the runtime-config values the policy reads. They are snapshotted
// once per check (and on [Collector.RefreshConfig]); no request path reads them.
type Knobs struct {
	IdleTTL       *runtime.DynamicValue[time.Duration]
	CheckInterval *runtime.DynamicValue[time.Duration]
	UnloadRate    *runtime.DynamicValue[int]
	UnloadWorkers *runtime.DynamicValue[int]
	Paused        *runtime.DynamicValue[bool]
}

// KnobsFrom shares c's runtime values, so a runtime-config reload reaches the policy.
func KnobsFrom(c config.IdleShardCollector) Knobs {
	return Knobs{
		IdleTTL:       c.IdleTTL,
		CheckInterval: c.CheckInterval,
		UnloadRate:    c.UnloadRate,
		UnloadWorkers: c.UnloadWorkers,
		Paused:        c.Paused,
	}
}

func (k Knobs) withDefaults() Knobs {
	orDefault(&k.IdleTTL, 0)
	orDefault(&k.CheckInterval, DefaultCheckInterval)
	orDefault(&k.UnloadRate, DefaultUnloadRate)
	orDefault(&k.UnloadWorkers, DefaultUnloadWorkers)
	orDefault(&k.Paused, false)
	return k
}

func orDefault[T runtime.DynamicType](dv **runtime.DynamicValue[T], def T) {
	if *dv == nil {
		*dv = runtime.NewDynamicValue(def)
	}
}

// snapshot is one consistent reading of the knobs; it is never mutated once published.
type snapshot struct {
	idleTTL       time.Duration
	checkInterval time.Duration
	rate          int
	workers       int
	paused        bool
}

// accessClockTicks rounds d up to whole ticks: rounded down, an idle selection runs ahead of TryUnload's own check.
func accessClockTicks(d time.Duration) uint32 {
	seconds := d / time.Second
	if d%time.Second != 0 {
		seconds++
	}
	return uint32(min(seconds, 1<<31))
}

func readSnapshot(k Knobs) *snapshot {
	s := &snapshot{
		idleTTL:       max(k.IdleTTL.Get(), 0),
		checkInterval: k.CheckInterval.Get(),
		rate:          max(k.UnloadRate.Get(), 0),
		workers:       min(max(k.UnloadWorkers.Get(), 1), MaxUnloadWorkers),
		paused:        k.Paused.Get(),
	}
	if s.checkInterval <= 0 {
		s.checkInterval = DefaultCheckInterval
	}
	return s
}
