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

package lazyshardunloading

import (
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// Defaults of the knobs; a nil knob takes its default.
const (
	DefaultIdleCheckInterval  = 30 * time.Second
	DefaultMinResidency       = 30 * time.Second
	DefaultUnloadRate         = 50
	DefaultInterval           = 5 * time.Second
	DefaultMakeRoomCandidates = 16
	DefaultUnloadWorkers      = 4
	// MaxMakeRoomCandidates bounds the per-load sample, and so the pooled buffers.
	MaxMakeRoomCandidates = 1024
	MaxUnloadWorkers      = 64
)

// Knobs are the runtime-config values the policy reads. They are snapshotted
// once per loop tick (and on [Unloader.RefreshConfig]); no request path reads them.
type Knobs struct {
	IdleTTL             *runtime.DynamicValue[time.Duration]
	IdleCheckInterval   *runtime.DynamicValue[time.Duration]
	MaxLoadedShards     *runtime.DynamicValue[int]
	MemoryHighWatermark *runtime.DynamicValue[float64]
	MemoryLowWatermark  *runtime.DynamicValue[float64]
	MinResidency        *runtime.DynamicValue[time.Duration]
	UnloadRate          *runtime.DynamicValue[int]
	Interval            *runtime.DynamicValue[time.Duration]
	MakeRoomCandidates  *runtime.DynamicValue[int]
	UnloadWorkers       *runtime.DynamicValue[int]
	NamespaceMaxShare   *runtime.DynamicValue[float64]
	Paused              *runtime.DynamicValue[bool]
}

func (k Knobs) withDefaults() Knobs {
	orDefault(&k.IdleTTL, 0)
	orDefault(&k.IdleCheckInterval, DefaultIdleCheckInterval)
	orDefault(&k.MaxLoadedShards, 0)
	orDefault(&k.MemoryHighWatermark, 0)
	orDefault(&k.MemoryLowWatermark, 0)
	orDefault(&k.MinResidency, DefaultMinResidency)
	orDefault(&k.UnloadRate, DefaultUnloadRate)
	orDefault(&k.Interval, DefaultInterval)
	orDefault(&k.MakeRoomCandidates, DefaultMakeRoomCandidates)
	orDefault(&k.UnloadWorkers, DefaultUnloadWorkers)
	orDefault(&k.NamespaceMaxShare, 0)
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
	idleTTL           time.Duration
	idleCheckInterval time.Duration
	minResidency      time.Duration
	interval          time.Duration
	maxLoaded         int64
	high, low         float64
	rate              int
	candidates        int
	workers           int
	nsShare           float64
	paused            bool
}

// snapshotter turns the knobs into snapshots, logging each invalid combination once.
type snapshotter struct {
	knobs  Knobs
	logger logrus.FieldLogger

	mu                sync.Mutex
	loggedWatermarks  [2]uint64 // float bits, so a NaN pair compares equal
	watermarksInvalid bool
	loggedIdle        [2]time.Duration
	idleInvalid       bool
}

func (sn *snapshotter) read() *snapshot {
	k := sn.knobs
	s := &snapshot{
		idleTTL:           max(k.IdleTTL.Get(), 0),
		idleCheckInterval: k.IdleCheckInterval.Get(),
		minResidency:      max(k.MinResidency.Get(), 0),
		interval:          k.Interval.Get(),
		maxLoaded:         int64(max(k.MaxLoadedShards.Get(), 0)),
		high:              k.MemoryHighWatermark.Get(),
		low:               k.MemoryLowWatermark.Get(),
		rate:              max(k.UnloadRate.Get(), 0),
		candidates:        min(max(k.MakeRoomCandidates.Get(), 0), MaxMakeRoomCandidates),
		workers:           min(max(k.UnloadWorkers.Get(), 1), MaxUnloadWorkers),
		nsShare:           k.NamespaceMaxShare.Get(),
		paused:            k.Paused.Get(),
	}
	if s.interval <= 0 {
		s.interval = DefaultInterval
	}
	if s.idleCheckInterval <= 0 {
		s.idleCheckInterval = DefaultIdleCheckInterval
	}
	if math.IsNaN(s.nsShare) || s.nsShare < 0 || s.nsShare > 1 {
		s.nsShare = 0
	}

	sn.mu.Lock()
	defer sn.mu.Unlock()
	if watermarksValid(s.high, s.low) {
		sn.watermarksInvalid = false
	} else {
		pair := [2]uint64{math.Float64bits(s.high), math.Float64bits(s.low)}
		if !sn.watermarksInvalid || sn.loggedWatermarks != pair {
			sn.logger.WithField("action", "lazy_shard_unloading_config").
				Errorf("memory watermarks ignored, memory-based unloading is off: need 0 < low < high <= 1 or both 0, got high=%v low=%v", s.high, s.low)
		}
		sn.watermarksInvalid, sn.loggedWatermarks = true, pair
		s.high, s.low = 0, 0
	}
	if s.idleTTL == 0 || s.idleTTL >= s.minResidency {
		sn.idleInvalid = false
	} else {
		pair := [2]time.Duration{s.idleTTL, s.minResidency}
		if !sn.idleInvalid || sn.loggedIdle != pair {
			sn.logger.WithField("action", "lazy_shard_unloading_config").
				Errorf("idle_ttl ignored, idle unloading is off: it must be 0 or at least min_residency, got idle_ttl=%v min_residency=%v", s.idleTTL, s.minResidency)
		}
		sn.idleInvalid, sn.loggedIdle = true, pair
		s.idleTTL = 0
	}
	return s
}

func watermarksValid(high, low float64) bool {
	if high == 0 && low == 0 {
		return true
	}
	return low > 0 && low < high && high <= 1
}
