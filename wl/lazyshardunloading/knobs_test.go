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
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDefaultsForNilKnobs(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	sn := &snapshotter{knobs: Knobs{}.withDefaults(), logger: logger}
	require.Equal(t, &snapshot{
		idleCheckInterval: DefaultIdleCheckInterval,
		minResidency:      DefaultMinResidency,
		interval:          DefaultInterval,
		rate:              DefaultUnloadRate,
		candidates:        DefaultMakeRoomCandidates,
		workers:           DefaultUnloadWorkers,
	}, sn.read())
}

func TestSnapshotSanitisesKnobs(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testKnobs)
		check     func(t *testing.T, s *snapshot)
		wantLogs  int
	}{
		{
			name:      "valid watermarks",
			configure: func(k *testKnobs) { set(nil, k.high, 0.9); set(nil, k.low, 0.7) },
			check:     func(t *testing.T, s *snapshot) { require.Equal(t, [2]float64{0.9, 0.7}, [2]float64{s.high, s.low}) },
		},
		{
			name:      "high of exactly one",
			configure: func(k *testKnobs) { set(nil, k.high, 1.0); set(nil, k.low, 0.5) },
			check:     func(t *testing.T, s *snapshot) { require.Equal(t, 1.0, s.high) },
		},
		{
			name:      "low above high is off",
			configure: func(k *testKnobs) { set(nil, k.high, 0.5); set(nil, k.low, 0.7) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.high+s.low) },
			wantLogs:  1,
		},
		{
			name:      "equal watermarks are off",
			configure: func(k *testKnobs) { set(nil, k.high, 0.7); set(nil, k.low, 0.7) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.high+s.low) },
			wantLogs:  1,
		},
		{
			name:      "only high set is off",
			configure: func(k *testKnobs) { set(nil, k.high, 0.8) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.high) },
			wantLogs:  1,
		},
		{
			name:      "high above one is off",
			configure: func(k *testKnobs) { set(nil, k.high, 1.5); set(nil, k.low, 0.5) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.high) },
			wantLogs:  1,
		},
		{
			name:      "NaN watermarks are off",
			configure: func(k *testKnobs) { set(nil, k.high, math.NaN()); set(nil, k.low, 0.5) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.high) },
			wantLogs:  1,
		},
		{
			name:      "idle_ttl below min_residency is off",
			configure: func(k *testKnobs) { set(nil, k.idleTTL, 10*time.Second); set(nil, k.minResidency, time.Minute) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.idleTTL) },
			wantLogs:  1,
		},
		{
			name:      "idle_ttl equal to min_residency",
			configure: func(k *testKnobs) { set(nil, k.idleTTL, time.Minute); set(nil, k.minResidency, time.Minute) },
			check:     func(t *testing.T, s *snapshot) { require.Equal(t, time.Minute, s.idleTTL) },
		},
		{
			name: "out-of-range counts are clamped",
			configure: func(k *testKnobs) {
				set(nil, k.workers, 1000)
				set(nil, k.candidates, 1<<20)
				set(nil, k.rate, -3)
				set(nil, k.maxLoaded, -1)
			},
			check: func(t *testing.T, s *snapshot) {
				require.Equal(t, MaxUnloadWorkers, s.workers)
				require.Equal(t, MaxMakeRoomCandidates, s.candidates)
				require.Zero(t, s.rate)
				require.Zero(t, s.maxLoaded)
			},
		},
		{
			name:      "non-positive intervals take their defaults",
			configure: func(k *testKnobs) { set(nil, k.interval, 0); set(nil, k.idleCheckInterval, -time.Second) },
			check: func(t *testing.T, s *snapshot) {
				require.Equal(t, DefaultInterval, s.interval)
				require.Equal(t, DefaultIdleCheckInterval, s.idleCheckInterval)
			},
		},
		{
			name:      "an out-of-range share is off",
			configure: func(k *testKnobs) { set(nil, k.share, 1.5) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.nsShare) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			k := newTestKnobs()
			tt.configure(k)
			sn := &snapshotter{knobs: k.knobs(), logger: logger}
			tt.check(t, sn.read())
			sn.read()
			require.Len(t, hook.AllEntries(), tt.wantLogs, "an invalid combination is logged once")
		})
	}
}

func TestSnapshotLogsAgainOnlyWhenTheInvalidCombinationChanges(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	k := newTestKnobs()
	sn := &snapshotter{knobs: k.knobs(), logger: logger}
	steps := []struct {
		high, low float64
		wantLogs  int
	}{
		{high: 0.5, low: 0.7, wantLogs: 1},
		{high: 0.5, low: 0.7, wantLogs: 1},
		{high: 0.4, low: 0.7, wantLogs: 2},
		{high: 0.9, low: 0.7, wantLogs: 2},
		{high: 0.4, low: 0.7, wantLogs: 3},
	}
	for i, step := range steps {
		set(t, k.high, step.high)
		set(t, k.low, step.low)
		sn.read()
		require.Len(t, hook.AllEntries(), step.wantLogs, "step %d", i)
	}
}
