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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotDefaultsForNilKnobs(t *testing.T) {
	require.Equal(t, &snapshot{
		checkInterval: DefaultCheckInterval,
		rate:          DefaultUnloadRate,
		workers:       DefaultUnloadWorkers,
	}, readSnapshot(Knobs{}.withDefaults()))
}

func TestSnapshotSanitisesKnobs(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testKnobs)
		check     func(t *testing.T, s *snapshot)
	}{
		{
			name:      "idle_ttl is taken as set",
			configure: func(k *testKnobs) { set(nil, k.idleTTL, time.Second) },
			check:     func(t *testing.T, s *snapshot) { require.Equal(t, time.Second, s.idleTTL) },
		},
		{
			name:      "zero idle_ttl is off",
			configure: func(k *testKnobs) { set(nil, k.idleTTL, 0) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.idleTTL) },
		},
		{
			name:      "a negative idle_ttl is off",
			configure: func(k *testKnobs) { set(nil, k.idleTTL, -time.Second) },
			check:     func(t *testing.T, s *snapshot) { require.Zero(t, s.idleTTL) },
		},
		{
			name: "out-of-range counts are clamped",
			configure: func(k *testKnobs) {
				set(nil, k.workers, 1000)
				set(nil, k.rate, -3)
			},
			check: func(t *testing.T, s *snapshot) {
				require.Equal(t, MaxUnloadWorkers, s.workers)
				require.Zero(t, s.rate)
			},
		},
		{
			name:      "a non-positive check interval takes its default",
			configure: func(k *testKnobs) { set(nil, k.checkInterval, -time.Second) },
			check:     func(t *testing.T, s *snapshot) { require.Equal(t, DefaultCheckInterval, s.checkInterval) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := newTestKnobs()
			tt.configure(k)
			tt.check(t, readSnapshot(k.knobs()))
		})
	}
}

func TestAccessClockTicks(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want uint32
	}{
		{d: 0, want: 0},
		{d: time.Millisecond, want: 1},
		{d: time.Second, want: 1},
		{d: 1500 * time.Millisecond, want: 2},
		{d: time.Hour, want: 3600},
		{d: math.MaxInt64, want: 1 << 31},
	}
	for _, tt := range tests {
		t.Run(tt.d.String(), func(t *testing.T) {
			require.Equal(t, tt.want, accessClockTicks(tt.d))
		})
	}
}
