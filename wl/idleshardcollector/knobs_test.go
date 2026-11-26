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

	"github.com/weaviate/weaviate/usecases/config"
)

func TestKnobsShareTheRuntimeConfigValues(t *testing.T) {
	require.Equal(t, config.MaxIdleShardCollectorUnloadWorkers, MaxUnloadWorkers)
	c := config.NewIdleShardCollector()
	knobs := KnobsFrom(c)
	defaults := &snapshot{checkInterval: DefaultCheckInterval, rate: DefaultUnloadRate, workers: DefaultUnloadWorkers}
	require.Equal(t, defaults, readSnapshot(Knobs{}.withDefaults()))
	require.Equal(t, defaults, readSnapshot(knobs), "the runtime config defaults match the collector's")

	set(t, c.IdleTTL, 2*time.Second)
	set(t, c.CheckInterval, time.Second)
	set(t, c.UnloadRate, 7)
	set(t, c.UnloadWorkers, 3)
	set(t, c.Paused, true)
	require.Equal(t, &snapshot{idleTTL: 2 * time.Second, checkInterval: time.Second, rate: 7, workers: 3, paused: true}, readSnapshot(knobs))
}

func TestSnapshotSanitisesKnobs(t *testing.T) {
	k := newTestKnobs()
	set(t, k.idleTTL, -time.Second)
	set(t, k.checkInterval, -time.Second)
	set(t, k.rate, -3)
	set(t, k.workers, 1000)
	require.Equal(t, &snapshot{checkInterval: DefaultCheckInterval, workers: MaxUnloadWorkers}, readSnapshot(k.knobs()))
}

func TestAccessClockTicks(t *testing.T) {
	for _, tt := range []struct {
		d    time.Duration
		want uint32
	}{
		{d: 0, want: 0},
		{d: time.Millisecond, want: 1},
		{d: time.Second, want: 1},
		{d: 1500 * time.Millisecond, want: 2},
		{d: math.MaxInt64, want: 1 << 31},
	} {
		require.Equal(t, tt.want, accessClockTicks(tt.d), tt.d)
	}
}
