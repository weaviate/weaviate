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

package monitoring

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newTestGauge() prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_gauge", Help: "test"})
}

func TestGroupedGauge(t *testing.T) {
	t.Run("sum across owners", func(t *testing.T) {
		g := newTestGauge()
		a := NewGroupedGauge(g)
		b := NewGroupedGauge(g)
		c := NewGroupedGauge(g)

		a.Set(100)
		b.Set(250)
		c.Set(50)
		require.Equal(t, float64(400), testutil.ToFloat64(g))

		b.Set(300) // +50 delta
		require.Equal(t, float64(450), testutil.ToFloat64(g))

		a.Set(10) // -90 delta
		require.Equal(t, float64(360), testutil.ToFloat64(g))
	})

	t.Run("inc dec mixed with set", func(t *testing.T) {
		g := newTestGauge()
		a := NewGroupedGauge(g)
		b := NewGroupedGauge(g)

		a.Set(100)
		a.Inc()
		a.Inc()
		a.Dec()
		require.Equal(t, float64(101), testutil.ToFloat64(g))

		b.Set(50)
		require.Equal(t, float64(151), testutil.ToFloat64(g))

		a.Set(0)
		require.Equal(t, float64(50), testutil.ToFloat64(g))
		b.Inc()
		require.Equal(t, float64(51), testutil.ToFloat64(g))
	})

	t.Run("reset removes contribution", func(t *testing.T) {
		g := newTestGauge()
		a := NewGroupedGauge(g)
		b := NewGroupedGauge(g)

		a.Set(100)
		b.Set(200)
		require.Equal(t, float64(300), testutil.ToFloat64(g))

		a.Reset()
		require.Equal(t, float64(200), testutil.ToFloat64(g))

		a.Reset() // idempotent
		require.Equal(t, float64(200), testutil.ToFloat64(g))

		a.Set(75)
		require.Equal(t, float64(275), testutil.ToFloat64(g))
	})

	t.Run("concurrent inc across owners", func(t *testing.T) {
		const owners = 100
		const ops = 1000
		g := newTestGauge()
		gs := make([]*GroupedGauge, owners)
		for i := range gs {
			gs[i] = NewGroupedGauge(g)
		}

		var wg sync.WaitGroup
		for _, gg := range gs {
			wg.Add(1)
			go func(gg *GroupedGauge) {
				defer wg.Done()
				for j := 0; j < ops; j++ {
					gg.Inc()
				}
			}(gg)
		}
		wg.Wait()

		require.Equal(t, float64(owners*ops), testutil.ToFloat64(g))
	})

	t.Run("concurrent set same owner converges", func(t *testing.T) {
		const writers = 50
		const iters = 200
		g := newTestGauge()
		gg := NewGroupedGauge(g)

		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(v float64) {
				defer wg.Done()
				for j := 0; j < iters; j++ {
					gg.Set(v)
				}
			}(float64(i + 1))
		}
		wg.Wait()

		gg.mu.Lock()
		last := gg.last
		gg.mu.Unlock()
		require.Equal(t, last, testutil.ToFloat64(g))
	})

	t.Run("set with same value is a no-op", func(t *testing.T) {
		g := newTestGauge()
		gg := NewGroupedGauge(g)

		gg.Set(42)
		require.Equal(t, float64(42), testutil.ToFloat64(g))

		// repeated Set with the same value must not touch the shared gauge
		// (HNSW's deferred SetSize fires per-insert with unchanged len(nodes))
		for i := 0; i < 1000; i++ {
			gg.Set(42)
		}
		require.Equal(t, float64(42), testutil.ToFloat64(g))
	})

	t.Run("nil safe", func(t *testing.T) {
		var g *GroupedGauge
		require.NotPanics(t, func() {
			g.Set(1)
			g.Inc()
			g.Dec()
			g.Add(5)
			g.Sub(2)
			g.Reset()
		})
	})
}

func TestAsSettable(t *testing.T) {
	t.Run("ungrouped returns raw gauge", func(t *testing.T) {
		g := newTestGauge()
		s := AsSettable(g, false)
		_, ok := s.(*GroupedGauge)
		require.False(t, ok)

		s.Set(42)
		require.Equal(t, float64(42), testutil.ToFloat64(g))
		s.Set(7) // last-writer-wins
		require.Equal(t, float64(7), testutil.ToFloat64(g))
	})

	t.Run("grouped wraps with delta semantics", func(t *testing.T) {
		g := newTestGauge()
		s1 := AsSettable(g, true)
		s2 := AsSettable(g, true)
		_, ok1 := s1.(*GroupedGauge)
		_, ok2 := s2.(*GroupedGauge)
		require.True(t, ok1)
		require.True(t, ok2)

		s1.Set(10)
		s2.Set(20)
		require.Equal(t, float64(30), testutil.ToFloat64(g))

		ResetGrouped(s1)
		require.Equal(t, float64(20), testutil.ToFloat64(g))
	})

	t.Run("reset grouped ignores plain gauges", func(t *testing.T) {
		g := newTestGauge()
		plain := AsSettable(g, false)
		plain.Set(5)
		ResetGrouped(plain)
		require.Equal(t, float64(5), testutil.ToFloat64(g))
	})
}
