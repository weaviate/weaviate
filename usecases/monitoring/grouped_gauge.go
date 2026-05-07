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

	"github.com/prometheus/client_golang/prometheus"
)

// SettableGauge is the subset of prometheus.Gauge that per-shard metric
// wrappers depend on. Both prometheus.Gauge and *GroupedGauge satisfy it.
type SettableGauge interface {
	Set(float64)
	Inc()
	Dec()
	Add(float64)
	Sub(float64)
}

// GroupedGauge wraps a prometheus.Gauge with delta-based semantics so that
// multiple owners writing to the same underlying gauge produce a SUM rather
// than last-writer-wins. Each instance tracks its own last-set value; Set(v)
// is translated to Add(v-last) on the shared gauge.
//
// Used when PROMETHEUS_MONITORING_GROUP=true to aggregate per-shard gauges
// (vector_index_size, queue_size, etc.) at the node level. Without this
// wrapper, every shard's .Set on the shared {class="n/a",shard="n/a"} entry
// would clobber the others.
//
// On upgrade, grouped-mode users will see step changes upward in the affected
// metrics: values now reflect the per-node sum across all shards rather than
// one arbitrary shard's value.
type GroupedGauge struct {
	gauge prometheus.Gauge

	mu   sync.Mutex
	last float64
}

func NewGroupedGauge(gauge prometheus.Gauge) *GroupedGauge {
	return &GroupedGauge{gauge: gauge}
}

func (g *GroupedGauge) Set(v float64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if v == g.last {
		g.mu.Unlock()
		return
	}
	delta := v - g.last
	g.last = v
	g.mu.Unlock()
	g.gauge.Add(delta)
}

func (g *GroupedGauge) Inc() { g.Add(1) }
func (g *GroupedGauge) Dec() { g.Add(-1) }

func (g *GroupedGauge) Add(delta float64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.last += delta
	g.mu.Unlock()
	g.gauge.Add(delta)
}

func (g *GroupedGauge) Sub(delta float64) { g.Add(-delta) }

// Reset subtracts this owner's contribution from the shared gauge. Call on
// shard unload/drop, otherwise the contribution leaks until process restart.
func (g *GroupedGauge) Reset() {
	if g == nil {
		return
	}
	g.Set(0)
}

// AsSettable wraps gauge in a GroupedGauge when group is true, otherwise
// returns gauge directly.
func AsSettable(gauge prometheus.Gauge, group bool) SettableGauge {
	if group {
		return NewGroupedGauge(gauge)
	}
	return gauge
}

// ResetGrouped calls Reset on any *GroupedGauge in gauges; others are ignored.
func ResetGrouped(gauges ...SettableGauge) {
	for _, g := range gauges {
		if gg, ok := g.(*GroupedGauge); ok {
			gg.Reset()
		}
	}
}
