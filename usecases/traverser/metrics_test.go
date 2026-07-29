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

package traverser

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TestMetricsLabels pins the label values each Metrics method emits. Currying
// query_type at construction and switching to WithLabelValues must keep the same
// (class_name, query_type) / (query_type, operation, class_name) mapping. It
// drives off the real monitoring.GetMetrics() vecs so that reordering a label
// list in prometheus.go — which would silently mislabel the positional
// AddUsageDimensions call — flips these assertions. Because those vecs live on
// the shared default registry, assertions measure deltas around each action.
func TestMetricsLabels(t *testing.T) {
	prom := monitoring.GetMetrics()

	newMetrics := func(group bool) *Metrics {
		return NewMetrics(&monitoring.PrometheusMetrics{
			QueriesCount:            prom.QueriesCount,
			QueriesDurations:        prom.QueriesDurations,
			QueryDimensions:         prom.QueryDimensions,
			QueryDimensionsCombined: prom.QueryDimensionsCombined,
			Group:                   group,
		})
	}

	t.Run("queries count aggregate and get use distinct query_type", func(t *testing.T) {
		m := newMetrics(false)
		aggregate := map[string]string{"class_name": "Foo", "query_type": "aggregate"}
		get := map[string]string{"class_name": "Foo", "query_type": "get_graphql"}
		beforeAggregate := gaugeValue(t, "concurrent_queries_count", aggregate)
		beforeGet := gaugeValue(t, "concurrent_queries_count", get)

		m.QueriesAggregateInc("Foo")
		m.QueriesGetInc("Foo")
		m.QueriesGetInc("Foo")
		m.QueriesGetDec("Foo")

		require.Equal(t, 1.0, gaugeValue(t, "concurrent_queries_count", aggregate)-beforeAggregate)
		require.Equal(t, 1.0, gaugeValue(t, "concurrent_queries_count", get)-beforeGet)
	})

	t.Run("observe duration records under get_graphql", func(t *testing.T) {
		m := newMetrics(false)
		get := map[string]string{"class_name": "Foo", "query_type": "get_graphql"}
		before := histogramCount(t, "queries_durations_ms", get)

		m.QueriesObserveDuration("Foo", 0)

		require.Equal(t, uint64(1), histogramCount(t, "queries_durations_ms", get)-before)
	})

	t.Run("usage dimensions keep query_type/operation/class_name order", func(t *testing.T) {
		m := newMetrics(false)
		dims := map[string]string{"query_type": "get", "operation": "objects", "class_name": "Foo"}
		beforeDims := counterValue(t, "query_dimensions_total", dims)
		beforeCombined := counterValue(t, "query_dimensions_combined_total", nil)

		m.AddUsageDimensions("Foo", "get", "objects", 5)

		require.Equal(t, 5.0, counterValue(t, "query_dimensions_total", dims)-beforeDims)
		require.Equal(t, 5.0, counterValue(t, "query_dimensions_combined_total", nil)-beforeCombined)
	})

	t.Run("group mode collapses class_name to n/a", func(t *testing.T) {
		m := newMetrics(true)
		na := map[string]string{"class_name": "n/a", "query_type": "get_graphql"}
		before := gaugeValue(t, "concurrent_queries_count", na)

		m.QueriesGetInc("Foo")

		require.Equal(t, 1.0, gaugeValue(t, "concurrent_queries_count", na)-before)
	})

	t.Run("nil metrics is a no-op", func(t *testing.T) {
		var m *Metrics
		require.NotPanics(t, func() {
			m.QueriesAggregateInc("Foo")
			m.QueriesGetInc("Foo")
			m.QueriesGetDec("Foo")
			m.QueriesObserveDuration("Foo", 0)
			m.AddUsageDimensions("Foo", "get", "objects", 1)
		})
	})
}

// findMetric returns the sample matching name+labels from the default registry,
// or nil if that series has not been emitted yet (so callers read 0 as baseline).
func findMetric(t *testing.T, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if labelsMatch(m, labels) {
				return m
			}
		}
	}
	return nil
}

func labelsMatch(m *dto.Metric, labels map[string]string) bool {
	if len(m.GetLabel()) != len(labels) {
		return false
	}
	for _, l := range m.GetLabel() {
		if labels[l.GetName()] != l.GetValue() {
			return false
		}
	}
	return true
}

func gaugeValue(t *testing.T, name string, labels map[string]string) float64 {
	return findMetric(t, name, labels).GetGauge().GetValue()
}

func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	return findMetric(t, name, labels).GetCounter().GetValue()
}

func histogramCount(t *testing.T, name string, labels map[string]string) uint64 {
	return findMetric(t, name, labels).GetHistogram().GetSampleCount()
}
