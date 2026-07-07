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
// (class_name, query_type) / (query_type, operation, class_name) mapping, so a
// wrong label order here would flip these assertions.
func TestMetricsLabels(t *testing.T) {
	newMetrics := func(group bool) (*Metrics, *prometheus.Registry) {
		reg := prometheus.NewRegistry()
		queriesCount := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "concurrent_queries_count",
		}, []string{"class_name", "query_type"})
		queriesDurations := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "queries_durations_ms",
		}, []string{"class_name", "query_type"})
		dimensions := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "query_dimensions_total",
		}, []string{"query_type", "operation", "class_name"})
		dimensionsCombined := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "query_dimensions_combined_total",
		})
		reg.MustRegister(queriesCount, queriesDurations, dimensions, dimensionsCombined)

		m := NewMetrics(&monitoring.PrometheusMetrics{
			QueriesCount:            queriesCount,
			QueriesDurations:        queriesDurations,
			QueryDimensions:         dimensions,
			QueryDimensionsCombined: dimensionsCombined,
			Group:                   group,
		})
		return m, reg
	}

	t.Run("queries count aggregate and get use distinct query_type", func(t *testing.T) {
		m, reg := newMetrics(false)

		m.QueriesAggregateInc("Foo")
		m.QueriesGetInc("Foo")
		m.QueriesGetInc("Foo")
		m.QueriesGetDec("Foo")

		require.Equal(t, 1.0, gaugeValue(t, reg, "concurrent_queries_count", map[string]string{
			"class_name": "Foo", "query_type": "aggregate",
		}))
		require.Equal(t, 1.0, gaugeValue(t, reg, "concurrent_queries_count", map[string]string{
			"class_name": "Foo", "query_type": "get_graphql",
		}))
	})

	t.Run("observe duration records under get_graphql", func(t *testing.T) {
		m, reg := newMetrics(false)

		m.QueriesObserveDuration("Foo", 0)

		require.Equal(t, uint64(1), histogramCount(t, reg, "queries_durations_ms", map[string]string{
			"class_name": "Foo", "query_type": "get_graphql",
		}))
	})

	t.Run("usage dimensions keep query_type/operation/class_name order", func(t *testing.T) {
		m, reg := newMetrics(false)

		m.AddUsageDimensions("Foo", "get", "objects", 5)

		require.Equal(t, 5.0, counterValue(t, reg, "query_dimensions_total", map[string]string{
			"query_type": "get", "operation": "objects", "class_name": "Foo",
		}))
		require.Equal(t, 5.0, counterValue(t, reg, "query_dimensions_combined_total", nil))
	})

	t.Run("group mode collapses class_name to n/a", func(t *testing.T) {
		m, reg := newMetrics(true)

		m.QueriesGetInc("Foo")

		require.Equal(t, 1.0, gaugeValue(t, reg, "concurrent_queries_count", map[string]string{
			"class_name": "n/a", "query_type": "get_graphql",
		}))
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

func findMetric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
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
	require.Failf(t, "metric not found", "%s with labels %v", name, labels)
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

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	return findMetric(t, reg, name, labels).GetGauge().GetValue()
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	return findMetric(t, reg, name, labels).GetCounter().GetValue()
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	return findMetric(t, reg, name, labels).GetHistogram().GetSampleCount()
}
