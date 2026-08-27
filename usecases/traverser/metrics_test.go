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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// newQueryMetrics builds a Metrics over a standalone histogram vec, so a
// subtest's samples cannot reach the process-global one every other test in
// the tree shares. The registered-vec pin does write to it, and removes only
// the series it wrote.
func newQueryMetrics(group bool) (*Metrics, *prometheus.HistogramVec) {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "queries_durations_ms",
		Help: "Duration of queries in milliseconds",
	}, []string{"class_name", "query_type", "collection_namespace"})
	return &Metrics{queriesDurations: vec, groupClasses: group}, vec
}

// observedCount reads back how many samples landed on one label set.
func observedCount(t *testing.T, vec *prometheus.HistogramVec, className, namespace string) uint64 {
	t.Helper()
	obs, err := vec.GetMetricWithLabelValues(className, "get_graphql", namespace)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount()
}

func TestQueriesObserveDuration(t *testing.T) {
	startMs := time.Now().UnixMilli()

	t.Run("qualified class carries its namespace", func(t *testing.T) {
		m, vec := newQueryMetrics(false)

		m.QueriesObserveDuration("ns_a:Docs", startMs)

		assert.Equal(t, uint64(1), observedCount(t, vec, "ns_a:Docs", "ns_a"))
		assert.Equal(t, uint64(0), observedCount(t, vec, "ns_a:Docs", ""),
			"the namespace must not also land on the empty label")
	})

	t.Run("unqualified class carries empty namespace", func(t *testing.T) {
		m, vec := newQueryMetrics(false)

		m.QueriesObserveDuration("Docs", startMs)

		assert.Equal(t, uint64(1), observedCount(t, vec, "Docs", ""))
	})

	t.Run("grouped mode yields one n/a series with empty namespace", func(t *testing.T) {
		m, vec := newQueryMetrics(true)

		m.QueriesObserveDuration("ns_a:Docs", startMs)
		m.QueriesObserveDuration("ns_b:Docs", startMs)

		assert.Equal(t, uint64(2), observedCount(t, vec, "n/a", ""),
			"both namespaces collapse onto the single grouped series")
	})

	t.Run("nil receiver is a no-op", func(t *testing.T) {
		var m *Metrics
		assert.NotPanics(t, func() { m.QueriesObserveDuration("ns_a:Docs", startMs) })
	})

	// The subtests above write to a standalone vec. A shipped vec whose label
	// set no longer matches the writer makes .With panic at runtime, not at
	// compile time, so this one drives the registered vec.
	t.Run("the registered vec carries every label the writer sets", func(t *testing.T) {
		vec := monitoring.GetMetrics().QueriesDurations
		m := NewMetrics(monitoring.GetMetrics())
		require.NotNil(t, m)
		// This is the file's only write to the process-global vec.
		t.Cleanup(func() {
			vec.DeletePartialMatch(prometheus.Labels{"collection_namespace": "ns_a"})
		})

		// The writer is name-keyed and this read back is positional, so a
		// renamed label panics and a reordered label set misses the series.
		assert.NotPanics(t, func() { m.QueriesObserveDuration("ns_a:Docs", startMs) })
		assert.Equal(t, uint64(1), observedCount(t, vec, "ns_a:Docs", "ns_a"),
			"the sample must land on the series the writer addresses")
	})
}
