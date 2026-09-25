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

package lsmkv

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// newTestMetrics builds a Metrics against the process-global vecs with
// grouping set per case. Group is flipped on a value copy, never on the
// shared global, so a parallel reader cannot observe the flip.
func newTestMetrics(t *testing.T, group bool, className, shardName string) *Metrics {
	t.Helper()
	promMetrics := *monitoring.GetMetrics()
	promMetrics.Group = group
	promMetrics.Registerer = monitoring.NoopRegisterer
	m, err := NewMetrics(&promMetrics, className, shardName)
	require.NoError(t, err)
	// NewMetrics rewrites the class to "n/a" in grouped mode, so the series this
	// case writes is keyed on that name rather than on className.
	written := className
	if group {
		written = "n/a"
	}
	t.Cleanup(func() {
		monitoring.GetMetrics().ObjectCount.DeletePartialMatch(prometheus.Labels{"class_name": written})
	})
	return m
}

func objectCountOf(t *testing.T, className, shardName, namespace string) float64 {
	t.Helper()
	g, err := monitoring.GetMetrics().ObjectCount.GetMetricWithLabelValues(className, shardName, namespace)
	require.NoError(t, err)
	return testutil.ToFloat64(g)
}

func TestNewMetrics(t *testing.T) {
	t.Run("qualified class carries its namespace", func(t *testing.T) {
		m := newTestMetrics(t, false, "ns_a:Qualified", "shard1")
		m.ObjectCount(7)

		assert.Equal(t, 7.0, objectCountOf(t, "ns_a:Qualified", "shard1", "ns_a"))
		assert.Equal(t, 0.0, objectCountOf(t, "ns_a:Qualified", "shard1", ""),
			"the namespace must not also land on the empty label")
	})

	t.Run("unqualified class carries empty namespace", func(t *testing.T) {
		m := newTestMetrics(t, false, "Unqualified", "shard1")
		m.ObjectCount(3)

		assert.Equal(t, 3.0, objectCountOf(t, "Unqualified", "shard1", ""))
	})

	t.Run("grouped mode yields one n/a series with empty namespace", func(t *testing.T) {
		// Grouping rewrites the class to "n/a" before the namespace is derived,
		// so two namespaces share the single grouped series rather than each
		// getting a series that overwrites the node total.
		newTestMetrics(t, true, "ns_a:Grouped", "shard1").ObjectCount(5)
		newTestMetrics(t, true, "ns_b:Grouped", "shard2").ObjectCount(9)

		assert.Equal(t, 9.0, objectCountOf(t, "n/a", "n/a", ""),
			"both namespaces write to the one grouped series")
		assert.Equal(t, 0.0, objectCountOf(t, "n/a", "n/a", "ns_a"))
		assert.Equal(t, 0.0, objectCountOf(t, "n/a", "n/a", "ns_b"))
	})
}
