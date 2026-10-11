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
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDeletableMetrics builds standalone vecs with the production label sets for
// every metric DeleteNamespace touches. The process-global ones are registered
// at package init and shared by every test in the tree, so the delete assertions
// below never run against them. The registered-vec pin does write to one, and
// removes only the series it wrote.
func newDeletableMetrics() *PrometheusMetrics {
	classShardNamespace := []string{"class_name", "shard_name", "collection_namespace"}
	return &PrometheusMetrics{
		BatchSizeBytes: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_size_bytes",
			Help: "Size of a raw batch request batch in bytes",
		}, []string{"api", "collection_namespace"}),
		ObjectCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "object_count",
		}, classShardNamespace),
		QueriesDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "queries_durations_ms",
		}, []string{"class_name", "query_type", "collection_namespace"}),
		VectorDimensionsSum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_dimensions_sum",
		}, classShardNamespace),
		VectorSegmentsSum: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_segments_sum",
		}, classShardNamespace),
	}
}

// groupedGaugeLabels is the label set of one grouped gauge series: class and
// shard are "n/a", and the namespace identifies the series.
func groupedGaugeLabels(namespace string) prometheus.Labels {
	return prometheus.Labels{
		"class_name":           "n/a",
		"shard_name":           "n/a",
		"collection_namespace": namespace,
	}
}

// gaugeNamespaces lists the namespaces a gauge currently has series for, sorted.
// It reports which series exist, which a value read cannot: reading a label set
// creates the series when it is absent.
func gaugeNamespaces(t *testing.T, vec *prometheus.GaugeVec) []string {
	t.Helper()

	// Collect sends on the channel and returns only once it is done, so the
	// receive has to run beside it rather than after.
	ch := make(chan prometheus.Metric, 64)
	go func() {
		vec.Collect(ch)
		close(ch)
	}()

	var namespaces []string
	for metric := range ch {
		var m dto.Metric
		// A failed Write must not end the goroutine: the producer holds the vec's
		// read lock until the channel is drained.
		if !assert.NoError(t, metric.Write(&m)) {
			continue
		}
		for _, label := range m.GetLabel() {
			if label.GetName() == "collection_namespace" {
				namespaces = append(namespaces, label.GetValue())
			}
		}
	}
	slices.Sort(namespaces)
	return namespaces
}

// observeBothAPIs records one sample per API for the namespace, the shape a
// cluster that batches over REST and gRPC produces.
func observeBothAPIs(vec *prometheus.SummaryVec, namespace string) {
	vec.WithLabelValues("rest", namespace).Observe(1)
	vec.WithLabelValues("grpc", namespace).Observe(1)
}

// summaryCount reads back the _count of one label set. A summary carries no
// single value, so testutil.ToFloat64 does not apply.
func summaryCount(t *testing.T, vec *prometheus.SummaryVec, api, namespace string) uint64 {
	t.Helper()
	obs, err := vec.GetMetricWithLabelValues(api, namespace)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetSummary().GetSampleCount()
}

func TestDeleteNamespace(t *testing.T) {
	t.Run("deleting one namespace removes its two label sets and no other", func(t *testing.T) {
		pm := newDeletableMetrics()
		observeBothAPIs(pm.BatchSizeBytes, "")
		observeBothAPIs(pm.BatchSizeBytes, "ns_a")
		observeBothAPIs(pm.BatchSizeBytes, "ns_b")
		require.Equal(t, 6, testutil.CollectAndCount(pm.BatchSizeBytes))

		pm.DeleteNamespace("ns_a")

		vec := pm.BatchSizeBytes
		assert.Equal(t, 4, testutil.CollectAndCount(vec))
		assert.Equal(t, uint64(1), summaryCount(t, vec, "rest", "ns_b"),
			"the surviving namespace keeps its sample")
		assert.Equal(t, uint64(1), summaryCount(t, vec, "rest", ""),
			"the empty-namespace bucket keeps its sample")
		assert.Equal(t, uint64(0), summaryCount(t, vec, "rest", "ns_a"),
			"the deleted namespace starts from zero if it batches again")
	})

	t.Run("grouped object_count and queries series for the namespace are deleted", func(t *testing.T) {
		pm := newDeletableMetrics()
		for _, ns := range []string{"", "ns_a", "ns_b"} {
			pm.ObjectCount.With(groupedGaugeLabels(ns)).Set(7)
			pm.QueriesDurations.With(prometheus.Labels{
				"class_name": "n/a", "query_type": "get_graphql", "collection_namespace": ns,
			}).Observe(1)
		}

		pm.DeleteNamespace("ns_a")

		assert.Equal(t, []string{"", "ns_b"}, gaugeNamespaces(t, pm.ObjectCount),
			"only the deleted namespace's grouped series goes")
		assert.Equal(t, 2, testutil.CollectAndCount(pm.QueriesDurations))
	})

	t.Run("class-keyed series are left to the class delete", func(t *testing.T) {
		pm := newDeletableMetrics()
		// These are the series an ungrouped cluster publishes for the same
		// namespace. DeleteClass
		// and DeleteShard own these; a namespace-wide sweep here would delete a
		// live class's series the moment any namespace is removed.
		pm.ObjectCount.With(prometheus.Labels{
			"class_name": "ns_a:Docs", "shard_name": "shard1", "collection_namespace": "ns_a",
		}).Set(7)

		pm.DeleteNamespace("ns_a")

		assert.Equal(t, 1, testutil.CollectAndCount(pm.ObjectCount))
	})

	t.Run("existing grouped dimension series are set to zero, not deleted", func(t *testing.T) {
		pm := newDeletableMetrics()
		for _, ns := range []string{"ns_a", "ns_b"} {
			pm.VectorDimensionsSum.With(groupedGaugeLabels(ns)).Set(64)
			pm.VectorSegmentsSum.With(groupedGaugeLabels(ns)).Set(8)
		}

		pm.DeleteNamespace("ns_a")

		assert.Equal(t, []string{"ns_a", "ns_b"}, gaugeNamespaces(t, pm.VectorDimensionsSum),
			"the series is retained for billing")
		assert.Zero(t, testutil.ToFloat64(pm.VectorDimensionsSum.With(groupedGaugeLabels("ns_a"))))
		assert.Zero(t, testutil.ToFloat64(pm.VectorSegmentsSum.With(groupedGaugeLabels("ns_a"))))
		assert.Equal(t, 64.0, testutil.ToFloat64(pm.VectorDimensionsSum.With(groupedGaugeLabels("ns_b"))))
		assert.Equal(t, 8.0, testutil.ToFloat64(pm.VectorSegmentsSum.With(groupedGaugeLabels("ns_b"))))
	})

	t.Run("absent dimension series are not created", func(t *testing.T) {
		// A node with dimension tracking off never starts the observer, so it has
		// no dimension series at all. Zeroing must not mint one there.
		pm := newDeletableMetrics()

		pm.DeleteNamespace("ns_a")

		assert.Zero(t, testutil.CollectAndCount(pm.VectorDimensionsSum))
		assert.Zero(t, testutil.CollectAndCount(pm.VectorSegmentsSum))
	})

	t.Run("empty namespace is never deleted", func(t *testing.T) {
		pm := newDeletableMetrics()
		observeBothAPIs(pm.BatchSizeBytes, "")
		observeBothAPIs(pm.BatchSizeBytes, "ns_a")
		pm.ObjectCount.With(groupedGaugeLabels("")).Set(7)
		pm.VectorDimensionsSum.With(groupedGaugeLabels("")).Set(64)

		pm.DeleteNamespace("")

		assert.Equal(t, 4, testutil.CollectAndCount(pm.BatchSizeBytes))
		assert.Equal(t, 1, testutil.CollectAndCount(pm.ObjectCount))
		assert.Equal(t, 64.0, testutil.ToFloat64(pm.VectorDimensionsSum.With(groupedGaugeLabels(""))))
	})

	t.Run("nil receiver is a no-op", func(t *testing.T) {
		var pm *PrometheusMetrics
		assert.NotPanics(t, func() { pm.DeleteNamespace("ns_a") })
	})
}

func TestNewPrometheusMetrics(t *testing.T) {
	// The REST middleware and the gRPC interceptor pass two label values; a
	// shipped vec declaring one makes WithLabelValues panic at runtime, not at
	// compile time. Every other batch test builds a standalone vec, so this is
	// the only unit assertion on the registered definition.
	t.Run("the registered batch_size_bytes vec carries every label the writers set", func(t *testing.T) {
		vec := GetMetrics().BatchSizeBytes
		require.NotNil(t, vec)
		// This is the file's only write to the process-global vec.
		t.Cleanup(func() {
			vec.DeletePartialMatch(prometheus.Labels{"collection_namespace": "ns_a"})
		})

		for _, api := range []string{"rest", "grpc"} {
			// The write is name-keyed and the read back is positional, the form
			// both writers use. .With panics if a label is renamed; the
			// positional read misses the series if the declared order changes.
			assert.NotPanics(t, func() {
				vec.With(prometheus.Labels{"api": api, "collection_namespace": "ns_a"}).Observe(1)
			}, "api=%s", api)
			assert.Equal(t, uint64(1), summaryCount(t, vec, api, "ns_a"),
				"the sample must land on the series the writers address, api=%s", api)
		}
	})
}
