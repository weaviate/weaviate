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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBatchSizeBytes builds a standalone vec with the production label set. The
// process-global one is registered at package init and shared by every test in
// the tree, so the delete assertions below never run against it. The
// registered-vec pin does write to it, and removes only the series it wrote.
func newBatchSizeBytes() *prometheus.SummaryVec {
	return prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "batch_size_bytes",
		Help: "Size of a raw batch request batch in bytes",
	}, []string{"api", "collection_namespace"})
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
		vec := newBatchSizeBytes()
		observeBothAPIs(vec, "")
		observeBothAPIs(vec, "ns_a")
		observeBothAPIs(vec, "ns_b")
		require.Equal(t, 6, testutil.CollectAndCount(vec))

		pm := &PrometheusMetrics{BatchSizeBytes: vec}
		pm.DeleteNamespace("ns_a")

		assert.Equal(t, 4, testutil.CollectAndCount(vec))
		assert.Equal(t, uint64(1), summaryCount(t, vec, "rest", "ns_b"),
			"the surviving namespace keeps its sample")
		assert.Equal(t, uint64(1), summaryCount(t, vec, "rest", ""),
			"the empty-namespace bucket keeps its sample")
		assert.Equal(t, uint64(0), summaryCount(t, vec, "rest", "ns_a"),
			"the deleted namespace starts from zero if it batches again")
	})

	t.Run("empty namespace is never deleted", func(t *testing.T) {
		vec := newBatchSizeBytes()
		observeBothAPIs(vec, "")
		observeBothAPIs(vec, "ns_a")

		pm := &PrometheusMetrics{BatchSizeBytes: vec}
		pm.DeleteNamespace("")

		assert.Equal(t, 4, testutil.CollectAndCount(vec))
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
