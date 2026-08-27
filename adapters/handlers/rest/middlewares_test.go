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

package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func Test_staticRoute(t *testing.T) {
	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)

	api := operations.NewWeaviateAPI(spec)
	api.Init()

	router := middleware.DefaultRouter(spec, api)
	ctx := middleware.NewRoutableContext(spec, api, router)

	cases := []struct {
		name     string
		req      *http.Request
		expected string
	}{
		{
			name:     "unmatched route",
			req:      newRequest(t, "/foo"), // un-matched route
			expected: "/foo",
		},
		{
			name:     "matched route",
			req:      newRequest(t, "/v1/schema"), // matched route
			expected: "/v1/schema",
		},
		{
			name:     "matched route with dynamic path",
			req:      newRequest(t, "/v1/schema/Movies/"), // matched route.
			expected: "/v1/schema/{className}",            // yay!
		},
		{
			name:     "matched route with dynamic path 2",
			req:      newRequest(t, "/v1/schema/Movies/shards"), // matched route.
			expected: "/v1/schema/{className}/shards",           // yay!
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := staticRoute(ctx)(tc.req)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func newRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	r, err := http.NewRequest("GET", path, nil)
	require.NoError(t, err)
	return r
}

// newBatchMetrics builds the two vecs makeAddMonitoring writes, standalone so
// a subtest cannot see another's samples.
func newBatchMetrics(group bool) *monitoring.PrometheusMetrics {
	return &monitoring.PrometheusMetrics{
		Group: group,
		BatchTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "batch_durations_ms",
		}, []string{"operation", "class_name", "shard_name"}),
		BatchSizeBytes: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_size_bytes",
		}, []string{"api", "collection_namespace"}),
	}
}

func batchSizeSamples(t *testing.T, metrics *monitoring.PrometheusMetrics, namespace string) (count uint64, sum float64) {
	t.Helper()
	obs, err := metrics.BatchSizeBytes.GetMetricWithLabelValues("rest", namespace)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetSummary().GetSampleCount(), m.GetSummary().GetSampleSum()
}

// drainingHandler reads the whole body, as go-swagger does before it reaches
// a batch handler, and records namespace in the monitoring slot when it is
// not empty.
func drainingHandler(t *testing.T, namespace string) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if namespace != "" {
			restCtx.SetBatchNamespace(r.Context(), namespace)
		}
	})
}

// instrumented wraps next the way makeSetupGlobalMiddleware does: the byte
// counter is installed outside the monitoring middleware and removed only
// after it returns.
func instrumented(next http.Handler) http.Handler {
	return monitoring.InstrumentHTTP(next,
		func(r *http.Request) (*http.Request, string) { return r, "/v1/batch/objects" },
		prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "inflight"}, []string{"method", "route"}),
		prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "duration"}, []string{"method", "route", "status"}),
		prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "req_size"}, []string{"method", "route"}),
		prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "resp_size"}, []string{"method", "route"}),
	)
}

func TestMakeAddMonitoring(t *testing.T) {
	const body = `{"objects":[]}`

	t.Run("content length is observed with the handler's namespace", func(t *testing.T) {
		metrics := newBatchMetrics(false)
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))

		makeAddMonitoring(metrics)(drainingHandler(t, "ns_a")).ServeHTTP(httptest.NewRecorder(), r)

		count, sum := batchSizeSamples(t, metrics, "ns_a")
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(len(body)), sum)

		count, _ = batchSizeSamples(t, metrics, "")
		assert.Zero(t, count, "the namespaced sample must not also land on the empty label")
	})

	t.Run("chunked request observes the instrumented byte count", func(t *testing.T) {
		metrics := newBatchMetrics(false)
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))
		r.ContentLength = -1

		instrumented(makeAddMonitoring(metrics)(drainingHandler(t, "ns_a"))).
			ServeHTTP(httptest.NewRecorder(), r)

		count, sum := batchSizeSamples(t, metrics, "ns_a")
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(len(body)), sum, "a chunked body is sized by the counter, never by -1")
	})

	t.Run("chunked request without instrumentation observes nothing", func(t *testing.T) {
		metrics := newBatchMetrics(false)
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))
		r.ContentLength = -1

		makeAddMonitoring(metrics)(drainingHandler(t, "ns_a")).ServeHTTP(httptest.NewRecorder(), r)

		count, _ := batchSizeSamples(t, metrics, "ns_a")
		assert.Zero(t, count, "skipping beats recording -1")
	})

	t.Run("handler that sets no namespace yields empty label", func(t *testing.T) {
		metrics := newBatchMetrics(false)
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))

		makeAddMonitoring(metrics)(drainingHandler(t, "")).ServeHTTP(httptest.NewRecorder(), r)

		count, _ := batchSizeSamples(t, metrics, "")
		assert.Equal(t, uint64(1), count)
	})

	t.Run("grouped mode yields empty label", func(t *testing.T) {
		metrics := newBatchMetrics(true)
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))

		makeAddMonitoring(metrics)(drainingHandler(t, "ns_a")).ServeHTTP(httptest.NewRecorder(), r)

		count, _ := batchSizeSamples(t, metrics, "")
		assert.Equal(t, uint64(1), count)
		count, _ = batchSizeSamples(t, metrics, "ns_a")
		assert.Zero(t, count)
	})

	t.Run("non-batch routes observe nothing", func(t *testing.T) {
		tests := []struct {
			name   string
			method string
			path   string
		}{
			{name: "GET on the batch route", method: http.MethodGet, path: "/v1/batch/objects"},
			{name: "POST on another route", method: http.MethodPost, path: "/v1/objects"},
			{name: "POST on batch references", method: http.MethodPost, path: "/v1/batch/references"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				metrics := newBatchMetrics(false)
				r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(body))

				makeAddMonitoring(metrics)(drainingHandler(t, "ns_a")).ServeHTTP(httptest.NewRecorder(), r)

				count, _ := batchSizeSamples(t, metrics, "ns_a")
				assert.Zero(t, count)
				count, _ = batchSizeSamples(t, metrics, "")
				assert.Zero(t, count)
			})
		}
	})
}
