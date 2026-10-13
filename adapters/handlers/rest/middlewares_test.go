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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestConsistencyLevelMetric(t *testing.T) {
	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)
	api := operations.NewWeaviateAPI(spec)
	api.Init()
	ctx := middleware.NewRoutableContext(spec, api, middleware.DefaultRouter(spec, api))

	const id = "8c29da7a-600a-43dc-85fb-83ab2b08c294"
	cases := []struct {
		name      string
		method    string
		path      string
		operation string // empty when nothing may be counted
		level     string
	}{
		{"get with ALL", http.MethodGet, "/v1/objects/C/" + id + "?consistency_level=ALL", "read", "ALL"},
		{"head with ONE", http.MethodHead, "/v1/objects/C/" + id + "?consistency_level=ONE", "read", "ONE"},
		{"create with ALL", http.MethodPost, "/v1/objects?consistency_level=ALL", "write", "ALL"},
		{"deprecated delete with QUORUM", http.MethodDelete, "/v1/objects/" + id + "?consistency_level=QUORUM", "write", "QUORUM"},
		{"reference add with ALL", http.MethodPost, "/v1/objects/C/" + id + "/references/p?consistency_level=ALL", "write", "ALL"},
		{"batch objects without level", http.MethodPost, "/v1/batch/objects", "write", "UNSET"},
		{"batch objects with empty level", http.MethodPost, "/v1/batch/objects?consistency_level=", "write", "UNSET"},
		{"batch delete with ALL", http.MethodDelete, "/v1/batch/objects?consistency_level=ALL", "write", "ALL"},
		{"batch references with ALL", http.MethodPost, "/v1/batch/references?consistency_level=ALL", "write", "ALL"},
		{"invalid level", http.MethodGet, "/v1/objects/C/" + id + "?consistency_level=bogus", "", ""},
		{"route without the parameter", http.MethodGet, "/v1/objects?consistency_level=ALL", "", ""},
		{"unrelated route", http.MethodGet, "/v1/schema", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.path, nil)
			require.NoError(t, err)
			_, routed, ok := ctx.RouteInfo(req)
			require.True(t, ok, "route must match")

			totalBefore := consistencyLevelRequestsTotal(t)
			var labelBefore float64
			if tc.operation != "" {
				labelBefore = consistencyLevelRequests(t, tc.operation, tc.level)
			}

			called := false
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
			addConsistencyLevelMetric(next).ServeHTTP(httptest.NewRecorder(), routed)
			require.True(t, called)

			if tc.operation == "" {
				assert.Equal(t, totalBefore, consistencyLevelRequestsTotal(t))
				return
			}
			assert.Equal(t, labelBefore+1, consistencyLevelRequests(t, tc.operation, tc.level))
			assert.Equal(t, totalBefore+1, consistencyLevelRequestsTotal(t))
		})
	}
}

func consistencyLevelRequests(t *testing.T, operation, level string) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, monitoring.GetMetrics().ConsistencyLevelRequests.WithLabelValues(operation, level).Write(&m))
	return m.GetCounter().GetValue()
}

func consistencyLevelRequestsTotal(t *testing.T) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	monitoring.GetMetrics().ConsistencyLevelRequests.Collect(ch)
	close(ch)
	var total float64
	for c := range ch {
		var m dto.Metric
		require.NoError(t, c.Write(&m))
		total += m.GetCounter().GetValue()
	}
	return total
}
