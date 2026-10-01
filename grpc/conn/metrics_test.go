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

package grpcconn

import (
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatheredNames registers a fresh set of conn metrics into reg, gives each a
// value so it is gatherable, and returns the exported metric family names.
func gatheredNames(t *testing.T, reg *prometheus.Registry, r prometheus.Registerer) []string {
	t.Helper()
	m := newConnMetrics(r)
	m.connCreateTotal.Inc()
	m.connReuseTotal.Inc()
	m.connCloseTotal.Inc()
	m.connOpenGauge.Set(1)
	m.connRejectTotal.WithLabelValues("limit").Inc()
	m.connEvictTotal.WithLabelValues("idle").Inc()

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	sort.Strings(names)
	return names
}

// TestConnMetricNames pins the exported names of the connection collectors.
//
// Four of them set Namespace "weaviate" AND a Name already carrying the
// "weaviate_" prefix, so they were exported as weaviate_weaviate_grpc_conn_*
// while their two siblings in the same struct were correct. Nothing gathered
// these collectors, so the defect was invisible to the test suite.
func TestConnMetricNames(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	got := gatheredNames(t, reg, reg)

	assert.Equal(t, []string{
		"weaviate_connection_evictions_total",
		"weaviate_connection_rejected_total",
		"weaviate_grpc_conn_close_total",
		"weaviate_grpc_conn_create_total",
		"weaviate_grpc_conn_open",
		"weaviate_grpc_conn_reuse_total",
	}, got)

	for _, n := range got {
		assert.NotContains(t, n, "weaviate_weaviate", "%s carries a doubled namespace prefix", n)
	}
}

// TestConnMetricNamesWithPrefixedRegisterer covers the replication pool, which
// wraps the same collectors in WrapRegistererWithPrefix("repl_", ...) — see
// adapters/handlers/rest/configure_api.go. The wrapper prepends to the FULL
// name, so a doubled prefix showed up there as repl_weaviate_weaviate_*.
func TestConnMetricNamesWithPrefixedRegisterer(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	got := gatheredNames(t, reg, prometheus.WrapRegistererWithPrefix("repl_", reg))

	assert.Equal(t, []string{
		"repl_weaviate_connection_evictions_total",
		"repl_weaviate_connection_rejected_total",
		"repl_weaviate_grpc_conn_close_total",
		"repl_weaviate_grpc_conn_create_total",
		"repl_weaviate_grpc_conn_open",
		"repl_weaviate_grpc_conn_reuse_total",
	}, got)

	for _, n := range got {
		assert.NotContains(t, n, "weaviate_weaviate", "%s carries a doubled namespace prefix", n)
	}
}
