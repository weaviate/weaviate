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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrometheusMetricsNilReceiver pins the `if pm == nil { return nil }` guards:
// on a nil receiver these must be a safe no-op (a `pm != nil` mutant would fall
// through and panic).
func TestPrometheusMetricsNilReceiver(t *testing.T) {
	var pm *PrometheusMetrics
	assert.NoError(t, pm.DeleteShard("MyClass", "shard1"))
	assert.NoError(t, pm.DeleteClass("MyClass"))
}

func TestEnsureRegisteredMetric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	opts := prometheus.CounterOpts{Name: "mutest_demo_total", Help: "demo"}

	first := prometheus.NewCounter(opts)
	got, existed, err := EnsureRegisteredMetric(reg, first)
	require.NoError(t, err)
	assert.False(t, existed)
	assert.Equal(t, first, got)

	// Registering an identical metric hits the already-registered error path
	// (`if err := reg.Register(metric); err != nil`): it must return the
	// EXISTING collector and report existed=true.
	second := prometheus.NewCounter(opts)
	got, existed, err = EnsureRegisteredMetric(reg, second)
	require.NoError(t, err)
	assert.True(t, existed)
	assert.Equal(t, first, got)
}
