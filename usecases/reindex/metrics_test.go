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

package reindex

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateMetricsRefused(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	metrics := NewGateMetrics(registry, nil)

	metrics.Refused(GateSubmit, VerdictBackupBusy)
	metrics.Refused(GateSubmit, VerdictBackupBusy)
	metrics.Refused(GateSubmit, VerdictUnreachable)
	metrics.Refused(GateBackup, VerdictHoldSubmit)

	assert.Equal(t, 2.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateSubmit, VerdictBackupBusy)))
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateSubmit, VerdictUnreachable)))
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateBackup, VerdictHoldSubmit)))
}

// A nil handle is what a fixture that wired no metrics has, and a gate must not
// depend on having been wired to refuse.
func TestGateMetricsRefusedWithoutARegistry(t *testing.T) {
	var metrics *GateMetrics
	assert.NotPanics(t, func() { metrics.Refused(GateSubmit, VerdictBackupBusy) })
}

// The gauges have to read on scrape, not on registration, or a hold raised
// after startup is invisible for exactly as long as it is open.
func TestGateMetricsOpenHoldsReadOnScrape(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	open := 0
	NewGateMetrics(registry, map[string]func() int{"submit": func() int { return open }})

	require.Equal(t, 0.0, gaugeValue(t, registry))
	open = 3
	assert.Equal(t, 3.0, gaugeValue(t, registry))
}

// TestGateMetricsLabelSetsAreBounded is the cardinality guard. Every series the
// gates can produce is one pair drawn from two closed vocabularies, so the
// worst case is their product and does not grow with the data — a collection or
// a shard in a label would give a multi-tenant cluster one series per tenant.
func TestGateMetricsLabelSetsAreBounded(t *testing.T) {
	gates := []string{GateSubmit, GateBackup, GateRestore, GateOverlap}
	verdicts := []string{
		VerdictBackupBusy, VerdictRestoreBusy, VerdictUnreachable, VerdictLiveTask,
		VerdictHoldSubmit, VerdictHoldCleanup, VerdictHoldUnknown,
		VerdictOverlap, VerdictOverlapUnsure,
	}

	registry := prometheus.NewPedanticRegistry()
	metrics := NewGateMetrics(registry, nil)
	for _, gate := range gates {
		for _, verdict := range verdicts {
			metrics.Refused(gate, verdict)
		}
	}

	assert.Equal(t, len(gates)*len(verdicts),
		testutil.CollectAndCount(metrics.refusals, "weaviate_reindex_gate_refusals_total"))
}

func gaugeValue(t *testing.T, registry *prometheus.Registry) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].GetMetric(), 1)
	return families[0].GetMetric()[0].GetGauge().GetValue()
}
