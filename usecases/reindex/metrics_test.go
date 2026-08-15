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
	metrics := NewGateMetrics(registry, nil, nil)

	metrics.Refused(GateSubmit, VerdictBackupBusy)
	metrics.Refused(GateSubmit, VerdictBackupBusy)
	metrics.Refused(GateSubmit, VerdictUnreachable)
	metrics.Refused(GateBackup, VerdictHoldSubmit)

	assert.Equal(t, 2.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateSubmit, VerdictBackupBusy)))
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateSubmit, VerdictUnreachable)))
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.refusals.WithLabelValues(GateBackup, VerdictHoldSubmit)))
}

// A gate must not depend on having been wired to metrics in order to refuse.
func TestGateMetricsRefusedWithoutARegistry(t *testing.T) {
	var metrics *GateMetrics
	assert.NotPanics(t, func() { metrics.Refused(GateSubmit, VerdictBackupBusy) })
}

// Read on scrape, not on registration, or a hold raised after startup is
// invisible for exactly as long as it is open.
func TestGateMetricsOpenHoldsReadOnScrape(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	open := 0
	NewGateMetrics(registry, map[string]func() int{"submit": func() int { return open }}, nil)

	require.Equal(t, 0.0, gaugeValue(t, registry))
	open = 3
	assert.Equal(t, 3.0, gaugeValue(t, registry))
}

// The cardinality guard: every series is one pair from two closed vocabularies,
// so the worst case is their product and does not grow with the data.
func TestGateMetricsLabelSetsAreBounded(t *testing.T) {
	gates := []string{GateSubmit, GateBackup, GateRestore, GateOverlap}
	verdicts := []string{
		VerdictBackupBusy, VerdictRestoreBusy, VerdictUnreachable, VerdictLiveTask,
		VerdictHoldSubmit, VerdictHoldCleanup, VerdictHoldUnknown,
		VerdictOverlap, VerdictOverlapUnsure,
	}

	registry := prometheus.NewPedanticRegistry()
	metrics := NewGateMetrics(registry, nil, nil)
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
	for _, family := range families {
		if family.GetName() != "weaviate_reindex_open_holds" {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatal("the open-holds gauge was never registered")
	return 0
}

// TestGateMetricsExposeEverySeriesFromTheStart is the alerting contract. A
// CounterVec with no children emits nothing at all, and "no data" is a
// different alerting state from zero — an operator watching a refusal rate
// would see neither until the first refusal ever happened.
func TestGateMetricsExposeEverySeriesFromTheStart(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	NewGateMetrics(registry, map[string]func() int{"submit": func() int { return 0 }},
		[]string{"cancelled", "failed"})

	families, err := registry.Gather()
	require.NoError(t, err)

	names := map[string]int{}
	for _, family := range families {
		names[family.GetName()] = len(family.GetMetric())
	}

	assert.Equal(t, 13, names["weaviate_reindex_gate_refusals_total"],
		"every gate/verdict pair production can emit must exist at zero")
	assert.Equal(t, 2, names["weaviate_reindex_submit_rollbacks_total"],
		"every rollback outcome must exist at zero")
	assert.Equal(t, 1, names["weaviate_reindex_open_holds"])
}
