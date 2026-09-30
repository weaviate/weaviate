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

package metricstest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSampleCountAndSum(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_hist_seconds", Help: "test"}, []string{"a"})
	summary := prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: "test_summary_seconds", Help: "test"}, []string{"a"})
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_plain_gauge", Help: "test"})
	reg.MustRegister(hist, summary, gauge)
	hist.WithLabelValues("x").Observe(1)
	hist.WithLabelValues("x").Observe(2)
	hist.WithLabelValues("y").Observe(3)
	summary.WithLabelValues("x").Observe(5)
	summary.WithLabelValues("x").Observe(7)

	tests := []struct {
		name      string
		metric    string
		labels    prometheus.Labels
		wantCount uint64
		wantSum   float64
		wantErr   bool
	}{
		{name: "histogram series", metric: "test_hist_seconds", labels: prometheus.Labels{"a": "x"}, wantCount: 2, wantSum: 3},
		{name: "other histogram series", metric: "test_hist_seconds", labels: prometheus.Labels{"a": "y"}, wantCount: 1, wantSum: 3},
		{name: "summary series", metric: "test_summary_seconds", labels: prometheus.Labels{"a": "x"}, wantCount: 2, wantSum: 12},
		{name: "unknown labels", metric: "test_hist_seconds", labels: prometheus.Labels{"a": "z"}, wantErr: true},
		{name: "unknown metric", metric: "nope", labels: prometheus.Labels{"a": "x"}, wantErr: true},
		{name: "neither summary nor histogram", metric: "test_plain_gauge", labels: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := SampleCount(reg, tt.metric, tt.labels)
			sum, sumErr := SampleSum(reg, tt.metric, tt.labels)
			if tt.wantErr {
				require.Error(t, err)
				require.Error(t, sumErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, sumErr)
			require.Equal(t, tt.wantCount, count)
			require.InDelta(t, tt.wantSum, sum, 1e-9)
		})
	}
}

func TestGaugeValue(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge", Help: "test"}, []string{"a"})
	scalar := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_scalar_gauge", Help: "test"})
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "test_not_a_gauge", Help: "test"})
	reg.MustRegister(vec, scalar, hist)
	vec.WithLabelValues("x").Set(7)
	scalar.Set(3)

	tests := []struct {
		name    string
		metric  string
		labels  prometheus.Labels
		want    float64
		wantErr bool
	}{
		{name: "labelled gauge", metric: "test_gauge", labels: prometheus.Labels{"a": "x"}, want: 7},
		{name: "scalar gauge", metric: "test_scalar_gauge", labels: nil, want: 3},
		{name: "unknown labels", metric: "test_gauge", labels: prometheus.Labels{"a": "z"}, wantErr: true},
		{name: "unknown metric", metric: "nope", labels: nil, wantErr: true},
		{name: "not a gauge", metric: "test_not_a_gauge", labels: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GaugeValue(reg, tt.metric, tt.labels)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
