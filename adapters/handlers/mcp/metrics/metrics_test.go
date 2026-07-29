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

package metrics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

func forbiddenErr() error {
	return authzerrors.NewForbidden(&models.Principal{Username: "alice"}, "read", "mcp")
}

func unauthenticatedErr() error {
	return authzerrors.NewUnauthenticated()
}

// metricValue gathers a single-series counter or gauge by name from reg.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1, "metric %q should have exactly one series", name)
		m := mf.GetMetric()[0]
		switch {
		case m.Gauge != nil:
			return m.Gauge.GetValue()
		case m.Counter != nil:
			return m.Counter.GetValue()
		}
		t.Fatalf("metric %q is neither gauge nor counter", name)
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func TestNew_NilRegistererReturnsNil(t *testing.T) {
	require.Nil(t, New(nil, func() bool { return true }))
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil error", nil, StatusSuccess},
		{"write disabled sentinel", ErrWriteDisabled, StatusWriteDisabled},
		{"wrapped write disabled", fmt.Errorf("upsert: %w", ErrWriteDisabled), StatusWriteDisabled},
		{"forbidden", forbiddenErr(), StatusDenied},
		{"wrapped forbidden", fmt.Errorf("authz: %w", forbiddenErr()), StatusDenied},
		{"unauthenticated", unauthenticatedErr(), StatusDenied},
		{"generic error", errors.New("boom"), StatusError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.err))
		})
	}
}

func TestInstrument_RecordsStatusAndDuration(t *testing.T) {
	tests := []struct {
		name       string
		handlerErr error
		wantStatus string
	}{
		{"success", nil, StatusSuccess},
		{"denied", forbiddenErr(), StatusDenied},
		{"write disabled", ErrWriteDisabled, StatusWriteDisabled},
		{"generic error", errors.New("boom"), StatusError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := New(reg, func() bool { return true })
			const tool = "weaviate-test-tool"

			var inflightDuringCall float64
			handler := func(ctx context.Context, req mcplib.CallToolRequest, args struct{}) (struct{}, error) {
				inflightDuringCall = testutil.ToFloat64(m.toolCallsInflight.WithLabelValues(tool))
				return struct{}{}, tt.handlerErr
			}

			wrapped := Instrument(m, tool, handler)
			_, err := wrapped(context.Background(), mcplib.CallToolRequest{}, struct{}{})
			require.Equal(t, tt.handlerErr, err, "Instrument must pass the handler error through unchanged")

			require.Equal(t, 1.0, inflightDuringCall, "inflight should be 1 during the call")
			require.Equal(t, 0.0, testutil.ToFloat64(m.toolCallsInflight.WithLabelValues(tool)),
				"inflight should return to 0 after the call")
			require.Equal(t, 1.0, testutil.ToFloat64(m.toolCalls.WithLabelValues(tool, tt.wantStatus)),
				"tool_calls_total should be incremented for the classified status")
			require.Equal(t, 1, testutil.CollectAndCount(m.toolCallDuration, "weaviate_mcp_tool_call_duration_seconds"),
				"a duration histogram series should exist for the call")
		})
	}
}

func TestInstrument_NilMetricsReturnsHandlerUnchanged(t *testing.T) {
	handler := func(ctx context.Context, req mcplib.CallToolRequest, args struct{}) (string, error) {
		return "ok", nil
	}
	wrapped := Instrument(nil, "tool", handler)
	got, err := wrapped(context.Background(), mcplib.CallToolRequest{}, struct{}{})
	require.NoError(t, err)
	require.Equal(t, "ok", got)
}

func TestObserveAuthFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg, func() bool { return true })

	m.ObserveAuthFailure(AuthReasonForbidden)
	m.ObserveAuthFailure(AuthReasonForbidden)
	m.ObserveAuthFailure(AuthReasonUnauthenticated)
	m.ObserveAuthFailure(AuthReasonInvalidToken)

	require.Equal(t, 2.0, testutil.ToFloat64(m.authFailures.WithLabelValues(AuthReasonForbidden)))
	require.Equal(t, 1.0, testutil.ToFloat64(m.authFailures.WithLabelValues(AuthReasonUnauthenticated)))
	require.Equal(t, 1.0, testutil.ToFloat64(m.authFailures.WithLabelValues(AuthReasonInvalidToken)))
}

func TestObserveListed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg, func() bool { return true })

	m.ObserveListed(true)
	m.ObserveListed(true)
	m.ObserveListed(false)

	require.Equal(t, 2.0, testutil.ToFloat64(m.toolsListed.WithLabelValues("enabled")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.toolsListed.WithLabelValues("disabled")))
}

func TestWriteAccessGauge_ReflectsLiveFlag(t *testing.T) {
	reg := prometheus.NewRegistry()
	writeEnabled := true
	New(reg, func() bool { return writeEnabled })

	require.Equal(t, 1.0, metricValue(t, reg, "weaviate_mcp_write_access_enabled"),
		"gauge should report 1 while the flag is enabled")

	// The GaugeFunc is polled at scrape time, so flipping the flag must be
	// reflected immediately without any explicit metric update call.
	writeEnabled = false
	require.Equal(t, 0.0, metricValue(t, reg, "weaviate_mcp_write_access_enabled"),
		"gauge should report 0 immediately after the flag is disabled")
}

func TestNilReceiver_ObserversAreNoOps(t *testing.T) {
	var m *MCPMetrics
	require.NotPanics(t, func() {
		m.ObserveAuthFailure(AuthReasonForbidden)
		m.ObserveListed(true)
	})
}
