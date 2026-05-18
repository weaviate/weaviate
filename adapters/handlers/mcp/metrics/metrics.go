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
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Auth failure reasons reported via ObserveAuthFailure.
const (
	AuthReasonMissingToken = "missing_token"
	AuthReasonInvalidToken = "invalid_token"
	AuthReasonForbidden    = "forbidden"
)

// Tool call statuses reported on tool_calls_total / tool_call_duration_seconds.
const (
	StatusSuccess       = "success"
	StatusError         = "error"
	StatusDenied        = "denied"
	StatusWriteDisabled = "write_disabled"
)

// ErrWriteDisabled is returned by write tools when the runtime write-access
// flag is disabled. classify() uses errors.Is to label the call as
// status="write_disabled" rather than a generic error.
var ErrWriteDisabled = errors.New("MCP write access is disabled")

// MCPMetrics holds MCP-server-level Prometheus metrics. It is safe to use a
// nil receiver — all observer methods become no-ops.
type MCPMetrics struct {
	toolCalls         *prometheus.CounterVec
	toolCallDuration  *prometheus.HistogramVec
	toolCallsInflight *prometheus.GaugeVec
	authFailures      *prometheus.CounterVec
	toolsListed       *prometheus.CounterVec
	writeAccess       prometheus.Gauge
}

// New constructs an MCPMetrics. If reg is nil, returns nil and the receiver
// methods become no-ops.
func New(reg prometheus.Registerer) *MCPMetrics {
	if reg == nil {
		return nil
	}
	r := promauto.With(reg)
	return &MCPMetrics{
		toolCalls: r.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_mcp_tool_calls_total",
			Help: "Total number of MCP tool calls, labeled by tool name and outcome.",
		}, []string{"tool", "status"}),
		toolCallDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_mcp_tool_call_duration_seconds",
			Help:    "Duration of MCP tool calls in seconds.",
			Buckets: monitoring.LatencyBuckets,
		}, []string{"tool", "status"}),
		toolCallsInflight: r.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_mcp_tool_calls_inflight",
			Help: "Number of MCP tool calls currently being processed.",
		}, []string{"tool"}),
		authFailures: r.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_mcp_auth_failures_total",
			Help: "Total number of MCP authentication/authorization failures, labeled by reason.",
		}, []string{"reason"}),
		toolsListed: r.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_mcp_tools_listed_total",
			Help: "Total number of tools/list requests served, labeled by whether write access was enabled at the time.",
		}, []string{"write_access"}),
		writeAccess: r.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_mcp_write_access_enabled",
			Help: "Current state of the runtime MCP_SERVER_WRITE_ACCESS_ENABLED flag (1 if enabled, 0 otherwise).",
		}),
	}
}

// ObserveAuthFailure records an auth failure with the given reason. Safe on a
// nil receiver.
func (m *MCPMetrics) ObserveAuthFailure(reason string) {
	if m == nil {
		return
	}
	m.authFailures.WithLabelValues(reason).Inc()
}

// ObserveListed records a tools/list invocation and the write-access state at
// the time. Safe on a nil receiver.
func (m *MCPMetrics) ObserveListed(writeAccessEnabled bool) {
	if m == nil {
		return
	}
	label := "disabled"
	if writeAccessEnabled {
		label = "enabled"
	}
	m.toolsListed.WithLabelValues(label).Inc()
}

// SetWriteAccessEnabled updates the write_access_enabled gauge. Safe on a nil
// receiver.
func (m *MCPMetrics) SetWriteAccessEnabled(enabled bool) {
	if m == nil {
		return
	}
	if enabled {
		m.writeAccess.Set(1)
	} else {
		m.writeAccess.Set(0)
	}
}

// Instrument wraps a typed structured tool handler so we can record call
// metrics with accurate status classification. We can't do this from a
// stock mcp-go middleware because mcp.NewStructuredToolHandler converts
// the typed Go error into a (CallToolResult{IsError:true}, nil) result,
// which makes the underlying error invisible at the middleware layer.
//
// Use as: mcp.NewStructuredToolHandler(metrics.Instrument(m, toolName, fn)).
// Safe on a nil receiver — returns fn unchanged.
func Instrument[TArgs any, TResult any](
	m *MCPMetrics,
	toolName string,
	fn func(ctx context.Context, req mcplib.CallToolRequest, args TArgs) (TResult, error),
) func(ctx context.Context, req mcplib.CallToolRequest, args TArgs) (TResult, error) {
	if m == nil {
		return fn
	}
	return func(ctx context.Context, req mcplib.CallToolRequest, args TArgs) (TResult, error) {
		m.toolCallsInflight.WithLabelValues(toolName).Inc()
		defer m.toolCallsInflight.WithLabelValues(toolName).Dec()
		start := time.Now()
		result, err := fn(ctx, req, args)
		status := classify(err)
		m.toolCalls.WithLabelValues(toolName, status).Inc()
		m.toolCallDuration.WithLabelValues(toolName, status).Observe(time.Since(start).Seconds())
		return result, err
	}
}

func classify(err error) string {
	if err == nil {
		return StatusSuccess
	}
	if errors.Is(err, ErrWriteDisabled) {
		return StatusWriteDisabled
	}
	var forbidden authzerrors.Forbidden
	if errors.As(err, &forbidden) {
		return StatusDenied
	}
	var unauth authzerrors.Unauthenticated
	if errors.As(err, &unauth) {
		return StatusDenied
	}
	return StatusError
}
