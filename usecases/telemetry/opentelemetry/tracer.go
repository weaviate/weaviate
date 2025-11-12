//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package opentelemetry

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Global provider instance
var globalProvider *Provider

// Init initializes the global OpenTelemetry provider
func Init(logger logrus.FieldLogger) error {
	cfg := FromEnvironment()

	provider, err := NewProvider(cfg, logger)
	if err != nil {
		return err
	}

	globalProvider = provider

	if provider.IsEnabled() {
		logger.WithFields(logrus.Fields{
			"service_name":  cfg.ServiceName,
			"environment":   cfg.Environment,
			"endpoint":      cfg.ExporterEndpoint,
			"protocol":      cfg.ExporterProtocol,
			"sampling_rate": cfg.SamplingRate,
		}).Info("OpenTelemetry tracing initialized")
	} else {
		logger.Debug("OpenTelemetry tracing disabled")
	}

	return nil
}

// Shutdown gracefully shuts down the global provider
func Shutdown(ctx context.Context) error {
	if globalProvider == nil {
		return nil
	}

	return globalProvider.Shutdown(ctx)
}

// GetTracer returns the global tracer
func GetTracer() trace.Tracer {
	if globalProvider == nil {
		return noop.NewTracerProvider().Tracer("noop")
	}
	return globalProvider.Tracer()
}

// IsEnabled returns whether OpenTelemetry is enabled globally
func IsEnabled() bool {
	return globalProvider != nil && globalProvider.IsEnabled()
}

// StartSpan starts a new span with the given name and options
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := GetTracer()
	return tracer.Start(ctx, name, opts...)
}

// StartSpanWithAttributes starts a new span with the given name and attributes
func StartSpanWithAttributes(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
	}
	return StartSpan(ctx, name, opts...)
}

// SpanFromContext returns the current span from the context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// TraceIDFromContext returns the trace ID from the context
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// SpanIDFromContext returns the span ID from the context
func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

// AddEvent adds an event to the current span
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(name, trace.WithAttributes(attrs...))
}

// SetAttributes sets attributes on the current span
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attrs...)
}

// RecordError records an error on the current span
func RecordError(ctx context.Context, err error, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err, trace.WithAttributes(attrs...))
}

// SetStatus sets the status on the current span
func SetStatus(ctx context.Context, code codes.Code, description string) {
	span := trace.SpanFromContext(ctx)
	span.SetStatus(code, description)
}

// GetGlobalTracerProvider returns the global tracer provider
func GetGlobalTracerProvider() trace.TracerProvider {
	return otel.GetTracerProvider()
}
