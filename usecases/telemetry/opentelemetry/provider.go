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
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Provider manages the OpenTelemetry tracing provider
type Provider struct {
	config   *Config
	logger   logrus.FieldLogger
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
}

// NewProvider creates a new OpenTelemetry provider
func NewProvider(cfg *Config, logger logrus.FieldLogger) (*Provider, error) {
	if err := cfg.IsValid(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if !cfg.Enabled {
		return &Provider{
			config: cfg,
			logger: logger,
		}, nil
	}

	// Create resource with service information
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"), // TODO: get from build info
			semconv.DeploymentEnvironment(cfg.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create exporter
	exporter, err := createExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	// Create batch span processor
	bsp := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithBatchTimeout(cfg.BatchTimeout),
		sdktrace.WithMaxExportBatchSize(cfg.MaxExportBatchSize),
	)

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRate))),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Create tracer
	tracer := tp.Tracer(cfg.ServiceName)

	return &Provider{
		config:   cfg,
		logger:   logger,
		provider: tp,
		tracer:   tracer,
	}, nil
}

// createExporter creates an OTLP exporter based on the configuration
func createExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	switch cfg.ExporterProtocol {
	case "http":
		return createHTTPExporter(cfg)
	case "grpc":
		return createGRPCExporter(cfg)
	default:
		return nil, ErrExporterNotSupported
	}
}

// createHTTPExporter creates an HTTP OTLP exporter
func createHTTPExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpoint(cfg.ExporterEndpoint),
		otlptracehttp.WithInsecure(), // TODO: make configurable
	)

	return otlptrace.New(context.Background(), client)
}

// createGRPCExporter creates a gRPC OTLP exporter
func createGRPCExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	// Parse the endpoint to extract host:port for gRPC
	endpoint := cfg.ExporterEndpoint
	if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	} else if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // TODO: make configurable
	)

	return otlptrace.New(context.Background(), client)
}

// Tracer returns the OpenTelemetry tracer
func (p *Provider) Tracer() trace.Tracer {
	if p.tracer == nil {
		// return trace.NewNoopTracerProvider().Tracer("noop")
		return noop.NewTracerProvider().Tracer("noop")
	}
	return p.tracer
}

// Shutdown gracefully shuts down the provider
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.provider == nil {
		return nil
	}

	// Set a timeout for shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := p.provider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("failed to shutdown trace provider: %w", err)
	}

	p.logger.Info("OpenTelemetry provider shutdown successfully")
	return nil
}

// IsEnabled returns whether OpenTelemetry is enabled
func (p *Provider) IsEnabled() bool {
	return p.config != nil && p.config.Enabled
}
