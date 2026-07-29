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

package opentelemetry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/build"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/credentials"
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
			semconv.ServiceVersion(build.Version),
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

	// Set global text map propagator for W3C TraceContext
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

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
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.ExporterEndpoint),
	}

	// Configure TLS if certificates are provided
	if cfg.TLSConfig != nil {
		tlsConfig, err := createTLSConfig(cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts = append(opts, otlptracehttp.WithTLSClientConfig(tlsConfig))
	} else if cfg.Insecure {
		// Only use insecure mode if explicitly configured and no TLS config is provided
		opts = append(opts, otlptracehttp.WithInsecure())
	} else {
		return nil, errors.New("http exporter: no TLS or insecure mode configured")
	}

	client := otlptracehttp.NewClient(opts...)
	return otlptrace.New(context.Background(), client)
}

// createGRPCExporter creates a gRPC OTLP exporter
func createGRPCExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.ExporterEndpoint),
	}

	// Configure TLS if certificates are provided
	if cfg.TLSConfig != nil {
		tlsConfig, err := createTLSConfig(cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		// For gRPC, we need to use credentials.TransportCredentials
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, otlptracegrpc.WithTLSCredentials(creds))
	} else if cfg.Insecure {
		// Only use insecure mode if explicitly configured and no TLS config is provided
		opts = append(opts, otlptracegrpc.WithInsecure())
	} else {
		return nil, errors.New("grpc exporter: no TLS or insecure mode configured")
	}

	client := otlptracegrpc.NewClient(opts...)
	return otlptrace.New(context.Background(), client)
}

// createTLSConfig creates a TLS configuration from the provided TLS config
func createTLSConfig(tlsCfg *TLSConfig) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	// Load CA certificate
	if tlsCfg.CAFile != "" {
		caCert, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		config.RootCAs = caCertPool
	}

	// Load client certificate and key if provided (for mutual TLS)
	if tlsCfg.CertFile != "" && tlsCfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	return config, nil
}

// Tracer returns the OpenTelemetry tracer
func (p *Provider) Tracer() trace.Tracer {
	if p.tracer == nil {
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
