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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/config"
)

// Config holds the OpenTelemetry configuration
type Config struct {
	Enabled            bool
	ServiceName        string
	Environment        string
	ExporterEndpoint   string
	ExporterProtocol   string // "http" or "grpc"
	SamplingRate       float64
	BatchTimeout       time.Duration
	MaxExportBatchSize int
	// Headers are sent with every OTLP export request, e.g. the auth token a
	// managed backend like dash0 requires (Authorization=Bearer …).
	Headers map[string]string
	// Insecure disables transport TLS for the OTLP exporter. It defaults to true
	// to preserve the historical localhost-collector behaviour; set it false for
	// TLS-only backends.
	Insecure bool
}

// DefaultConfig returns the default OpenTelemetry configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:            false,
		ServiceName:        "weaviate",
		Environment:        "development",
		ExporterEndpoint:   "localhost:4317",
		ExporterProtocol:   "grpc",
		SamplingRate:       0.01, // 1% sampling by default
		BatchTimeout:       5 * time.Second,
		MaxExportBatchSize: 512,
		Insecure:           true,
	}
}

// FromEnvironment creates a Config from environment variables
func FromEnvironment() *Config {
	cfg := DefaultConfig()

	// Basic configuration
	cfg.Enabled = config.Enabled(os.Getenv("EXPERIMENTAL_OTEL_ENABLED"))
	if !cfg.Enabled {
		return cfg
	}

	// Service configuration
	if serviceName := os.Getenv("EXPERIMENTAL_OTEL_SERVICE_NAME"); serviceName != "" {
		cfg.ServiceName = serviceName
	}

	if environment := os.Getenv("EXPERIMENTAL_OTEL_ENVIRONMENT"); environment != "" {
		cfg.Environment = environment
	}

	// Exporter configuration
	if endpoint := os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		cfg.ExporterEndpoint = endpoint
	}

	if protocol := os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_PROTOCOL"); protocol != "" {
		cfg.ExporterProtocol = protocol
	}

	// Ensure endpoint format matches protocol
	if cfg.ExporterProtocol == "grpc" && (strings.HasPrefix(cfg.ExporterEndpoint, "http://") || strings.HasPrefix(cfg.ExporterEndpoint, "https://")) {
		// Remove http/https prefix for gRPC
		if strings.HasPrefix(cfg.ExporterEndpoint, "http://") {
			cfg.ExporterEndpoint = strings.TrimPrefix(cfg.ExporterEndpoint, "http://")
		} else if strings.HasPrefix(cfg.ExporterEndpoint, "https://") {
			cfg.ExporterEndpoint = strings.TrimPrefix(cfg.ExporterEndpoint, "https://")
		}
	} else if cfg.ExporterProtocol == "http" && !strings.HasPrefix(cfg.ExporterEndpoint, "http://") && !strings.HasPrefix(cfg.ExporterEndpoint, "https://") {
		// Add http prefix for HTTP if not present
		cfg.ExporterEndpoint = "http://" + cfg.ExporterEndpoint
	}

	// Sampling configuration
	if samplerArg := os.Getenv("EXPERIMENTAL_OTEL_TRACES_SAMPLER_ARG"); samplerArg != "" {
		if rate, err := strconv.ParseFloat(samplerArg, 64); err == nil && rate >= 0.0 && rate <= 1.0 {
			cfg.SamplingRate = rate
		}
	}

	// Batch configuration
	if batchTimeout := os.Getenv("EXPERIMENTAL_OTEL_BSP_EXPORT_TIMEOUT"); batchTimeout != "" {
		if timeout, err := time.ParseDuration(batchTimeout); err == nil {
			cfg.BatchTimeout = timeout
		}
	}

	if batchSize := os.Getenv("EXPERIMENTAL_OTEL_BSP_MAX_EXPORT_BATCH_SIZE"); batchSize != "" {
		if size, err := strconv.Atoi(batchSize); err == nil && size > 0 {
			cfg.MaxExportBatchSize = size
		}
	}

	// Exporter transport: TLS + headers for managed OTLP backends (e.g. dash0).
	if insecure := os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_INSECURE"); insecure != "" {
		cfg.Insecure = config.Enabled(insecure)
	}

	if headers := parseHeaders(os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_HEADERS")); len(headers) > 0 {
		cfg.Headers = headers
	}

	return cfg
}

// parseHeaders parses "k=v,k=v" OTLP export headers, e.g.
// "Authorization=Bearer abc,Dash0-Dataset=prod". Entries without "=" or with an
// empty key are skipped; only the first "=" splits, so values may contain "=".
func parseHeaders(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	headers := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			continue
		}
		headers[k] = strings.TrimSpace(v)
	}
	return headers
}

// IsValid checks if the configuration is valid
func (c *Config) IsValid() error {
	if !c.Enabled {
		return nil
	}

	if c.ServiceName == "" {
		return ErrEmptyServiceName
	}

	if c.ExporterEndpoint == "" {
		return ErrEmptyExporterEndpoint
	}

	if c.SamplingRate < 0.0 || c.SamplingRate > 1.0 {
		return ErrInvalidSamplingRate
	}

	if c.BatchTimeout <= 0 {
		return ErrInvalidBatchTimeout
	}

	if c.MaxExportBatchSize <= 0 {
		return ErrInvalidBatchSize
	}

	return nil
}
