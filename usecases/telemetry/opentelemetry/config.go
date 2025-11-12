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
}

// DefaultConfig returns the default OpenTelemetry configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:            false,
		ServiceName:        "weaviate",
		Environment:        "development",
		ExporterEndpoint:   "localhost:4317",
		ExporterProtocol:   "grpc",
		SamplingRate:       0.1, // 10% sampling by default
		BatchTimeout:       5 * time.Second,
		MaxExportBatchSize: 512,
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

	return cfg
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
