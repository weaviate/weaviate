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

	entconfig "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// Config holds the OpenTelemetry configuration
type Config struct {
	Enabled            *runtime.DynamicValue[bool]
	ServiceName        *runtime.DynamicValue[string]
	Environment        *runtime.DynamicValue[string]
	ExporterEndpoint   *runtime.DynamicValue[string]
	ExporterProtocol   *runtime.DynamicValue[string] // "http" or "grpc"
	SamplingRate       *runtime.DynamicValue[float64]
	BatchTimeout       *runtime.DynamicValue[time.Duration]
	MaxExportBatchSize *runtime.DynamicValue[int]
}

// DefaultConfig returns the default OpenTelemetry configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:            runtime.NewDynamicValue(false),
		ServiceName:        runtime.NewDynamicValue("weaviate"),
		Environment:        runtime.NewDynamicValue("development"),
		ExporterEndpoint:   runtime.NewDynamicValue("localhost:4317"),
		ExporterProtocol:   runtime.NewDynamicValue("grpc"),
		SamplingRate:       runtime.NewDynamicValue(0.01), // 1% sampling by default
		BatchTimeout:       runtime.NewDynamicValue(5 * time.Second),
		MaxExportBatchSize: runtime.NewDynamicValue(512),
	}
}

// FromEnvironment creates a Config from environment variables
func FromEnvironment() *Config {
	cfg := DefaultConfig()

	// Basic configuration
	enabled := entconfig.Enabled(os.Getenv("EXPERIMENTAL_OTEL_ENABLED"))
	cfg.Enabled.SetValue(enabled)
	if !enabled {
		return cfg
	}

	// Service configuration
	if serviceName := os.Getenv("EXPERIMENTAL_OTEL_SERVICE_NAME"); serviceName != "" {
		cfg.ServiceName.SetValue(serviceName)
	}

	if environment := os.Getenv("EXPERIMENTAL_OTEL_ENVIRONMENT"); environment != "" {
		cfg.Environment.SetValue(environment)
	}

	// Exporter configuration
	if endpoint := os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		cfg.ExporterEndpoint.SetValue(endpoint)
	}

	if protocol := os.Getenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_PROTOCOL"); protocol != "" {
		cfg.ExporterProtocol.SetValue(protocol)
	}

	// Ensure endpoint format matches protocol
	protocol := cfg.ExporterProtocol.Get()
	endpoint := cfg.ExporterEndpoint.Get()
	if protocol == "grpc" && (strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://")) {
		// Remove http/https prefix for gRPC
		if strings.HasPrefix(endpoint, "http://") {
			endpoint = strings.TrimPrefix(endpoint, "http://")
		} else if strings.HasPrefix(endpoint, "https://") {
			endpoint = strings.TrimPrefix(endpoint, "https://")
		}
		cfg.ExporterEndpoint.SetValue(endpoint)
	} else if protocol == "http" && !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		// Add http prefix for HTTP if not present
		cfg.ExporterEndpoint.SetValue("http://" + endpoint)
	}

	// Sampling configuration
	if samplerArg := os.Getenv("EXPERIMENTAL_OTEL_TRACES_SAMPLER_ARG"); samplerArg != "" {
		if rate, err := strconv.ParseFloat(samplerArg, 64); err == nil && rate >= 0.0 && rate <= 1.0 {
			cfg.SamplingRate.SetValue(rate)
		}
	}

	// Batch configuration
	if batchTimeout := os.Getenv("EXPERIMENTAL_OTEL_BSP_EXPORT_TIMEOUT"); batchTimeout != "" {
		if timeout, err := time.ParseDuration(batchTimeout); err == nil {
			cfg.BatchTimeout.SetValue(timeout)
		}
	}

	if batchSize := os.Getenv("EXPERIMENTAL_OTEL_BSP_MAX_EXPORT_BATCH_SIZE"); batchSize != "" {
		if size, err := strconv.Atoi(batchSize); err == nil && size > 0 {
			cfg.MaxExportBatchSize.SetValue(size)
		}
	}

	return cfg
}

// IsValid checks if the configuration is valid
func (c *Config) IsValid() error {
	if !c.Enabled.Get() {
		return nil
	}

	if c.ServiceName.Get() == "" {
		return ErrEmptyServiceName
	}

	if c.ExporterEndpoint.Get() == "" {
		return ErrEmptyExporterEndpoint
	}

	samplingRate := c.SamplingRate.Get()
	if samplingRate < 0.0 || samplingRate > 1.0 {
		return ErrInvalidSamplingRate
	}

	if c.BatchTimeout.Get() <= 0 {
		return ErrInvalidBatchTimeout
	}

	if c.MaxExportBatchSize.Get() <= 0 {
		return ErrInvalidBatchSize
	}

	return nil
}
