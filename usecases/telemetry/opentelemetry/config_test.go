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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEndpointParsing(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		protocol       string
		expectedResult string
	}{
		{
			name:           "gRPC with http prefix",
			endpoint:       "http://localhost:4317",
			protocol:       "grpc",
			expectedResult: "localhost:4317",
		},
		{
			name:           "gRPC with https prefix",
			endpoint:       "https://localhost:4317",
			protocol:       "grpc",
			expectedResult: "localhost:4317",
		},
		{
			name:           "gRPC without prefix",
			endpoint:       "localhost:4317",
			protocol:       "grpc",
			expectedResult: "localhost:4317",
		},
		{
			name:           "HTTP without prefix",
			endpoint:       "localhost:4318",
			protocol:       "http",
			expectedResult: "http://localhost:4318",
		},
		{
			name:           "HTTP with prefix",
			endpoint:       "http://localhost:4318",
			protocol:       "http",
			expectedResult: "http://localhost:4318",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables
			os.Setenv("EXPERIMENTAL_OTEL_ENABLED", "true")
			os.Setenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_ENDPOINT", tt.endpoint)
			os.Setenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_PROTOCOL", tt.protocol)

			// Create config from environment
			cfg := FromEnvironment()

			// Verify the endpoint was parsed correctly
			assert.Equal(t, tt.expectedResult, cfg.ExporterEndpoint.Get())
			assert.Equal(t, tt.protocol, cfg.ExporterProtocol.Get())

			// Clean up
			os.Unsetenv("EXPERIMENTAL_OTEL_ENABLED")
			os.Unsetenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_ENDPOINT")
			os.Unsetenv("EXPERIMENTAL_OTEL_EXPORTER_OTLP_PROTOCOL")
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.False(t, cfg.Enabled.Get())
	assert.Equal(t, "weaviate", cfg.ServiceName.Get())
	assert.Equal(t, "development", cfg.Environment.Get())
	assert.Equal(t, "localhost:4317", cfg.ExporterEndpoint.Get())
	assert.Equal(t, "grpc", cfg.ExporterProtocol.Get())
	assert.Equal(t, 0.01, cfg.SamplingRate.Get())
}

func TestConfigValidation(t *testing.T) {
	// Test valid config
	cfg := DefaultConfig()
	cfg.Enabled.SetValue(true)
	err := cfg.IsValid()
	assert.NoError(t, err)

	// Test invalid service name
	cfg.ServiceName.SetValue("")
	err = cfg.IsValid()
	assert.Error(t, err)
	assert.Equal(t, ErrEmptyServiceName, err)

	// Test invalid endpoint
	cfg.ServiceName.SetValue("test")
	cfg.ExporterEndpoint.SetValue("")
	err = cfg.IsValid()
	assert.Error(t, err)
	assert.Equal(t, ErrEmptyExporterEndpoint, err)

	// Test invalid sampling rate
	cfg.ExporterEndpoint.SetValue("localhost:4317")
	cfg.SamplingRate.SetValue(1.5)
	err = cfg.IsValid()
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidSamplingRate, err)
}
