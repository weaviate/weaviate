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
		logger.Info("OpenTelemetry tracing disabled")
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
