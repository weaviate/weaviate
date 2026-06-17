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
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// globalProvider holds the process-wide provider. It is an atomic pointer so the
// lockless hot-path reads in GetTracer/IsEnabled never race with Init (startup)
// or SetTestProvider (tests).
var globalProvider atomic.Pointer[Provider]

// Init initializes the global OpenTelemetry provider
func Init(logger logrus.FieldLogger) error {
	cfg := FromEnvironment()

	provider, err := NewProvider(cfg, logger)
	if err != nil {
		return err
	}

	globalProvider.Store(provider)

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
	p := globalProvider.Load()
	if p == nil {
		return nil
	}

	return p.Shutdown(ctx)
}

// GetTracer returns the global tracer
func GetTracer() trace.Tracer {
	p := globalProvider.Load()
	if p == nil {
		return noop.NewTracerProvider().Tracer("noop")
	}
	return p.Tracer()
}

// IsEnabled returns whether OpenTelemetry is enabled globally
func IsEnabled() bool {
	p := globalProvider.Load()
	return p != nil && p.IsEnabled()
}
