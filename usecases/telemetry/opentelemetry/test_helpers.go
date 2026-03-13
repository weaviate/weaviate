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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SetTestProvider replaces the global provider for testing purposes.
// It returns a cleanup function that restores the previous provider.
// This must not be used concurrently — callers should not use t.Parallel().
func SetTestProvider(p *Provider) func() {
	prev := globalProvider
	globalProvider = p
	return func() {
		globalProvider = prev
	}
}

// NewTestProvider constructs a Provider from an existing SDK TracerProvider.
// Use this with tracetest.InMemoryExporter to capture spans in tests without
// requiring a real OTLP exporter.
func NewTestProvider(tp *sdktrace.TracerProvider, serviceName string) *Provider {
	return &Provider{
		config: &Config{
			Enabled:     true,
			ServiceName: serviceName,
		},
		provider: tp,
		tracer:   tp.Tracer(serviceName),
	}
}
