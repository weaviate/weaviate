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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSetTestProvider_SetsAndRestores(t *testing.T) {
	// Precondition: globalProvider is nil (OTel not initialized in tests)
	require.Nil(t, globalProvider)
	require.False(t, IsEnabled())

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())

	cleanup := SetTestProvider(NewTestProvider(tp, "test"))

	assert.True(t, IsEnabled(), "should be enabled after SetTestProvider")

	cleanup()

	assert.Nil(t, globalProvider, "should restore nil provider")
	assert.False(t, IsEnabled(), "should be disabled after cleanup")
}

func TestGetTracer_ReturnsTestTracer(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())

	cleanup := SetTestProvider(NewTestProvider(tp, "test"))
	defer cleanup()

	tracer := GetTracer()
	ctx, span := tracer.Start(context.Background(), "test.span")
	span.End()

	_ = ctx
	require.Len(t, exporter.GetSpans(), 1)
	assert.Equal(t, "test.span", exporter.GetSpans()[0].Name)
}

func TestNewTestProvider_IsEnabled(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background())

	p := NewTestProvider(tp, "test-service")

	assert.True(t, p.IsEnabled())
	assert.NotNil(t, p.Tracer())
}
