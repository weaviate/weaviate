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

package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

func TestRuntimeToggle_EnableDisableEnable(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	cleanup := opentelemetry.SetTestProvider(
		opentelemetry.NewTestProvider(tp, "test"),
	)
	t.Cleanup(func() {
		cleanup()
		tp.Shutdown(context.Background())
	})

	// Request 1: flags enabled → real span
	ctx1 := WithFlags(context.Background(), Flags{VectorSearch: true})
	_, span1 := StartSpan(ctx1, PathVectorSearch, "request.1")
	span1.End()

	require.Len(t, exporter.GetSpans(), 1)
	assert.Equal(t, "request.1", exporter.GetSpans()[0].Name)

	// Request 2: flags disabled → noop span (no new span in exporter)
	ctx2 := WithFlags(context.Background(), Flags{VectorSearch: false})
	_, span2 := StartSpan(ctx2, PathVectorSearch, "request.2")
	span2.End()

	assert.Len(t, exporter.GetSpans(), 1, "no new span should be added when disabled")

	// Request 3: flags enabled again → real span
	ctx3 := WithFlags(context.Background(), Flags{VectorSearch: true})
	_, span3 := StartSpan(ctx3, PathVectorSearch, "request.3")
	span3.End()

	require.Len(t, exporter.GetSpans(), 2)
	assert.Equal(t, "request.3", exporter.GetSpans()[1].Name)
}

func TestFlags_DefaultsToDisabled_OTelEnabled(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	cleanup := opentelemetry.SetTestProvider(
		opentelemetry.NewTestProvider(tp, "test"),
	)
	t.Cleanup(func() {
		cleanup()
		tp.Shutdown(context.Background())
	})

	// Context without WithFlags → default Flags{} → all paths disabled
	ctx := context.Background()
	_, span := StartSpan(ctx, PathVectorSearch, "should.not.appear")
	span.End()

	assert.Empty(t, exporter.GetSpans(), "no spans should be created with default flags")
}
