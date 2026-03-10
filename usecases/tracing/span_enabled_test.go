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
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

// setupTestTracing configures an in-memory OTel provider for testing.
// Returns the exporter so callers can inspect captured spans.
func setupTestTracing(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
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
	return exporter
}

func TestStartSpan_VectorSearchTrue_OTelEnabled(t *testing.T) {
	exporter := setupTestTracing(t)

	ctx := WithFlags(context.Background(), Flags{VectorSearch: true})
	outCtx, span := StartSpan(ctx, PathVectorSearch, "test.enabled.span")
	defer span.End()

	assert.True(t, span.SpanContext().IsValid(), "span should have a valid SpanContext")
	assert.NotEqual(t, ctx, outCtx, "context should be updated with the span")

	// End span so it appears in the exporter
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, "test.enabled.span", spans[0].Name)
}

func TestStartSpan_SpanAttributes(t *testing.T) {
	exporter := setupTestTracing(t)

	ctx := WithFlags(context.Background(), Flags{VectorSearch: true})
	_, span := StartSpan(ctx, PathVectorSearch, "test.attrs",
		trace.WithAttributes(
			attribute.String("collection", "MyClass"),
			attribute.Int("limit", 10),
		),
	)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)

	attrMap := make(map[string]attribute.Value)
	for _, a := range spans[0].Attributes {
		attrMap[string(a.Key)] = a.Value
	}

	assert.Equal(t, "MyClass", attrMap["collection"].AsString())
	assert.Equal(t, int64(10), attrMap["limit"].AsInt64())
}

func TestStartSpan_AllProductionSpanNames(t *testing.T) {
	spanNames := []string{
		"weaviate.traverser.GetClass",
		"weaviate.explorer.GetClass",
		"weaviate.db.VectorSearch",
		"weaviate.db.ResolveReferences",
		"weaviate.index.VectorSearch",
		"weaviate.shard.VectorSearch",
		"weaviate.vectorindex.Search",
		"weaviate.shard.BuildAllowList",
	}

	for _, name := range spanNames {
		t.Run(name, func(t *testing.T) {
			exporter := setupTestTracing(t)

			ctx := WithFlags(context.Background(), Flags{VectorSearch: true})
			_, span := StartSpan(ctx, PathVectorSearch, name)
			span.End()

			spans := exporter.GetSpans()
			require.Len(t, spans, 1)
			assert.Equal(t, name, spans[0].Name)
		})
	}
}

func TestStartSpan_DynamicAttributes(t *testing.T) {
	exporter := setupTestTracing(t)

	ctx := WithFlags(context.Background(), Flags{VectorSearch: true})
	_, span := StartSpan(ctx, PathVectorSearch, "test.dynamic")

	// Simulate the buildAllowList pattern: set attributes after span creation
	span.SetAttributes(
		attribute.Int("allowlist.size", 42),
		attribute.String("shard", "abc123"),
	)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)

	attrMap := make(map[string]attribute.Value)
	for _, a := range spans[0].Attributes {
		attrMap[string(a.Key)] = a.Value
	}

	assert.Equal(t, int64(42), attrMap["allowlist.size"].AsInt64())
	assert.Equal(t, "abc123", attrMap["shard"].AsString())
}

func TestStartSpan_SpanHierarchy(t *testing.T) {
	exporter := setupTestTracing(t)

	ctx := WithFlags(context.Background(), Flags{VectorSearch: true})

	// Create parent span
	ctx, parentSpan := StartSpan(ctx, PathVectorSearch, "parent.span")

	// Create child span from the context that contains the parent
	_, childSpan := StartSpan(ctx, PathVectorSearch, "child.span")

	childSpan.End()
	parentSpan.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)

	// Find parent and child by name
	var parent, child tracetest.SpanStub
	for _, s := range spans {
		switch s.Name {
		case "parent.span":
			parent = s
		case "child.span":
			child = s
		}
	}

	require.NotEmpty(t, parent.Name, "parent span not found")
	require.NotEmpty(t, child.Name, "child span not found")

	// Same trace
	assert.Equal(t, parent.SpanContext.TraceID(), child.SpanContext.TraceID(),
		"parent and child should share the same TraceID")

	// Child's parent is the parent span
	assert.Equal(t, parent.SpanContext.SpanID(), child.Parent.SpanID(),
		"child's ParentSpanID should be the parent's SpanID")
}

func TestStartSpan_NoFlagsInContext_OTelEnabled(t *testing.T) {
	setupTestTracing(t)

	// OTel is active but no flags in context → noop
	ctx := context.Background()
	outCtx, span := StartSpan(ctx, PathVectorSearch, "should.not.appear")
	defer span.End()

	assert.Equal(t, ctx, outCtx, "context should be unchanged")
	assert.False(t, span.SpanContext().IsValid(), "span should be noop without flags")
}
