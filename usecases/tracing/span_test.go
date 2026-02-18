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
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestStartSpan_NoFlagsInContext(t *testing.T) {
	ctx := context.Background()
	outCtx, span := StartSpan(ctx, PathVectorSearch, "test.span")
	defer span.End()

	assert.Equal(t, ctx, outCtx, "context should be unchanged")
	assert.False(t, span.SpanContext().IsValid(), "span should be noop")
}

func TestStartSpan_VectorSearchFalse(t *testing.T) {
	ctx := WithFlags(context.Background(), Flags{VectorSearch: false})
	outCtx, span := StartSpan(ctx, PathVectorSearch, "test.span")
	defer span.End()

	assert.Equal(t, ctx, outCtx, "context should be unchanged")
	assert.False(t, span.SpanContext().IsValid(), "span should be noop")
}

func TestStartSpan_VectorSearchTrue_OTelDisabled(t *testing.T) {
	// OTel is not initialized in tests, so IsEnabled() returns false
	ctx := WithFlags(context.Background(), Flags{VectorSearch: true})
	outCtx, span := StartSpan(ctx, PathVectorSearch, "test.span")
	defer span.End()

	assert.Equal(t, ctx, outCtx, "context should be unchanged when OTel is disabled")
	assert.False(t, span.SpanContext().IsValid(), "span should be noop when OTel is disabled")
}

func TestWithFlags_RoundTrip(t *testing.T) {
	ctx := context.Background()

	// No flags set
	flags := getFlags(ctx)
	assert.False(t, flags.VectorSearch)

	// Set flags
	ctx = WithFlags(ctx, Flags{VectorSearch: true})
	flags = getFlags(ctx)
	assert.True(t, flags.VectorSearch)
}

func TestFlags_IsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		flags    Flags
		path     TracePath
		expected bool
	}{
		{
			name:     "VectorSearch enabled",
			flags:    Flags{VectorSearch: true},
			path:     PathVectorSearch,
			expected: true,
		},
		{
			name:     "VectorSearch disabled",
			flags:    Flags{VectorSearch: false},
			path:     PathVectorSearch,
			expected: false,
		},
		{
			name:     "unknown path",
			flags:    Flags{VectorSearch: true},
			path:     TracePath(999),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.flags.isEnabled(tt.path))
		})
	}
}

func TestNoopSpanIsValid(t *testing.T) {
	// Verify our package-level noop span is actually a noop
	require.NotNil(t, noopSpan)
	assert.False(t, noopSpan.SpanContext().IsValid())
	assert.IsType(t, noop.Span{}, noopSpan)

	// Calling End() on noop span should not panic
	noopSpan.End()
}

func TestStartSpan_ReturnsNoopSpanType(t *testing.T) {
	ctx := context.Background()
	_, span := StartSpan(ctx, PathVectorSearch, "test.span")
	defer span.End()

	// When disabled, should return the same noop span (not a new allocation)
	assert.Equal(t, noopSpan, span)
}

func TestStartSpan_WithSpanOptions(t *testing.T) {
	ctx := context.Background()
	_, span := StartSpan(ctx, PathVectorSearch, "test.span",
		trace.WithAttributes())
	defer span.End()

	// Should not panic even with options when returning noop
	assert.False(t, span.SpanContext().IsValid())
}
