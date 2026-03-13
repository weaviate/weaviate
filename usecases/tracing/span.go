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

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

var noopSpan trace.Span

func init() {
	_, noopSpan = noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
}

// StartSpan creates an OTel span only if:
//  1. The given TracePath is enabled in the context (via WithFlags)
//  2. The global OTel provider is active
//
// When either condition is false, returns the original context and a
// noop span — zero allocation beyond a single bool check.
func StartSpan(ctx context.Context, path TracePath, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !getFlags(ctx).isEnabled(path) || !opentelemetry.IsEnabled() {
		return ctx, noopSpan
	}
	return opentelemetry.GetTracer().Start(ctx, name, opts...)
}
