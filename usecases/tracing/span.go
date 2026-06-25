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
	"crypto/rand"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

// noopSpan is a single shared no-op span returned on the disabled path. Reusing
// it (rather than NeverSample, which allocates) keeps StartSpan allocation-free
// when an area is off.
var noopSpan trace.Span

func init() {
	_, noopSpan = noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
}

// StartSpan creates an OTel span only when the area is enabled in the
// context-carried Flags and the global provider is active; otherwise it returns
// the original context and a shared no-op span (zero allocation beyond a map
// lookup). Callers always defer span.End().
func StartSpan(ctx context.Context, area Area, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	f, ok := flagsFromContext(ctx)
	if !ok || !f.IsEnabled(area) || !opentelemetry.IsEnabled() {
		return ctx, noopSpan
	}
	return opentelemetry.GetTracer().Start(ctx, name, opts...)
}

// StartRootSpan creates the request-level root span when any area is enabled or
// the per-request force-all flag is set. It is gated on "any area" rather than a
// single area so a pure-bm25 query still produces a coherent root.
//
// When force-all is active (the per-request toggle) it seeds a fresh, sampled
// remote parent so the forced trace records even under low/zero global sampling:
// the production sampler is ParentBased, which honours a sampled parent, so the
// whole forced subtree is captured regardless of the configured ratio. The fresh
// random trace ID detaches the forced trace from the upstream gRPC trace — the
// intended tradeoff for "trace this one request regardless of sampling".
//
// (trace.WithNewRoot alone is insufficient here: a new root is still subject to
// the ParentBased root sampler — TraceIDRatioBased — and would be dropped at low
// rates. Seeding a sampled parent is what actually forces recording.)
func StartRootSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	f, ok := flagsFromContext(ctx)
	if !ok || !f.anyEnabled() || !opentelemetry.IsEnabled() {
		return ctx, noopSpan
	}
	if f.forceAll {
		ctx = trace.ContextWithSpanContext(ctx, forcedSampledParent())
	}
	return opentelemetry.GetTracer().Start(ctx, name, opts...)
}

// forcedSampledParent builds a synthetic, sampled, remote SpanContext with a
// fresh random trace/span ID. Used as the parent of a force-all root so a
// ParentBased sampler records the request despite a low global ratio.
func forcedSampledParent() trace.SpanContext {
	var tid trace.TraceID
	var sid trace.SpanID
	// crypto/rand.Read never returns a short read or error for these sizes.
	_, _ = rand.Read(tid[:])
	_, _ = rand.Read(sid[:])
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}
