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

// recordingProvider installs a test provider backed by an in-memory exporter
// that records spans synchronously and always samples. It returns the exporter
// and registers cleanup. Pass a non-nil sampler to override AlwaysSample.
func recordingProvider(t *testing.T, sampler sdktrace.Sampler) *tracetest.InMemoryExporter {
	t.Helper()
	if sampler == nil {
		sampler = sdktrace.AlwaysSample()
	}
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
		sdktrace.WithSampler(sampler),
	)
	cleanup := opentelemetry.SetTestProvider(opentelemetry.NewTestProvider(tp, "test"))
	t.Cleanup(func() {
		cleanup()
		_ = tp.Shutdown(context.Background())
	})
	return exporter
}

func recordedNames(exporter *tracetest.InMemoryExporter) []string {
	spans := exporter.GetSpans()
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name
	}
	return names
}

// TestStartSpan_TargetedGating is the DoD-4 proof: an area can be traced without
// the others. Each row enables a subset and asserts exactly that subset records.
func TestStartSpan_TargetedGating(t *testing.T) {
	tests := []struct {
		name  string
		areas map[Area]bool
		want  []string // span names expected, in start order
	}{
		{
			name:  "hybrid+vector on, bm25 off",
			areas: map[Area]bool{AreaHybrid: true, AreaBM25: false, AreaVector: true},
			want:  []string{"hybrid", "vector"},
		},
		{
			name:  "hybrid+bm25 on, vector off",
			areas: map[Area]bool{AreaHybrid: true, AreaBM25: true, AreaVector: false},
			want:  []string{"hybrid", "bm25"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter := recordingProvider(t, nil)
			ctx := WithFlags(context.Background(), NewFlags(tt.areas))

			for _, area := range []Area{AreaHybrid, AreaBM25, AreaVector} {
				_, span := StartSpan(ctx, area, string(area))
				span.End()
			}

			assert.ElementsMatch(t, tt.want, recordedNames(exporter))
		})
	}
}

func TestStartSpan_NoopZeroAlloc(t *testing.T) {
	// no flags in the context, and an explicitly disabled area: both must hit
	// the shared no-op path without allocating.
	disabled := WithFlags(context.Background(), NewFlags(map[Area]bool{AreaVector: false}))

	for _, tt := range []struct {
		name string
		ctx  context.Context
	}{
		{"no flags in context", context.Background()},
		{"area disabled", disabled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outCtx, span := StartSpan(tt.ctx, AreaVector, "x")
			assert.Equal(t, tt.ctx, outCtx, "context must be unchanged on the no-op path")
			assert.Equal(t, noopSpan, span, "must return the shared no-op span, not a fresh allocation")
			assert.False(t, span.IsRecording())

			allocs := testing.AllocsPerRun(100, func() {
				_, s := StartSpan(tt.ctx, AreaVector, "x")
				s.End()
			})
			assert.Zero(t, allocs, "disabled StartSpan must not allocate")
		})
	}
}

func TestStartSpan_ProviderDisabled(t *testing.T) {
	// Flags enabled, but no provider installed → IsEnabled() is false.
	require.False(t, opentelemetry.IsEnabled())
	ctx := WithFlags(context.Background(), NewFlags(map[Area]bool{AreaVector: true}))

	_, span := StartSpan(ctx, AreaVector, "x")
	assert.Equal(t, noopSpan, span)
	assert.False(t, span.IsRecording())
}

func TestStartSpan_ForceAll(t *testing.T) {
	exporter := recordingProvider(t, nil)
	// every per-area flag is false; only forceAll is set.
	ctx := WithFlags(context.Background(), WithForceAll(NewFlags(map[Area]bool{
		AreaVector: false, AreaBM25: false, AreaHybrid: false,
	})))

	for _, area := range []Area{AreaHybrid, AreaBM25, AreaVector} {
		_, span := StartSpan(ctx, area, string(area))
		span.End()
	}

	assert.ElementsMatch(t, []string{"hybrid", "bm25", "vector"}, recordedNames(exporter))
}

// TestStartRootSpan_ForcedRootUnderZeroRate pins the sampling-bypass contract: a
// per-request-forced trace must record even when the global sampler would drop
// it. The sampler here is the production shape — ParentBased(TraceIDRatioBased)
// at a 0 ratio, i.e. "sample nothing by ratio" — yet the forced root and its
// children record because force-all seeds a sampled parent that ParentBased
// honours. (A bare NeverSample is intentionally not used: it is not ParentBased,
// so it would not represent how Weaviate's provider is configured.)
func TestStartRootSpan_ForcedRootUnderZeroRate(t *testing.T) {
	exporter := recordingProvider(t, sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0)))
	ctx := WithFlags(context.Background(), WithForceAll(NewFlags(nil)))

	// rootCtx descends from the flagged ctx, so the force-all Flags are still
	// present for the child span.
	rootCtx, root := StartRootSpan(ctx, "root")
	require.True(t, root.IsRecording(), "forced root must record despite a 0 sampling ratio")

	// a child started under the forced root must also record (ParentBased follows
	// the sampled parent), proving the whole forced subtree is captured.
	_, child := StartSpan(rootCtx, AreaVector, "child")
	require.True(t, child.IsRecording(), "child of a forced root must record")
	child.End()
	root.End()

	assert.ElementsMatch(t, []string{"root", "child"}, recordedNames(exporter))
}

func TestStartRootSpan_AnyAreaGate(t *testing.T) {
	exporter := recordingProvider(t, nil)

	// no area enabled, not forced → no root.
	noneCtx := WithFlags(context.Background(), NewFlags(map[Area]bool{AreaBM25: false}))
	_, span := StartRootSpan(noneCtx, "root-none")
	assert.Equal(t, noopSpan, span)
	span.End()
	assert.Empty(t, recordedNames(exporter))

	// a single area enabled → root records.
	someCtx := WithFlags(context.Background(), NewFlags(map[Area]bool{AreaBM25: true}))
	_, span = StartRootSpan(someCtx, "root-some")
	span.End()
	assert.Equal(t, []string{"root-some"}, recordedNames(exporter))
}

func TestFlags_IsEnabled(t *testing.T) {
	tests := []struct {
		name string
		f    Flags
		area Area
		want bool
	}{
		{"area on", NewFlags(map[Area]bool{AreaVector: true}), AreaVector, true},
		{"area off", NewFlags(map[Area]bool{AreaVector: false}), AreaVector, false},
		{"unknown area", NewFlags(map[Area]bool{AreaVector: true}), Area("nope"), false},
		{"zero value", Flags{}, AreaVector, false},
		{"force-all wins over disabled area", WithForceAll(NewFlags(map[Area]bool{AreaVector: false})), AreaVector, true},
		{"force-all over nil map", WithForceAll(NewFlags(nil)), AreaHybrid, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.f.IsEnabled(tt.area))
		})
	}
}
