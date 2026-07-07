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

package traverser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/tracing"
)

func traceCfg(vector, bm25, hybrid bool) config.Config {
	return config.Config{
		TraceVectorSearch: configRuntime.NewDynamicValue(vector),
		TraceBM25Search:   configRuntime.NewDynamicValue(bm25),
		TraceHybridSearch: configRuntime.NewDynamicValue(hybrid),
	}
}

// TestBuildTraceFlags is the DoD-3 OR-semantics proof: the per-request toggle
// (traceSpans) forces every area on, ORing over the runtime toggles — so a
// request can be traced even while all runtime toggles are off.
func TestBuildTraceFlags(t *testing.T) {
	tests := []struct {
		name       string
		cfg        config.Config
		traceSpans bool
		want       map[tracing.Area]bool
	}{
		{
			name:       "per-request forces all areas over disabled runtime toggles",
			cfg:        traceCfg(false, false, false),
			traceSpans: true,
			want:       map[tracing.Area]bool{tracing.AreaVector: true, tracing.AreaBM25: true, tracing.AreaHybrid: true},
		},
		{
			name:       "no per-request, runtime toggles passed through",
			cfg:        traceCfg(true, false, true),
			traceSpans: false,
			want:       map[tracing.Area]bool{tracing.AreaVector: true, tracing.AreaBM25: false, tracing.AreaHybrid: true},
		},
		{
			name:       "no per-request, all runtime off",
			cfg:        traceCfg(false, false, false),
			traceSpans: false,
			want:       map[tracing.Area]bool{tracing.AreaVector: false, tracing.AreaBM25: false, tracing.AreaHybrid: false},
		},
		{
			name:       "per-request ORs over partially-enabled runtime toggles",
			cfg:        traceCfg(true, false, false),
			traceSpans: true,
			want:       map[tracing.Area]bool{tracing.AreaVector: true, tracing.AreaBM25: true, tracing.AreaHybrid: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := buildTraceFlags(tt.cfg, tt.traceSpans)
			for area, want := range tt.want {
				assert.Equalf(t, want, flags.IsEnabled(area), "area %q", area)
			}
		})
	}
}
