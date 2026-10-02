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

package modules

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

// Map iteration order is random per call, so one call can pass by chance.
const additionalExtendRuns = 50

func TestAdditionalExtendOrder(t *testing.T) {
	type extendFn func(p *Provider, in []search.Result, params map[string]any) ([]search.Result, error)
	capabilities := map[string]extendFn{
		"ExploreGet": func(p *Provider, in []search.Result, params map[string]any) ([]search.Result, error) {
			return p.GetExploreAdditionalExtend(context.Background(), in, params, nil, nil)
		},
		"ExploreList": func(p *Provider, in []search.Result, params map[string]any) ([]search.Result, error) {
			return p.ListExploreAdditionalExtend(context.Background(), in, params, nil)
		},
	}

	tests := []struct {
		name string
		// keep is how many results rerank returns.
		keep      int
		params    []string
		wantCalls []string
		wantLen   int
	}{
		{
			name:      "rerank runs before generate",
			keep:      3,
			params:    []string{"generate", "rerank"},
			wantCalls: []string{"rerank:3", "generate:3"},
			wantLen:   3,
		},
		{
			name:      "generate sees only the results rerank kept",
			keep:      1,
			params:    []string{"generate", "rerank"},
			wantCalls: []string{"rerank:3", "generate:1"},
			wantLen:   1,
		},
		{
			name:      "properties after rerank run in name order",
			keep:      2,
			params:    []string{"summary", "generate", "rerank", "answer"},
			wantCalls: []string{"rerank:3", "answer:2", "generate:2", "summary:2"},
			wantLen:   2,
		},
		{
			name:      "nothing runs after rerank drops every result",
			keep:      0,
			params:    []string{"generate", "rerank", "answer"},
			wantCalls: []string{"rerank:3"},
			wantLen:   0,
		},
		{
			name:      "without rerank the order is by name",
			keep:      3,
			params:    []string{"summary", "generate", "answer"},
			wantCalls: []string{"answer:3", "generate:3", "summary:3"},
			wantLen:   3,
		},
	}
	for capability, extend := range capabilities {
		for _, tt := range tests {
			t.Run(capability+"/"+tt.name, func(t *testing.T) {
				for range additionalExtendRuns {
					recorder := &callRecorder{keep: tt.keep}
					p := newProviderWithAdditionalProperties(t, recorder)

					params := map[string]any{}
					for _, name := range tt.params {
						params[name] = "params"
					}
					in := []search.Result{
						{ClassName: "ClassOne"}, {ClassName: "ClassOne"}, {ClassName: "ClassOne"},
					}

					out, err := extend(p, in, params)

					require.NoError(t, err)
					require.Equal(t, tt.wantCalls, recorder.calls)
					assert.Len(t, out, tt.wantLen)
				}
			})
		}
	}
}

type callRecorder struct {
	keep  int
	calls []string
}

func (r *callRecorder) property(name string) modulecapabilities.AdditionalProperty {
	fn := func(ctx context.Context, in []search.Result, params any, limit *int,
		argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
	) ([]search.Result, error) {
		r.calls = append(r.calls, name+":"+string(rune('0'+len(in))))
		if name == "rerank" {
			return in[:r.keep], nil
		}
		return in, nil
	}
	return modulecapabilities.AdditionalProperty{
		SearchFunctions: modulecapabilities.AdditionalSearch{ExploreGet: fn, ExploreList: fn},
	}
}

func newProviderWithAdditionalProperties(t *testing.T, recorder *callRecorder) *Provider {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(getMockSchemaReader(t))

	module := newGraphQLAdditionalModule("mod1")
	for _, name := range []string{"rerank", "generate", "answer", "summary"} {
		module.additionalProperties[name] = recorder.property(name)
	}
	p.Register(module)
	return p
}
