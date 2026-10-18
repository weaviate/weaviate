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
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	enitiesSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/annotate"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// typedModule is a module of any type that serves the given additional
// properties, recording in served which module ran rerank.
type typedModule struct {
	dummyNonVectorizerModule
	moduleType modulecapabilities.ModuleType
	properties []string
	served     *[]string
}

func (m typedModule) Type() modulecapabilities.ModuleType {
	return m.moduleType
}

func (m typedModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	out := map[string]modulecapabilities.AdditionalProperty{}
	for _, name := range m.properties {
		if name == "rerank" {
			out[name] = rerankProperty(m.name, m.served)[name]
			continue
		}
		if name == "annotate" {
			out[name] = annotateProperty(m.name, m.served)
			continue
		}
		out[name] = modulecapabilities.AdditionalProperty{}
	}
	return out
}

// annotateProperty serves annotate by recording the module and the
// maxDocuments setting it reads from the config it is given.
func annotateProperty(module string, served *[]string) modulecapabilities.AdditionalProperty {
	fn := func(ctx context.Context, in []search.Result, params any, limit *int,
		argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
	) ([]search.Result, error) {
		if served != nil {
			*served = append(*served, fmt.Sprintf("%s maxDocuments=%v", module, cfg.ClassByModuleName(module)["maxDocuments"]))
		}
		return in, nil
	}
	return modulecapabilities.AdditionalProperty{SearchFunctions: modulecapabilities.AdditionalSearch{ExploreGet: fn, ExploreList: fn}}
}

func rerankerModule(name string, served *[]string) typedModule {
	return typedModule{dummyNonVectorizerModule{name: name}, modulecapabilities.Text2TextReranker, []string{"rerank"}, served}
}

func decisionsModule(name string, served *[]string) typedModule {
	return typedModule{dummyNonVectorizerModule{name: name}, modulecapabilities.Decisions, []string{"rerank", "annotate"}, served}
}

// A module's kind is its type. The schema holds one reranker and one
// decisions module per class.
func TestIsRerankerAndIsDecisionsByType(t *testing.T) {
	tests := []struct {
		name          string
		module        modulecapabilities.Module
		wantReranker  bool
		wantDecisions bool
		unregister    bool
	}{
		{name: "reranker", module: rerankerModule("reranker-x", nil), wantReranker: true},
		{name: "decisions module", module: decisionsModule("decisions-x", nil), wantDecisions: true},
		{
			name:   "module of another type that serves rerank",
			module: typedModule{dummyNonVectorizerModule{name: "other-x"}, modulecapabilities.Text2TextQnA, []string{"rerank"}, nil},
		},
		{name: "generative module", module: typedModule{dummyNonVectorizerModule{name: "generative-x"}, modulecapabilities.Text2TextGenerative, nil, nil}},
		{name: "module that is not registered", module: rerankerModule("reranker-missing", nil), unregister: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			if !tt.unregister {
				p.Register(tt.module)
			}

			assert.Equal(t, tt.wantReranker, p.IsReranker(tt.module.Name()))
			assert.Equal(t, tt.wantDecisions, p.IsDecisions(tt.module.Name()))
		})
	}
}

func kindsSchemaReader() schemaGetter {
	class := func(name string, modules ...string) *models.Class {
		moduleConfig := map[string]interface{}{}
		for _, module := range modules {
			moduleConfig[module] = map[string]interface{}{}
		}
		return &models.Class{Class: name, ModuleConfig: moduleConfig}
	}
	return &fakeSchemaGetter{schema: enitiesSchema.Schema{Objects: &models.Schema{Classes: []*models.Class{
		class("Reranker", "reranker-a"),
		class("Decisions", "decisions-z"),
		// decisions-z sorts after reranker-a, so a choice by name would pick it.
		class("Both", "reranker-a", "decisions-z"),
		class("None"),
		{Class: "Tuned", ModuleConfig: map[string]interface{}{"decisions-z": map[string]interface{}{"maxDocuments": 5, "model": "class-model"}}},
	}}}}
}

// The request decides which module reranks: the decisions module when it
// asks for a decision, otherwise the reranker, or the decisions module in
// its place when the class has none.
func TestRerankModule(t *testing.T) {
	tests := []struct {
		class    string
		decision bool
		want     string
		wantErr  string
	}{
		{class: "Reranker", want: "reranker-a"},
		{class: "Reranker", decision: true, wantErr: `rerank.decision: class "Reranker" has no decisions module and the request names no provider`},
		{class: "Decisions", want: "decisions-z"},
		{class: "Decisions", decision: true, want: "decisions-z"},
		{class: "Both", want: "reranker-a"},
		{class: "Both", decision: true, want: "decisions-z"},
		{class: "None", want: ""},
		{class: "None", decision: true, wantErr: `rerank.decision: class "None" has no decisions module and the request names no provider`},
	}
	for _, tt := range tests {
		t.Run(tt.class+"/decision="+map[bool]string{false: "no", true: "yes"}[tt.decision], func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", nil))
			p.Register(decisionsModule("decisions-z", nil))
			// A second module of each kind, so that no class gets one implicitly.
			p.Register(rerankerModule("reranker-b", nil))
			p.Register(decisionsModule("decisions-y", nil))

			class, err := p.getClass(tt.class)
			require.NoError(t, err)
			module, err := p.rerankModule(class, tt.decision, nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.want == "" {
				assert.Nil(t, module)
				return
			}
			require.NotNil(t, module)
			assert.Equal(t, tt.want, module.Name())
		})
	}
}

// A class that names a reranker and a decisions module gets its rerank from
// the reranker on every request, whatever the names, and decide from the
// decisions module.
func TestClassWithBothKindsReranksWithTheReranker(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var served []string
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(kindsSchemaReader())
	p.Register(rerankerModule("reranker-a", &served))
	p.Register(decisionsModule("decisions-z", &served))
	p.Register(rerankerModule("reranker-b", &served))
	p.Register(decisionsModule("decisions-y", &served))
	in := []search.Result{{ClassName: "Both"}}

	for range additionalExtendRuns {
		_, err := p.GetExploreAdditionalExtend(context.Background(), in, map[string]any{"rerank": "params"}, nil, nil)
		require.NoError(t, err)
	}

	require.Len(t, served, additionalExtendRuns)
	for _, module := range served {
		require.Equal(t, "reranker-a", module)
	}
	class, err := p.getClass("Both")
	require.NoError(t, err)
	properties := p.classAdditionalProperties(class)
	assert.Contains(t, properties, "annotate")
}

// A class whose moduleConfig names no module gets one of a kind implicitly
// only when exactly one module of that kind is registered. A reranker and a
// decisions module are two kinds, so both are included.
func TestImplicitInclusionCountsPerKind(t *testing.T) {
	class := &models.Class{Class: "Plain"}
	tests := []struct {
		name       string
		registered []modulecapabilities.Module
		want       map[string]bool
	}{
		{
			name:       "one reranker",
			registered: []modulecapabilities.Module{rerankerModule("reranker-x", nil)},
			want:       map[string]bool{"reranker-x": true},
		},
		{
			name:       "one decisions module",
			registered: []modulecapabilities.Module{decisionsModule("decisions-x", nil)},
			want:       map[string]bool{"decisions-x": true},
		},
		{
			name:       "a reranker and a decisions module",
			registered: []modulecapabilities.Module{rerankerModule("reranker-x", nil), decisionsModule("decisions-x", nil)},
			want:       map[string]bool{"reranker-x": true, "decisions-x": true},
		},
		{
			name:       "two rerankers",
			registered: []modulecapabilities.Module{rerankerModule("reranker-x", nil), rerankerModule("reranker-y", nil)},
			want:       map[string]bool{"reranker-x": false, "reranker-y": false},
		},
		{
			name:       "two decisions modules",
			registered: []modulecapabilities.Module{decisionsModule("decisions-x", nil), decisionsModule("decisions-y", nil)},
			want:       map[string]bool{"decisions-x": false, "decisions-y": false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			for _, module := range tt.registered {
				p.Register(module)
			}
			for name, want := range tt.want {
				module := p.GetByName(name)
				got := p.shouldIncludeClassArgument(class, name, module.Type(), p.getModuleAltNames(module))
				assert.Equal(t, want, got, name)
			}
		})
	}
}

// limitedDecisions is a decisions module that declares its limits.
type limitedDecisions struct {
	typedModule
	limits modulecapabilities.DecisionLimits
	err    error
}

func (m limitedDecisions) DecisionLimits(moduletools.ClassConfig) (modulecapabilities.DecisionLimits, error) {
	return m.limits, m.err
}

func TestAnnotateLimits(t *testing.T) {
	declared := modulecapabilities.DecisionLimits{MaxDocuments: 7, MaxLevels: 10, DataTypes: []enitiesSchema.DataType{enitiesSchema.DataTypeText}}
	tests := []struct {
		name    string
		class   string
		module  modulecapabilities.Module
		want    annotate.Limits
		wantErr string
	}{
		{
			name: "a module that declares its limits", class: "Decisions",
			module: limitedDecisions{decisionsModule("decisions-z", nil), declared, nil},
			want:   annotate.Limits{Module: "decisions-z", DecisionLimits: declared},
		},
		{
			name: "a class setting the module cannot work with", class: "Decisions",
			module:  limitedDecisions{decisionsModule("decisions-z", nil), declared, errors.New("maxDocuments must be a whole number between 1 and 1000, got many")},
			wantErr: "decisions-z: maxDocuments must be a whole number between 1 and 1000, got many",
		},
		{
			name: "a module that declares none judges text within the ceilings", class: "Decisions",
			module: decisionsModule("decisions-z", nil),
			want: annotate.Limits{Module: "decisions-z", DecisionLimits: modulecapabilities.DecisionLimits{
				DataTypes: []enitiesSchema.DataType{enitiesSchema.DataTypeText},
			}},
		},
		{
			name: "a class without a decisions module", class: "Reranker",
			module:  decisionsModule("decisions-z", nil),
			wantErr: `class "Reranker" has no decisions module and the request names no provider`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", nil))
			p.Register(rerankerModule("reranker-b", nil))
			p.Register(tt.module)
			p.Register(decisionsModule("decisions-y", nil))

			limits, err := p.AnnotateLimits(context.Background(), kindsSchemaReader().ReadOnlyClass(tt.class), nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, limits)
		})
	}
}

// A rerank that asks for a decision goes to the decisions module whatever
// module serves rerank otherwise, and fails on a class without one.
func TestRerankDecisionRoutesToTheDecisionsModule(t *testing.T) {
	property := "text"
	plain := &rank.Params{Property: &property}
	decision := &rank.Params{Property: &property, Decision: &rank.Decision{}}
	tests := []struct {
		name    string
		class   string
		params  *rank.Params
		want    string
		wantErr string
	}{
		{name: "both kinds, plain rerank", class: "Both", params: plain, want: "reranker-a"},
		{name: "both kinds, decision", class: "Both", params: decision, want: "decisions-z"},
		{name: "decisions only, plain rerank", class: "Decisions", params: plain, want: "decisions-z"},
		{name: "decisions only, decision", class: "Decisions", params: decision, want: "decisions-z"},
		{name: "reranker only, decision", class: "Reranker", params: decision, wantErr: `rerank.decision: class "Reranker" has no decisions module and the request names no provider`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			var served []string
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", &served))
			p.Register(decisionsModule("decisions-z", &served))
			p.Register(rerankerModule("reranker-b", &served))
			p.Register(decisionsModule("decisions-y", &served))
			in := []search.Result{{ClassName: tt.class}}

			_, err := p.GetExploreAdditionalExtend(context.Background(), in, map[string]any{"rerank": tt.params}, nil, nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, served)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []string{tt.want}, served)
		})
	}
}

// A request names a decisions module instead of the class's. The module
// needs no class config; it must be enabled and be a decisions module.
func TestDecisionsModuleByProvider(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		provider *ent.DecisionsProvider
		want     string
		wantErr  string
	}{
		{name: "the class's module", class: "Decisions", want: "decisions-z"},
		{name: "a module the class does not configure", class: "Reranker", provider: &ent.DecisionsProvider{Module: "decisions-y"}, want: "decisions-y"},
		{name: "another module than the class's", class: "Decisions", provider: &ent.DecisionsProvider{Module: "decisions-y"}, want: "decisions-y"},
		{name: "a module that is not enabled", class: "Decisions", provider: &ent.DecisionsProvider{Module: "decisions-nope"}, wantErr: `provider "decisions-nope" is not enabled`},
		{name: "a module of another type", class: "Decisions", provider: &ent.DecisionsProvider{Module: "reranker-a"}, wantErr: `provider "reranker-a" is not a decisions module`},
		{name: "no provider on a class without a module", class: "Reranker", wantErr: `class "Reranker" has no decisions module and the request names no provider`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", nil))
			p.Register(decisionsModule("decisions-z", nil))
			p.Register(decisionsModule("decisions-y", nil))

			module, err := p.decisionsModule(kindsSchemaReader().ReadOnlyClass(tt.class), tt.provider)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, module.Name())
		})
	}
}

// settingsDecisions is a decisions module whose limits come from its
// maxDocuments setting and whose class validation bounds it.
type settingsDecisions struct {
	typedModule
}

func (m settingsDecisions) DecisionLimits(cfg moduletools.ClassConfig) (modulecapabilities.DecisionLimits, error) {
	maxDocuments, _ := cfg.ClassByModuleName(m.name)["maxDocuments"].(int)
	return modulecapabilities.DecisionLimits{MaxDocuments: maxDocuments, DataTypes: []enitiesSchema.DataType{enitiesSchema.DataTypeText}}, nil
}

func (m settingsDecisions) ClassConfigDefaults() map[string]any { return nil }

func (m settingsDecisions) PropertyConfigDefaults(*enitiesSchema.DataType) map[string]any {
	return nil
}

func (m settingsDecisions) ValidateClass(_ context.Context, _ *models.Class, cfg moduletools.ClassConfig) error {
	if maxDocuments, _ := cfg.ClassByModuleName(m.name)["maxDocuments"].(int); maxDocuments > 1000 {
		return fmt.Errorf("maxDocuments must be between 1 and 1000, got %d", maxDocuments)
	}
	return nil
}

// The settings a request names are read over the class's, and go through
// the module's class validation before the search.
func TestAnnotateLimitsWithProvider(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		provider *ent.DecisionsProvider
		want     int
		wantErr  string
	}{
		{name: "the class setting", class: "Tuned", want: 5},
		{name: "the class's module with a request setting over the class's", class: "Tuned", provider: &ent.DecisionsProvider{Module: "decisions-z", Settings: map[string]any{"maxDocuments": 7}}, want: 7},
		{name: "the class's module named without settings", class: "Tuned", provider: &ent.DecisionsProvider{Module: "decisions-z"}, want: 5},
		{name: "another module reads its own defaults, not the class's settings", class: "Tuned", provider: &ent.DecisionsProvider{Module: "decisions-y", Settings: map[string]any{"maxDocuments": 9}}, want: 9},
		{name: "a module the class does not configure", class: "Reranker", provider: &ent.DecisionsProvider{Module: "decisions-y", Settings: map[string]any{"maxDocuments": 3}}, want: 3},
		{name: "a request setting the module rejects", class: "Tuned", provider: &ent.DecisionsProvider{Module: "decisions-z", Settings: map[string]any{"maxDocuments": 5000}}, wantErr: "decisions-z: maxDocuments must be between 1 and 1000, got 5000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", nil))
			p.Register(settingsDecisions{decisionsModule("decisions-z", nil)})
			p.Register(settingsDecisions{decisionsModule("decisions-y", nil)})

			limits, err := p.AnnotateLimits(context.Background(), kindsSchemaReader().ReadOnlyClass(tt.class), tt.provider)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, limits.MaxDocuments)
		})
	}
}

// annotate and a rerank by a decision go to the module the request names,
// with the request's settings over the class's; without a provider they go
// to the class's module.
func TestAdditionalExtendRoutesByProvider(t *testing.T) {
	property := "text"
	questions := []ent.DecisionQuestion{{Name: "a", Property: property, Instructions: "x", Kind: ent.DecisionPredicate}}
	tests := []struct {
		name    string
		class   string
		params  map[string]any
		want    []string
		wantErr string
	}{
		{
			name: "annotate by the class's module", class: "Tuned",
			params: map[string]any{"annotate": &annotate.Params{Questions: questions}},
			want:   []string{"decisions-z maxDocuments=5"},
		},
		{
			name: "annotate by a named module the class does not configure", class: "Reranker",
			params: map[string]any{"annotate": &annotate.Params{Questions: questions, Provider: &ent.DecisionsProvider{Module: "decisions-y", Settings: map[string]any{"maxDocuments": 7}}}},
			want:   []string{"decisions-y maxDocuments=7"},
		},
		{
			name: "annotate by the class's module with a request setting", class: "Tuned",
			params: map[string]any{"annotate": &annotate.Params{Questions: questions, Provider: &ent.DecisionsProvider{Module: "decisions-z", Settings: map[string]any{"maxDocuments": 8}}}},
			want:   []string{"decisions-z maxDocuments=8"},
		},
		{
			name: "annotate by a module that is not enabled", class: "Tuned",
			params:  map[string]any{"annotate": &annotate.Params{Questions: questions, Provider: &ent.DecisionsProvider{Module: "decisions-nope"}}},
			wantErr: `annotate: provider "decisions-nope" is not enabled`,
		},
		{
			name: "rerank by a decision of a named module on a class with a reranker only", class: "Reranker",
			params: map[string]any{"rerank": &rank.Params{Property: &property, Decision: &rank.Decision{}, Provider: &ent.DecisionsProvider{Module: "decisions-y"}}},
			want:   []string{"decisions-y"},
		},
		{
			name: "rerank by a decision without a provider goes to the class's module", class: "Both",
			params: map[string]any{"rerank": &rank.Params{Property: &property, Decision: &rank.Decision{}}},
			want:   []string{"decisions-z"},
		},
		{
			name: "rerank and annotate by different modules in one query", class: "Both",
			params: map[string]any{
				"rerank":   &rank.Params{Property: &property, Decision: &rank.Decision{}, Provider: &ent.DecisionsProvider{Module: "decisions-y"}},
				"annotate": &annotate.Params{Questions: questions},
			},
			want: []string{"decisions-y", "decisions-z maxDocuments=<nil>"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			var served []string
			p := NewProvider(logger, config.Config{})
			p.SetSchemaGetter(kindsSchemaReader())
			p.Register(rerankerModule("reranker-a", &served))
			p.Register(decisionsModule("decisions-z", &served))
			p.Register(decisionsModule("decisions-y", &served))
			in := []search.Result{{ClassName: tt.class, Schema: map[string]interface{}{property: "some text"}}}

			_, err := p.GetExploreAdditionalExtend(context.Background(), in, tt.params, nil, nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, served)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, served)
		})
	}
}
