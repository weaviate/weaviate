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
	"github.com/stretchr/testify/require"
	"github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/language/ast"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	enitiesSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
)

// plainReranker is a reranker module that records in served when it runs.
type plainReranker struct {
	dummyNonVectorizerModule
	served *[]string
}

func (m plainReranker) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m plainReranker) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return rerankProperty(m.name, m.served)
}

// rerankProperty is a rerank additional property that records in served
// which module ran it.
func rerankProperty(module string, served *[]string) map[string]modulecapabilities.AdditionalProperty {
	fn := func(ctx context.Context, in []search.Result, params any, limit *int,
		argumentModuleParams map[string]any, cfg moduletools.ClassConfig,
	) ([]search.Result, error) {
		if served != nil {
			*served = append(*served, module)
		}
		return in, nil
	}
	return map[string]modulecapabilities.AdditionalProperty{
		"rerank": {SearchFunctions: modulecapabilities.AdditionalSearch{ExploreGet: fn, ExploreList: fn}},
	}
}

func rerankSchemaReader(t *testing.T) schemaGetter {
	t.Helper()
	sch := enitiesSchema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:        "ClassA",
					ModuleConfig: map[string]interface{}{"reranker-a": map[string]interface{}{"fetchDepth": 30}},
				},
				{
					Class: "ClassBoth",
					ModuleConfig: map[string]interface{}{
						"reranker-a": map[string]interface{}{"fetchDepth": 30},
						"reranker-z": map[string]interface{}{"fetchDepth": 55},
					},
				},
				{
					Class:        "ClassNone",
					ModuleConfig: map[string]interface{}{},
				},
			},
		},
	}
	return &fakeSchemaGetter{schema: sch}
}

// A class that names two rerankers must get the same one on every request.
func TestRerankModuleChoiceIsStable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var served []string
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(rerankSchemaReader(t))
	for _, name := range []string{"reranker-m", "reranker-z", "reranker-a"} {
		p.Register(plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: name}, served: &served})
	}
	in := []search.Result{{ClassName: "ClassBoth"}}

	for range additionalExtendRuns {
		_, err := p.GetExploreAdditionalExtend(context.Background(), in, map[string]any{"rerank": "params"}, nil, nil)
		require.NoError(t, err)
	}

	require.Len(t, served, additionalExtendRuns)
	for _, module := range served {
		require.Equal(t, "reranker-z", module)
	}
}

// graphQLReranker is a reranker module whose GraphQL field and extract
// function tell which module they belong to.
type graphQLReranker struct {
	dummyNonVectorizerModule
}

func (m graphQLReranker) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m graphQLReranker) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	property := rerankProperty(m.name, nil)["rerank"]
	property.GraphQLFieldFunction = func(string) *graphql.Field {
		return &graphql.Field{Description: m.name}
	}
	property.GraphQLExtractFunction = func([]*ast.Argument, *models.Class) any {
		return m.name
	}
	return map[string]modulecapabilities.AdditionalProperty{"rerank": property}
}

// The GraphQL field of a property two modules provide and the parsing of
// its arguments must come from the module that serves the search, on every
// call.
func TestRerankGraphQLChoiceIsStable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(rerankSchemaReader(t))
	for _, name := range []string{"reranker-m", "reranker-z", "reranker-a"} {
		p.Register(graphQLReranker{dummyNonVectorizerModule{name: name}})
	}
	class, err := p.getClass("ClassBoth")
	require.NoError(t, err)

	for range additionalExtendRuns {
		fields := p.GetAdditionalFields(class)
		require.Contains(t, fields, "rerank")
		require.Equal(t, "reranker-z", fields["rerank"].Description)
		require.Equal(t, "reranker-z", p.ExtractAdditionalField("ClassBoth", "rerank", nil))
	}
}

// generativeModule is a generative module whose client records in served
// when it generates a grouped result.
type generativeModule struct {
	dummyNonVectorizerModule
	served *[]string
}

func (m generativeModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m generativeModule) AdditionalGenerativeProperties() map[string]modulecapabilities.GenerativeProperty {
	return map[string]modulecapabilities.GenerativeProperty{
		m.name: {Client: recordingGenerativeClient{name: m.name, served: m.served}},
	}
}

type recordingGenerativeClient struct {
	name   string
	served *[]string
}

func (c recordingGenerativeClient) GenerateSingleResult(ctx context.Context,
	properties *modulecapabilities.GenerateProperties, prompt string, requestParams any, debug bool,
	cfg moduletools.ClassConfig,
) (*modulecapabilities.GenerateResponse, error) {
	return &modulecapabilities.GenerateResponse{}, nil
}

func (c recordingGenerativeClient) GenerateAllResults(ctx context.Context,
	properties []*modulecapabilities.GenerateProperties, task string, requestParams any, debug bool,
	cfg moduletools.ClassConfig,
) (*modulecapabilities.GenerateResponse, error) {
	*c.served = append(*c.served, c.name)
	return &modulecapabilities.GenerateResponse{}, nil
}

// A class that names two generative modules must use the same default
// provider on every request: the last of its modules by name.
func TestGenerativeDefaultProviderIsStable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var served []string
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(&fakeSchemaGetter{schema: enitiesSchema.Schema{Objects: &models.Schema{
		Classes: []*models.Class{{
			Class: "ClassTwoGenerative",
			ModuleConfig: map[string]interface{}{
				"generative-a": map[string]interface{}{},
				"generative-m": map[string]interface{}{},
			},
		}},
	}}})
	for _, name := range []string{"generative-m", "generative-z", "generative-a"} {
		p.Register(generativeModule{dummyNonVectorizerModule: dummyNonVectorizerModule{name: name}, served: &served})
	}
	task := "summarize"
	in := []search.Result{{ClassName: "ClassTwoGenerative", Schema: map[string]any{}}}

	for range additionalExtendRuns {
		params := map[string]any{"generate": &generate.Params{Task: &task}}
		_, err := p.GetExploreAdditionalExtend(context.Background(), in, params, nil, nil)
		require.NoError(t, err)
	}

	require.Len(t, served, additionalExtendRuns)
	for _, provider := range served {
		require.Equal(t, "generative-m", provider)
	}
}

// countingReranker counts the calls to AdditionalProperties.
type countingReranker struct {
	plainReranker
	calls *int
}

func (m countingReranker) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	*m.calls++
	return m.plainReranker.AdditionalProperties()
}

// A query that asks for no module property reads no module.
func TestAdditionalExtendWithoutModuleParamsReadsNoModule(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var calls int
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(rerankSchemaReader(t))
	p.Register(countingReranker{
		plainReranker: plainReranker{dummyNonVectorizerModule: dummyNonVectorizerModule{name: "reranker-a"}},
		calls:         &calls,
	})
	in := []search.Result{{ClassName: "ClassA"}}

	out, err := p.GetExploreAdditionalExtend(context.Background(), in, nil, nil, nil)

	require.NoError(t, err)
	require.Equal(t, in, out)
	require.Zero(t, calls)
}
