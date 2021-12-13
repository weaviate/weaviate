//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modcontextionary

import (
	"context"
	"fmt"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/explore"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	test_helper "github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
	text2vecadditional "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional"
	text2vecadditionalprojector "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/projector"
	text2vecadditionalsempath "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/sempath"
	text2vecneartext "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/neartext"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	test_helper.MockResolver
}

type fakeInterpretation struct{}

func (f *fakeInterpretation) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return in, nil
}

func (f *fakeInterpretation) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeInterpretation) AdditonalPropertyDefaultValue() interface{} {
	return true
}

type fakeExtender struct {
	returnArgs []search.Result
}

func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeExtender) AdditonalPropertyDefaultValue() interface{} {
	return true
}

type fakeProjector struct {
	returnArgs []search.Result
}

func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	if len(param) > 0 {
		p := &text2vecadditionalprojector.Params{}
		err := p.SetDefaultsAndValidate(100, 4)
		if err != nil {
			return nil
		}
		return p
	}
	return &text2vecadditionalprojector.Params{
		Enabled: true,
	}
}

func (f *fakeProjector) AdditonalPropertyDefaultValue() interface{} {
	return &text2vecadditionalprojector.Params{}
}

type fakePathBuilder struct {
	returnArgs []search.Result
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &text2vecadditionalsempath.Params{}
}

func (f *fakePathBuilder) AdditonalPropertyDefaultValue() interface{} {
	return &text2vecadditionalsempath.Params{}
}

type mockText2vecContextionaryModule struct{}

func (m *mockText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *mockText2vecContextionaryModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *mockText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

// graphql arguments
func (m *mockText2vecContextionaryModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return text2vecneartext.New(nil).Arguments()
}

// additional properties
func (m *mockText2vecContextionaryModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return text2vecadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}, &fakeInterpretation{}).AdditionalProperties()
}

type fakeModulesProvider struct{}

func (p *fakeModulesProvider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	args := map[string]*graphql.ArgumentConfig{}
	txt2vec := &mockText2vecContextionaryModule{}
	if class.Vectorizer == txt2vec.Name() {
		for name, argument := range txt2vec.Arguments() {
			args[name] = argument.GetArgumentsFunction(class.Class)
		}
	}
	return args
}

func (p *fakeModulesProvider) ExploreArguments(schema *models.Schema) map[string]*graphql.ArgumentConfig {
	args := map[string]*graphql.ArgumentConfig{}
	txt2vec := &mockText2vecContextionaryModule{}
	for _, c := range schema.Classes {
		if c.Vectorizer == txt2vec.Name() {
			for name, argument := range txt2vec.Arguments() {
				args[name] = argument.ExploreArgumentsFunction()
			}
		}
	}
	return args
}

func (p *fakeModulesProvider) CrossClassExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	return p.ExtractSearchParams(arguments, "")
}

func (p *fakeModulesProvider) ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	if param, ok := arguments["nearText"]; ok {
		exractedParams["nearText"] = extractNearTextParam(param.(map[string]interface{}))
	}
	return exractedParams
}

func (p *fakeModulesProvider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	txt2vec := &mockText2vecContextionaryModule{}
	additionalProperties := map[string]*graphql.Field{}
	for name, additionalProperty := range txt2vec.AdditionalProperties() {
		if additionalProperty.GraphQLFieldFunction != nil {
			additionalProperties[name] = additionalProperty.GraphQLFieldFunction(class.Class)
		}
	}
	return additionalProperties
}

func (p *fakeModulesProvider) ExtractAdditionalField(className, name string, params []*ast.Argument) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	if additionalProperties := txt2vec.AdditionalProperties(); len(additionalProperties) > 0 {
		if additionalProperty, ok := additionalProperties[name]; ok {
			if additionalProperty.GraphQLExtractFunction != nil {
				return additionalProperty.GraphQLExtractFunction(params)
			}
		}
	}
	return nil
}

func (p *fakeModulesProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet", argumentModuleParams)
}

func (p *fakeModulesProvider) additionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{},
	searchVector []float32, capability string, argumentModuleParams map[string]interface{}) (search.Results, error) {
	txt2vec := &mockText2vecContextionaryModule{}
	additionalProperties := txt2vec.AdditionalProperties()
	for name, value := range moduleParams {
		additionalPropertyFn := p.getAdditionalPropertyFn(additionalProperties[name], capability)
		if additionalPropertyFn != nil && value != nil {
			searchValue := value
			if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector); ok {
				searchVectorValue.SetSearchVector(searchVector)
				searchValue = searchVectorValue
			}
			resArray, err := additionalPropertyFn(ctx, in, searchValue, nil, nil)
			if err != nil {
				return nil, err
			}
			in = resArray
		}
	}
	return in, nil
}

func (p *fakeModulesProvider) getAdditionalPropertyFn(additionalProperty modulecapabilities.AdditionalProperty,
	capability string) modulecapabilities.AdditionalPropertyFn {
	switch capability {
	case "ObjectGet":
		return additionalProperty.SearchFunctions.ObjectGet
	case "ObjectList":
		return additionalProperty.SearchFunctions.ObjectList
	case "ExploreGet":
		return additionalProperty.SearchFunctions.ExploreGet
	case "ExploreList":
		return additionalProperty.SearchFunctions.ExploreList
	default:
		return nil
	}
}

func (p *fakeModulesProvider) GraphQLAdditionalFieldNames() []string {
	txt2vec := &mockText2vecContextionaryModule{}
	additionalPropertiesNames := []string{}
	for _, additionalProperty := range txt2vec.AdditionalProperties() {
		if additionalProperty.GraphQLNames != nil {
			additionalPropertiesNames = append(additionalPropertiesNames, additionalProperty.GraphQLNames...)
		}
	}
	return additionalPropertiesNames
}

func extractNearTextParam(param map[string]interface{}) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	argument := txt2vec.Arguments()["nearText"]
	return argument.ExtractFunction(param)
}

func createArg(name string, value string) *ast.Argument {
	n := ast.Name{
		Value: name,
	}
	val := ast.StringValue{
		Kind:  "Kind",
		Value: value,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	a := ast.NewArgument(&arg)
	return a
}

func extractAdditionalParam(name string, args []*ast.Argument) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	additionalProperties := txt2vec.AdditionalProperties()
	switch name {
	case "semanticPath", "featureProjection":
		if ap, ok := additionalProperties[name]; ok {
			return ap.GraphQLExtractFunction(args)
		}
		return nil
	default:
		return nil
	}
}

func getFakeModulesProvider() *fakeModulesProvider {
	return &fakeModulesProvider{}
}

func newMockResolver() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := get.Build(&test_helper.SimpleSchema, logger, getFakeModulesProvider())
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": GetResolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func newExploreMockResolver() *mockResolver {
	field := explore.Build(test_helper.SimpleSchema.Objects, getFakeModulesProvider())
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Explore"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    ExploreResolver(mocker),
		"RequestsLog": mockLog,
	}
	return mocker
}

func (m *mockResolver) GetClass(ctx context.Context, principal *models.Principal,
	params traverser.GetParams) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}

func (m *mockResolver) Explore(ctx context.Context,
	principal *models.Principal, params traverser.ExploreParams) ([]search.Result, error) {
	args := m.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

// Resolver is a local abstraction of the required UC resolvers
type GetResolver interface {
	GetClass(ctx context.Context, principal *models.Principal, info traverser.GetParams) (interface{}, error)
}

type ExploreResolver interface {
	Explore(ctx context.Context, principal *models.Principal, params traverser.ExploreParams) ([]search.Result, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}
