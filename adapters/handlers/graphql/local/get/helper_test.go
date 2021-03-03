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

package get

import (
	"context"
	"fmt"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	test_helper "github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
	modcontextionaryadditional "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional"
	modcontextionaryadditionalprojector "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/projector"
	modcontextionaryadditionalsempath "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/additional/sempath"
	modcontextionaryneartext "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/neartext"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	test_helper.MockResolver
}

type fakeExtender struct {
	returnArgs []search.Result
}

func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeExtender) DefaultValueFn() interface{} {
	return true
}

type fakeProjector struct {
	returnArgs []search.Result
}

func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	if len(param) > 0 {
		return &modcontextionaryadditionalprojector.Params{
			Enabled:      true,
			Algorithm:    ptString("tsne"),
			Dimensions:   ptInt(3),
			Iterations:   ptInt(100),
			LearningRate: ptInt(15),
			Perplexity:   ptInt(10),
		}
	}
	return &modcontextionaryadditionalprojector.Params{
		Enabled: true,
	}
}

func (f *fakeProjector) DefaultValueFn() interface{} {
	return &modcontextionaryadditionalprojector.Params{}
}

type fakePathBuilder struct {
	returnArgs []search.Result
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &modcontextionaryadditionalsempath.Params{}
}

func (f *fakePathBuilder) DefaultValueFn() interface{} {
	return &modcontextionaryadditionalsempath.Params{}
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

func (m *mockText2vecContextionaryModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return modcontextionaryneartext.New().GetArguments(classname)
}

func (m *mockText2vecContextionaryModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return modcontextionaryneartext.New().ExploreArguments()
}

func (m *mockText2vecContextionaryModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return modcontextionaryneartext.New().ExtractFunctions()
}

func (m *mockText2vecContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return map[string]modulecapabilities.VectorForParams{}
}

// additional properties
func (m *mockText2vecContextionaryModule) GetAdditionalFields(classname string) map[string]*graphql.Field {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).GetAdditionalFields(classname)
}

func (m *mockText2vecContextionaryModule) ExtractAdditionalFunctions() map[string]modulecapabilities.ExtractAdditionalFn {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).ExtractAdditionalFunctions()
}

func (m *mockText2vecContextionaryModule) AdditionalPropertiesDefaultValues() map[string]modulecapabilities.DefaultValueFn {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).AdditionalPropertiesDefaultValues()
}

func (m *mockText2vecContextionaryModule) RestApiAdditionalProperties() map[string][]string {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).RestApiAdditionalProperties()
}

func (m *mockText2vecContextionaryModule) GraphQLAdditionalProperties() map[string][]string {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).GraphQLAdditionalProperties()
}

func (m *mockText2vecContextionaryModule) SearchAdditionalFunctions() map[string]modulecapabilities.AdditionalSearch {
	return modcontextionaryadditional.New(&fakeExtender{}, &fakeProjector{}, &fakePathBuilder{}).SearchAdditionalFunctions()
}

type fakeModulesProvider struct{}

func (p *fakeModulesProvider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	txt2vec := &mockText2vecContextionaryModule{}
	if class.Vectorizer == txt2vec.Name() {
		return txt2vec.GetArguments(class.Class)
	}
	return map[string]*graphql.ArgumentConfig{}
}

func (p *fakeModulesProvider) ExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	if param, ok := arguments["nearText"]; ok {
		exractedParams["nearText"] = extractNearTextParam(param.(map[string]interface{}))
	}
	return exractedParams
}

func (p *fakeModulesProvider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	txt2vec := &mockText2vecContextionaryModule{}
	return txt2vec.GetAdditionalFields(class.Class)
}

func (p *fakeModulesProvider) ExtractAdditionalField(name string, params []*ast.Argument) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	if fns := txt2vec.ExtractAdditionalFunctions(); fns != nil {
		return fns[name](params)
	}
	return nil
}

func (p *fakeModulesProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet")
}

func (p *fakeModulesProvider) additionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{},
	searchVector []float32, capability string) (search.Results, error) {
	txt2vec := &mockText2vecContextionaryModule{}
	fns := txt2vec.SearchAdditionalFunctions()
	for name, value := range moduleParams {
		additionalPropertyFn := p.getAdditionalPropertyFn(fns[name], capability)
		if additionalPropertyFn != nil && value != nil {
			searchValue := value
			if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector); ok {
				searchVectorValue.SetSearchVector(searchVector)
				searchValue = searchVectorValue
			}
			resArray, err := additionalPropertyFn(ctx, in, searchValue, nil)
			if err != nil {
				return nil, err
			}
			in = resArray
		}
	}
	return in, nil
}

func (p *fakeModulesProvider) getAdditionalPropertyFn(searchAdditionalFns modulecapabilities.AdditionalSearch,
	capability string) modulecapabilities.AdditionalPropertyFn {
	switch capability {
	case "ObjectGet":
		return searchAdditionalFns.ObjectGet
	case "ObjectList":
		return searchAdditionalFns.ObjectList
	case "ExploreGet":
		return searchAdditionalFns.ExploreGet
	case "ExploreList":
		return searchAdditionalFns.ExploreList
	default:
		return nil
	}
}

func (p *fakeModulesProvider) GraphQLAdditionalFieldNames() []string {
	txt2vec := &mockText2vecContextionaryModule{}
	additionalPropertiesNames := []string{}
	for _, names := range txt2vec.GraphQLAdditionalProperties() {
		for i := range names {
			additionalPropertiesNames = append(additionalPropertiesNames, names[i])
		}
	}
	return additionalPropertiesNames
}

func extractNearTextParam(param map[string]interface{}) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	return txt2vec.ExtractFunctions()["nearText"](param)
}

func createArg(name string, value string) *ast.Argument {
	val := ast.StringValue{
		Kind:  "Kind",
		Value: value,
	}
	arg := ast.Argument{
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	return &arg
}

func extractAdditionalParam(name string, args []*ast.Argument) interface{} {
	txt2vec := &mockText2vecContextionaryModule{}
	fns := txt2vec.ExtractAdditionalFunctions()
	switch name {
	case "semanticPath", "featureProjection":
		if fn, ok := fns[name]; ok {
			return fn(args)
		}
		return nil
	default:
		return nil
	}
}

func getFakeModulesProvider() ModulesProvider {
	return &fakeModulesProvider{}
}

func newMockResolver() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := Build(&test_helper.SimpleSchema, logger, getFakeModulesProvider())
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func newMockResolverWithNoModules() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := Build(&test_helper.SimpleSchema, logger, nil)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func (m *mockResolver) GetClass(ctx context.Context, principal *models.Principal,
	params traverser.GetParams) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
