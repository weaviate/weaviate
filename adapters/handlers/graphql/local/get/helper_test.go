//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package get

import (
	"context"
	"fmt"
	"net/http"

	"github.com/graphql-go/graphql"
	test_helper "github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	modcontextionarygraphql "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/graphql"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	test_helper.MockResolver
}

type mockText2vecContextionaryModule struct{}

func (m *mockText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *mockText2vecContextionaryModule) Init(params modulecapabilities.ModuleInitParams) error {
	return nil
}

func (m *mockText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

func (m *mockText2vecContextionaryModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return modcontextionarygraphql.New().GetArguments(classname)
}

func (m *mockText2vecContextionaryModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return modcontextionarygraphql.New().ExploreArguments()
}

func (m *mockText2vecContextionaryModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return modcontextionarygraphql.New().ExtractFunctions()
}

func (m *mockText2vecContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return map[string]modulecapabilities.VectorForParams{}
}

func getMockModules() []modulecapabilities.Module {
	modules := []modulecapabilities.Module{}
	modules = append(modules, &mockText2vecContextionaryModule{})
	return modules
}

func newMockResolver() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := Build(&test_helper.SimpleSchema, logger, getMockModules())
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
