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

package modules

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/stretchr/testify/assert"
)

func TestModulesProvider(t *testing.T) {
	t.Run("should register simple module", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		class := &models.Class{
			Vectorizer: "mod1",
		}
		schema := &models.Schema{
			Classes: []*models.Class{class},
		}

		params := map[string]interface{}{}
		params["nearArgumentSomeParam"] = string("doesn't matter here")
		arguments := map[string]interface{}{}
		arguments["nearArgument"] = params

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		err := modulesProvider.Init(nil)
		registered := modulesProvider.GetAll()
		getArgs := modulesProvider.GetArguments(class)
		exploreArgs := modulesProvider.ExploreArguments(schema)
		extractedArgs := modulesProvider.ExtractSearchParams(arguments)

		// then
		mod1 := registered[0]
		assert.Nil(t, err)
		assert.Equal(t, "mod1", mod1.Name())
		assert.NotNil(t, getArgs["nearArgument"])
		assert.NotNil(t, exploreArgs["nearArgument"])
		assert.NotNil(t, extractedArgs["nearArgument"])
	})

	t.Run("should not register modules providing the same search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod2").withArg("nearArgument"))
		err := modulesProvider.Init(nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearArgument defined in more than one module")
	})

	t.Run("should not register modules providing internal search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		err := modulesProvider.Init(nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "limit conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should not register modules providing faulty params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod2").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		err := modulesProvider.Init(nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearArgument defined in more than one module")
		assert.Contains(t, err.Error(), "nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "limit conflicts with weaviate's internal searcher in modules: [mod3]")
	})
}

func fakeExtractFn(param map[string]interface{}) interface{} {
	extracted := map[string]interface{}{}
	extracted["nearArgumentParam"] = []string{"fake"}
	return extracted
}

func fakeValidateFn(param interface{}) error {
	return nil
}

func newGraphQLModule(name string) *dummyGraphQLModule {
	return &dummyGraphQLModule{
		dummyModuleNoCapabilities: newDummyModuleWithName(name),
		getArguments:              map[string]*graphql.ArgumentConfig{},
		exploreArguments:          map[string]*graphql.ArgumentConfig{},
		extractFunctions:          map[string]modulecapabilities.ExtractFn{},
		validateFunctions:         map[string]modulecapabilities.ValidateFn{},
	}
}

type dummyGraphQLModule struct {
	dummyModuleNoCapabilities
	getArguments      map[string]*graphql.ArgumentConfig
	exploreArguments  map[string]*graphql.ArgumentConfig
	extractFunctions  map[string]modulecapabilities.ExtractFn
	validateFunctions map[string]modulecapabilities.ValidateFn
}

func (m *dummyGraphQLModule) withArg(argName string) *dummyGraphQLModule {
	m.getArguments[argName] = &graphql.ArgumentConfig{}
	m.exploreArguments[argName] = &graphql.ArgumentConfig{}
	m.extractFunctions[argName] = fakeExtractFn
	m.validateFunctions[argName] = fakeValidateFn
	return m
}

func (m *dummyGraphQLModule) withExtractFn(argName string) *dummyGraphQLModule {
	m.extractFunctions[argName] = fakeExtractFn
	return m
}

func (m *dummyGraphQLModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return m.getArguments
}

func (m *dummyGraphQLModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return m.exploreArguments
}

func (m *dummyGraphQLModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return m.extractFunctions
}

func (m *dummyGraphQLModule) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return m.validateFunctions
}
