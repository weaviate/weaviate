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
	"context"
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
		err := modulesProvider.Init(context.Background(), nil)
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
		err := modulesProvider.Init(context.Background(), nil)

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
		err := modulesProvider.Init(context.Background(), nil)

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
		err := modulesProvider.Init(context.Background(), nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearArgument defined in more than one module")
		assert.Contains(t, err.Error(), "nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "limit conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should register simple additional property module", func(t *testing.T) {
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
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}).
			withArg("nearArgument"),
		)
		err := modulesProvider.Init(context.Background(), nil)
		registered := modulesProvider.GetAll()
		getArgs := modulesProvider.GetArguments(class)
		exploreArgs := modulesProvider.ExploreArguments(schema)
		extractedArgs := modulesProvider.ExtractSearchParams(arguments)
		restApiArgs := modulesProvider.RestApiAdditionalProperties("featureProjection")
		graphQLArgs := modulesProvider.GraphQLAdditionalFieldNames()

		// then
		mod1 := registered[0]
		assert.Nil(t, err)
		assert.Equal(t, "mod1", mod1.Name())
		assert.NotNil(t, getArgs["nearArgument"])
		assert.NotNil(t, exploreArgs["nearArgument"])
		assert.NotNil(t, extractedArgs["nearArgument"])
		assert.NotNil(t, restApiArgs["featureProjection"])
		assert.Contains(t, graphQLArgs, "featureProjection")
	})

	t.Run("should not register additional property modules providing the same params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withArg("nearArgument").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod2").
			withArg("nearArgument").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		err := modulesProvider.Init(context.Background(), nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "searcher: nearArgument defined in more than one module")
		assert.Contains(t, err.Error(), "graphql additional property: featureProjection defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: featureProjection defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: fp defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: f-p defined in more than one module")
	})

	t.Run("should not register additional property modules providing internal search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLAdditionalModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group").
			withGraphQLArg("classification", []string{"classification"}).
			withRestApiArg("classification", []string{"classification"}).
			withGraphQLArg("interpretation", []string{"interpretation"}).
			withRestApiArg("interpretation", []string{"interpretation"}).
			withGraphQLArg("certainty", []string{"certainty"}).
			withRestApiArg("certainty", []string{"certainty"}).
			withGraphQLArg("id", []string{"id"}).
			withRestApiArg("id", []string{"id"}),
		)
		err := modulesProvider.Init(context.Background(), nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "searcher: nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: limit conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: interpretation conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: interpretation conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should not register additional property modules providing faulty params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withArg("nearArgument").
			withGraphQLArg("semanticPath", []string{"semanticPath"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod2").
			withArg("nearArgument").
			withGraphQLArg("semanticPath", []string{"semanticPath"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod4").
			withGraphQLArg("classification", []string{"classification"}).
			withRestApiArg("classification", []string{"classification"}).
			withGraphQLArg("interpretation", []string{"interpretation"}).
			withRestApiArg("interpretation", []string{"interpretation"}).
			withGraphQLArg("certainty", []string{"certainty"}).
			withRestApiArg("certainty", []string{"certainty"}).
			withGraphQLArg("id", []string{"id"}).
			withRestApiArg("id", []string{"id"}),
		)
		err := modulesProvider.Init(context.Background(), nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "searcher: nearArgument defined in more than one module")
		assert.Contains(t, err.Error(), "searcher: nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: limit conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: classification conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "rest api additional property: interpretation conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "rest api additional property: certainty conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "rest api additional property: id conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: classification conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: interpretation conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: certainty conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: id conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: semanticPath defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: featureProjection defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: fp defined in more than one module")
		assert.Contains(t, err.Error(), "rest api additional property: f-p defined in more than one module")
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

func newGraphQLAdditionalModule(name string) *dummyAdditionalModule {
	return &dummyAdditionalModule{
		dummyGraphQLModule:                *newGraphQLModule(name),
		additionalFields:                  map[string]*graphql.Field{},
		extractAdditionalFunctions:        map[string]modulecapabilities.ExtractAdditionalFn{},
		additionalPropertiesDefaultValues: map[string]modulecapabilities.DefaultValueFn{},
		restApiAdditionalProperties:       map[string][]string{},
		graphQLAdditionalProperties:       map[string][]string{},
		searchAdditionalFunctions:         map[string]modulecapabilities.AdditionalSearch{},
	}
}

type dummyAdditionalModule struct {
	dummyGraphQLModule
	additionalFields                  map[string]*graphql.Field
	extractAdditionalFunctions        map[string]modulecapabilities.ExtractAdditionalFn
	additionalPropertiesDefaultValues map[string]modulecapabilities.DefaultValueFn
	restApiAdditionalProperties       map[string][]string
	graphQLAdditionalProperties       map[string][]string
	searchAdditionalFunctions         map[string]modulecapabilities.AdditionalSearch
}

func (m *dummyAdditionalModule) withArg(argName string) *dummyAdditionalModule {
	m.dummyGraphQLModule.withArg(argName)
	return m
}

func (m *dummyAdditionalModule) withExtractFn(argName string) *dummyAdditionalModule {
	m.dummyGraphQLModule.extractFunctions[argName] = fakeExtractFn
	return m
}

func (m *dummyAdditionalModule) withGraphQLArg(argName string, values []string) *dummyAdditionalModule {
	vals := m.graphQLAdditionalProperties[argName]
	if vals == nil {
		vals = []string{}
	}
	vals = append(vals, values...)
	m.graphQLAdditionalProperties[argName] = vals
	return m
}

func (m *dummyAdditionalModule) withRestApiArg(argName string, values []string) *dummyAdditionalModule {
	vals := m.restApiAdditionalProperties[argName]
	if vals == nil {
		vals = []string{}
	}
	vals = append(vals, values...)
	m.restApiAdditionalProperties[argName] = vals
	m.additionalPropertiesDefaultValues[argName] = func() interface{} { return 100 }
	return m
}

func (m *dummyAdditionalModule) GetAdditionalFields(classname string) map[string]*graphql.Field {
	return m.additionalFields
}

func (m *dummyAdditionalModule) ExtractAdditionalFunctions() map[string]modulecapabilities.ExtractAdditionalFn {
	return m.extractAdditionalFunctions
}

func (m *dummyAdditionalModule) AdditionalPropertiesDefaultValues() map[string]modulecapabilities.DefaultValueFn {
	return m.additionalPropertiesDefaultValues
}

func (m *dummyAdditionalModule) RestApiAdditionalProperties() map[string][]string {
	return m.restApiAdditionalProperties
}

func (m *dummyAdditionalModule) GraphQLAdditionalProperties() map[string][]string {
	return m.graphQLAdditionalProperties
}

func (m *dummyAdditionalModule) SearchAdditionalFunctions() map[string]modulecapabilities.AdditionalSearch {
	return m.searchAdditionalFunctions
}
