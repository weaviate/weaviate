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
	"net/http"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
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
		modulesProvider.Register(&mockMod1Module{})
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
		modulesProvider.Register(&mockMod1Module{})
		modulesProvider.Register(&mockMod2Module{})
		err := modulesProvider.Init(nil)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearArgument defined in more than one module")
	})

	t.Run("should not register modules providing internal search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()

		// when
		modulesProvider.Register(&mockMod1Module{})
		modulesProvider.Register(&mockMod3Module{})
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
		modulesProvider.Register(&mockMod1Module{})
		modulesProvider.Register(&mockMod2Module{})
		modulesProvider.Register(&mockMod3Module{})
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

type mockMod1Module struct{}

func (m *mockMod1Module) Name() string {
	return "mod1"
}

func (m *mockMod1Module) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *mockMod1Module) RootHandler() http.Handler {
	return nil
}

func (m *mockMod1Module) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	arguments["nearArgument"] = &graphql.ArgumentConfig{}
	return arguments
}

func (m *mockMod1Module) ExploreArguments() map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	arguments["nearArgument"] = &graphql.ArgumentConfig{}
	return arguments
}

func (m *mockMod1Module) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	extractFns := map[string]modulecapabilities.ExtractFn{}
	extractFns["nearArgument"] = fakeExtractFn
	return extractFns
}

func (m *mockMod1Module) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	validateFns := map[string]modulecapabilities.ValidateFn{}
	validateFns["nearArgument"] = fakeValidateFn
	return validateFns
}

type mockMod2Module struct{}

func (m *mockMod2Module) Name() string {
	return "mod2"
}

func (m *mockMod2Module) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *mockMod2Module) RootHandler() http.Handler {
	return nil
}

func (m *mockMod2Module) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return nil
}

func (m *mockMod2Module) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return nil
}

func (m *mockMod2Module) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	extractFns := map[string]modulecapabilities.ExtractFn{}
	extractFns["nearArgument"] = fakeExtractFn
	return extractFns
}

func (m *mockMod2Module) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return nil
}

type mockMod3Module struct{}

func (m *mockMod3Module) Name() string {
	return "mod3"
}

func (m *mockMod3Module) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *mockMod3Module) RootHandler() http.Handler {
	return nil
}

func (m *mockMod3Module) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return nil
}

func (m *mockMod3Module) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return nil
}

func (m *mockMod3Module) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	extractFns := map[string]modulecapabilities.ExtractFn{}
	extractFns["nearObject"] = fakeExtractFn
	extractFns["nearVector"] = fakeExtractFn
	extractFns["where"] = fakeExtractFn
	extractFns["group"] = fakeExtractFn
	extractFns["limit"] = fakeExtractFn
	return extractFns
}

func (m *mockMod3Module) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return nil
}
