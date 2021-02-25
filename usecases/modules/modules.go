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
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
)

var (
	internalSearchers            = []string{"nearObject", "nearVector", "where", "group", "limit"}
	internalAdditionalProperties = []string{"classification", "interpretation", "certainty", "id"}
)

type Provider struct {
	registered   map[string]modulecapabilities.Module
	schemaGetter schemaGetter
}

type schemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

func NewProvider() *Provider {
	return &Provider{
		registered: map[string]modulecapabilities.Module{},
	}
}

func (m *Provider) Register(mod modulecapabilities.Module) {
	m.registered[mod.Name()] = mod
}

func (m *Provider) GetByName(name string) modulecapabilities.Module {
	return m.registered[name]
}

func (m *Provider) GetAll() []modulecapabilities.Module {
	out := make([]modulecapabilities.Module, len(m.registered))
	i := 0
	for _, mod := range m.registered {
		out[i] = mod
		i++
	}

	return out
}

func (m *Provider) SetSchemaGetter(sg schemaGetter) {
	m.schemaGetter = sg
}

func (m *Provider) Init(params moduletools.ModuleInitParams) error {
	for i, mod := range m.GetAll() {
		if err := mod.Init(params); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		}
	}
	if err := m.validate(); err != nil {
		return errors.Wrap(err, "validate modules")
	}

	return nil
}

func (m *Provider) validate() error {
	searchers := map[string][]string{}
	additionalProps := map[string][]string{}
	for _, mod := range m.GetAll() {
		if module, ok := mod.(modulecapabilities.GraphQLArguments); ok {
			for argument := range module.ExtractFunctions() {
				if searchers[argument] == nil {
					searchers[argument] = []string{}
				}
				modules := searchers[argument]
				modules = append(modules, mod.Name())
				searchers[argument] = modules
			}
		}
		if module, ok := mod.(modulecapabilities.GraphQLAdditionalProperties); ok {
			for additionalProperty := range module.GetAdditionalFields("") {
				if additionalProps[additionalProperty] == nil {
					additionalProps[additionalProperty] = []string{}
				}
				modules := additionalProps[additionalProperty]
				modules = append(modules, mod.Name())
				searchers[additionalProperty] = modules
			}
		}
	}

	var errorMessages []string
	errorMessages = append(errorMessages,
		m.validateModules("searcher", searchers, internalSearchers)...)
	errorMessages = append(errorMessages,
		m.validateModules("additional property", additionalProps, internalAdditionalProperties)...)
	if len(errorMessages) > 0 {
		return errors.Errorf("%v", errorMessages)
	}

	return nil
}

func (m *Provider) validateModules(name string, properties map[string][]string, internalProperties []string) []string {
	errorMessages := []string{}
	for propertyName, modules := range properties {
		for i := range internalProperties {
			if internalProperties[i] == propertyName {
				errorMessages = append(errorMessages,
					fmt.Sprintf("%s: %s conflicts with weaviate's internal searcher in modules: %v",
						name, propertyName, modules))
			}
		}
		if len(modules) > 1 {
			errorMessages = append(errorMessages,
				fmt.Sprintf("%s: %s defined in more than one module: %v", name, propertyName, modules))
		}
	}
	return errorMessages
}

func (m *Provider) shouldIncludeClassArgument(class *models.Class, vectorizer string) bool {
	return class.Vectorizer == vectorizer
}

func (m *Provider) shouldIncludeArgument(schema *models.Schema, vectorizer string) bool {
	for _, c := range schema.Classes {
		if m.shouldIncludeClassArgument(c, vectorizer) {
			return true
		}
	}
	return false
}

// GetArguments provides GraphQL Get arguments
func (m *Provider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.GetArguments(class.Class) {
					arguments[name] = argument
				}
			}
		}
	}
	return arguments
}

// ExploreArguments provides GraphQL Explore arguments
func (m *Provider) ExploreArguments(schema *models.Schema) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range m.GetAll() {
		if m.shouldIncludeArgument(schema, module.Name()) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.ExploreArguments() {
					arguments[name] = argument
				}
			}
		}
	}
	return arguments
}

// GetAdditionalFields provides GraphQL Get additional fields
func (m *Provider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	for _, module := range m.GetAll() {
		if arg, ok := module.(modulecapabilities.GraphQLAdditionalProperties); ok {
			for name, field := range arg.GetAdditionalFields(class.Class) {
				additionalProperties[name] = field
			}
		}
	}
	return additionalProperties
}

func (m *Provider) ExtractAdditionalField(name string, params []*ast.Argument) interface{} {
	for _, module := range m.GetAll() {
		if arg, ok := module.(modulecapabilities.GraphQLAdditionalProperties); ok {
			if len(arg.ExtractAdditionalFunctions()) > 0 {
				if extractFn, ok := arg.ExtractAdditionalFunctions()[name]; ok {
					return extractFn(params)
				}
			}
		}
	}
	return nil
}

func (m *Provider) AdditionalPropertyFunction(name string) modulecapabilities.AdditionalPropertyFn {
	for _, module := range m.GetAll() {
		if arg, ok := module.(modulecapabilities.GraphQLAdditionalProperties); ok {
			if len(arg.AdditionalPropetiesFunctions()) > 0 {
				if additionalPropertyFn, ok := arg.AdditionalPropetiesFunctions()[name]; ok {
					return additionalPropertyFn
				}
			}
		}
	}

	panic("AdditionalPropertyFunction was called without any known params present")
}

// ExtractSearchParams extracts GraphQL arguments
func (m *Provider) ExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	for _, module := range m.GetAll() {
		if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
			for paramName, extractFn := range args.ExtractFunctions() {
				if param, ok := arguments[paramName]; ok {
					extracted := extractFn(param.(map[string]interface{}))
					exractedParams[paramName] = extracted
				}
			}
		}
	}
	return exractedParams
}

// ValidateSearchParam validates module parameters
func (m *Provider) ValidateSearchParam(name string, value interface{}) error {
	for _, module := range m.GetAll() {
		if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
			if validateFns := args.ValidateFunctions(); validateFns != nil {
				if validateFn, ok := validateFns[name]; ok {
					return validateFn(value)
				}
			}
		}
	}

	panic("ValidateParam was called without any known params present")
}

// VectorFromSearchParam gets a vector for a given argument. This is used in
// Get { Class() } for example
func (m *Provider) VectorFromSearchParam(ctx context.Context,
	className string, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn) ([]float32, error) {
	sch := m.schemaGetter.GetSchemaSkipAuth()
	class := sch.FindClassByName(schema.ClassName(className))
	if class == nil {
		return nil, errors.Errorf("class %q not found in schema", className)
	}

	for _, mod := range m.GetAll() {
		if searcher, ok := mod.(modulecapabilities.Searcher); ok {
			if vectorSearches := searcher.VectorSearches(); vectorSearches != nil {
				if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
					cfg := NewClassBasedModuleConfig(class, mod.Name())
					vector, err := searchVectorFn(ctx, params, findVectorFn, cfg)
					if err != nil {
						return nil, errors.Errorf("vectorize params: %v", err)
					}
					return vector, nil
				}
			}
		}
	}

	panic("VectorFromParams was called without any known params present")
}

// CrossClassVectorFromSearchParam gets a vector for a given argument without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (m *Provider) CrossClassVectorFromSearchParam(ctx context.Context,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn) ([]float32, error) {
	for _, mod := range m.GetAll() {
		if searcher, ok := mod.(modulecapabilities.Searcher); ok {
			if vectorSearches := searcher.VectorSearches(); vectorSearches != nil {
				if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
					vector, err := searchVectorFn(ctx, params, findVectorFn, nil)
					if err != nil {
						return nil, errors.Errorf("vectorize params: %v", err)
					}
					return vector, nil
				}
			}
		}
	}

	panic("VectorFromParams was called without any known params present")
}
