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
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus"
)

var (
	internalSearchers            = []string{"nearObject", "nearVector", "where", "group", "limit"}
	internalAdditionalProperties = []string{"classification", "certainty", "id"}
)

type Provider struct {
	registered             map[string]modulecapabilities.Module
	schemaGetter           schemaGetter
	hasMultipleVectorizers bool
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

func (m *Provider) GetAllExclude(module string) []modulecapabilities.Module {
	filtered := []modulecapabilities.Module{}
	for _, mod := range m.GetAll() {
		if mod.Name() != module {
			filtered = append(filtered, mod)
		}
	}
	return filtered
}

func (m *Provider) SetSchemaGetter(sg schemaGetter) {
	m.schemaGetter = sg
}

func (m *Provider) Init(ctx context.Context,
	params moduletools.ModuleInitParams, logger logrus.FieldLogger) error {
	for i, mod := range m.GetAll() {
		if err := mod.Init(ctx, params); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		}
	}
	for i, mod := range m.GetAll() {
		if modDependency, ok := mod.(modulecapabilities.ModuleDependency); ok {
			if err := modDependency.InitDependency(m.GetAllExclude(mod.Name())); err != nil {
				return errors.Wrapf(err, "init module dependency %d (%q)", i, mod.Name())
			}
		}
	}
	if err := m.validate(); err != nil {
		return errors.Wrap(err, "validate modules")
	}
	if m.HasMultipleVectorizers() {
		logger.Warn("Multiple vector spaces are present, " +
			"GraphQL Explore and REST API list objects endpoint module include params has been disabled as a result.")
	}
	return nil
}

func (m *Provider) validate() error {
	searchers := map[string][]string{}
	additionalGraphQLProps := map[string][]string{}
	additionalRestAPIProps := map[string][]string{}
	for _, mod := range m.GetAll() {
		if module, ok := mod.(modulecapabilities.GraphQLArguments); ok {
			allArguments := []string{}
			for paraName, argument := range module.Arguments() {
				if argument.ExtractFunction != nil {
					allArguments = append(allArguments, paraName)
				}
			}
			searchers = m.scanProperties(searchers, allArguments, mod.Name())
		}
		if module, ok := mod.(modulecapabilities.AdditionalProperties); ok {
			allAdditionalRestAPIProps, allAdditionalGrapQLProps := m.getAdditionalProps(module.AdditionalProperties())
			additionalGraphQLProps = m.scanProperties(additionalGraphQLProps,
				allAdditionalGrapQLProps, mod.Name())
			additionalRestAPIProps = m.scanProperties(additionalRestAPIProps,
				allAdditionalRestAPIProps, mod.Name())
		}
	}

	var errorMessages []string
	errorMessages = append(errorMessages,
		m.validateModules("searcher", searchers, internalSearchers)...)
	errorMessages = append(errorMessages,
		m.validateModules("graphql additional property", additionalGraphQLProps, internalAdditionalProperties)...)
	errorMessages = append(errorMessages,
		m.validateModules("rest api additional property", additionalRestAPIProps, internalAdditionalProperties)...)
	if len(errorMessages) > 0 {
		return errors.Errorf("%v", errorMessages)
	}

	return nil
}

func (m *Provider) scanProperties(result map[string][]string, properties []string, module string) map[string][]string {
	for i := range properties {
		if result[properties[i]] == nil {
			result[properties[i]] = []string{}
		}
		modules := result[properties[i]]
		modules = append(modules, module)
		result[properties[i]] = modules
	}
	return result
}

func (m *Provider) getAdditionalProps(additionalProps map[string]modulecapabilities.AdditionalProperty) ([]string, []string) {
	restProps := []string{}
	graphQLProps := []string{}

	for _, additionalProperty := range additionalProps {
		if additionalProperty.RestNames != nil {
			restProps = append(restProps, additionalProperty.RestNames...)
		}
		if additionalProperty.GraphQLNames != nil {
			graphQLProps = append(graphQLProps, additionalProperty.GraphQLNames...)
		}
	}
	return restProps, graphQLProps
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
			m.hasMultipleVectorizers = true
		}
	}
	return errorMessages
}

func (m *Provider) isDefaultModule(module string) bool {
	return module == "qna-transformers" || module == "text-spellcheck"
}

func (m *Provider) shouldIncludeClassArgument(class *models.Class, module string) bool {
	return class.Vectorizer == module || m.isDefaultModule(module)
}

func (m *Provider) shouldIncludeArgument(schema *models.Schema, module string) bool {
	for _, c := range schema.Classes {
		if m.shouldIncludeClassArgument(c, module) {
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
				for name, argument := range arg.Arguments() {
					if argument.GetArgumentsFunction != nil {
						arguments[name] = argument.GetArgumentsFunction(class.Class)
					}
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
				for name, argument := range arg.Arguments() {
					if argument.ExploreArgumentsFunction != nil {
						arguments[name] = argument.ExploreArgumentsFunction()
					}
				}
			}
		}
	}
	return arguments
}

// CrossClassExtractSearchParams extract search params from modules without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (m *Provider) CrossClassExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	return m.extractSearchParams(arguments, nil)
}

// ExtractSearchParams extracts GraphQL arguments
func (m *Provider) ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	class, err := m.getClass(className)
	if err != nil {
		return exractedParams
	}
	return m.extractSearchParams(arguments, class)
}

func (m *Provider) extractSearchParams(arguments map[string]interface{}, class *models.Class) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	for _, module := range m.GetAll() {
		if !m.hasMultipleVectorizers || m.shouldIncludeClassArgument(class, module.Name()) {
			if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for paramName, argument := range args.Arguments() {
					if param, ok := arguments[paramName]; ok && argument.ExtractFunction != nil {
						extracted := argument.ExtractFunction(param.(map[string]interface{}))
						exractedParams[paramName] = extracted
					}
				}
			}
		}
	}
	return exractedParams
}

// ValidateSearchParam validates module parameters
func (m *Provider) ValidateSearchParam(name string, value interface{}, className string) error {
	class, err := m.getClass(className)
	if err != nil {
		return err
	}

	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for paramName, argument := range args.Arguments() {
					if paramName == name && argument.ValidateFunction != nil {
						return argument.ValidateFunction(value)
					}
				}
			}
		}
	}

	panic("ValidateParam was called without any known params present")
}

// GetAdditionalFields provides GraphQL Get additional fields
func (m *Provider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
				for name, additionalProperty := range arg.AdditionalProperties() {
					if additionalProperty.GraphQLFieldFunction != nil {
						additionalProperties[name] = additionalProperty.GraphQLFieldFunction(class.Class)
					}
				}
			}
		}
	}
	return additionalProperties
}

// ExtractAdditionalField extracts additional properties from given graphql arguments
func (m *Provider) ExtractAdditionalField(className, name string, params []*ast.Argument) interface{} {
	class, err := m.getClass(className)
	if err != nil {
		return err
	}
	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
				if additionalProperties := arg.AdditionalProperties(); len(additionalProperties) > 0 {
					if additionalProperty, ok := additionalProperties[name]; ok {
						if additionalProperty.GraphQLExtractFunction != nil {
							return additionalProperty.GraphQLExtractFunction(params)
						}
					}
				}
			}
		}
	}
	return nil
}

// GetObjectAdditionalExtend extends rest api get queries with additional properties
func (m *Provider) GetObjectAdditionalExtend(ctx context.Context,
	in *search.Result, moduleParams map[string]interface{}) (*search.Result, error) {
	resArray, err := m.additionalExtend(ctx, search.Results{*in}, moduleParams, nil, "ObjectGet", nil)
	if err != nil {
		return nil, err
	}
	return &resArray[0], nil
}

// ListObjectsAdditionalExtend extends rest api list queries with additional properties
func (m *Provider) ListObjectsAdditionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{}) (search.Results, error) {
	return m.additionalExtend(ctx, in, moduleParams, nil, "ObjectList", nil)
}

// GetExploreAdditionalExtend extends graphql api get queries with additional properties
func (m *Provider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return m.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet", argumentModuleParams)
}

// ListExploreAdditionalExtend extends graphql api list queries with additional properties
func (m *Provider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{},
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return m.additionalExtend(ctx, in, moduleParams, nil, "ExploreList", argumentModuleParams)
}

func (m *Provider) additionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	capability string, argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	toBeExtended := in
	if len(toBeExtended) > 0 {
		class, err := m.getClassFromSearchResult(toBeExtended)
		if err != nil {
			return nil, err
		}
		allAdditionalProperties := map[string]modulecapabilities.AdditionalProperty{}
		for _, module := range m.GetAll() {
			if m.shouldIncludeClassArgument(class, module.Name()) {
				if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
					if arg != nil && arg.AdditionalProperties() != nil {
						for name, additionalProperty := range arg.AdditionalProperties() {
							allAdditionalProperties[name] = additionalProperty
						}
					}
				}
			}
		}
		if len(allAdditionalProperties) > 0 {
			if err := m.checkCapabilities(allAdditionalProperties, moduleParams, capability); err != nil {
				return nil, err
			}
			for name, value := range moduleParams {
				additionalPropertyFn := m.getAdditionalPropertyFn(allAdditionalProperties[name], capability)
				if additionalPropertyFn != nil && value != nil {
					searchValue := value
					if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector); ok {
						searchVectorValue.SetSearchVector(searchVector)
						searchValue = searchVectorValue
					}
					resArray, err := additionalPropertyFn(ctx, toBeExtended, searchValue, nil, argumentModuleParams)
					if err != nil {
						return nil, errors.Errorf("extend %s: %v", name, err)
					}
					toBeExtended = resArray
				} else {
					return nil, errors.Errorf("unknown capability: %s", name)
				}
			}
		}
	}
	return toBeExtended, nil
}

func (m *Provider) getClassFromSearchResult(in []search.Result) (*models.Class, error) {
	if len(in) > 0 {
		return m.getClass(in[0].ClassName)
	}
	return nil, errors.Errorf("unknown class")
}

func (m *Provider) checkCapabilities(additionalProperties map[string]modulecapabilities.AdditionalProperty,
	moduleParams map[string]interface{}, capability string) error {
	for name := range moduleParams {
		additionalPropertyFn := m.getAdditionalPropertyFn(additionalProperties[name], capability)
		if additionalPropertyFn == nil {
			return errors.Errorf("unknown capability: %s", name)
		}
	}
	return nil
}

func (m *Provider) getAdditionalPropertyFn(
	additionalProperty modulecapabilities.AdditionalProperty,
	capability string,
) modulecapabilities.AdditionalPropertyFn {
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

// GraphQLAdditionalFieldNames get's all additional field names used in graphql
func (m *Provider) GraphQLAdditionalFieldNames() []string {
	additionalPropertiesNames := []string{}
	for _, module := range m.GetAll() {
		if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
			for _, additionalProperty := range arg.AdditionalProperties() {
				if additionalProperty.GraphQLNames != nil {
					additionalPropertiesNames = append(additionalPropertiesNames, additionalProperty.GraphQLNames...)
				}
			}
		}
	}
	return additionalPropertiesNames
}

// RestApiAdditionalProperties get's all rest specific additional properties with their
// default values
func (m *Provider) RestApiAdditionalProperties(includeProp string, class *models.Class) map[string]interface{} {
	moduleParams := map[string]interface{}{}
	for _, module := range m.GetAll() {
		if !m.hasMultipleVectorizers || m.shouldIncludeClassArgument(class, module.Name()) {
			if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
				for name, additionalProperty := range arg.AdditionalProperties() {
					for _, includePropName := range additionalProperty.RestNames {
						if includePropName == includeProp && moduleParams[name] == nil {
							moduleParams[name] = additionalProperty.DefaultValue
						}
					}
				}
			}
		}
	}
	return moduleParams
}

// VectorFromSearchParam gets a vector for a given argument. This is used in
// Get { Class() } for example
func (m *Provider) VectorFromSearchParam(ctx context.Context,
	className string, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn) ([]float32, error) {
	class, err := m.getClass(className)
	if err != nil {
		return nil, err
	}

	for _, mod := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, mod.Name()) {
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

// ParseClassifierSettings parses and adds classifier specific settings
func (m *Provider) ParseClassifierSettings(name string,
	params *models.Classification) error {
	class, err := m.getClass(params.Class)
	if err != nil {
		return err
	}
	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if c, ok := module.(modulecapabilities.ClassificationProvider); ok {
				for _, classifier := range c.Classifiers() {
					if classifier != nil && classifier.Name() == name {
						return classifier.ParseClassifierSettings(params)
					}
				}
			}
		}
	}
	return nil
}

// GetClassificationFn returns given module's classification
func (m *Provider) GetClassificationFn(className, name string,
	params modulecapabilities.ClassifyParams) (modulecapabilities.ClassifyItemFn, error) {
	class, err := m.getClass(className)
	if err != nil {
		return nil, err
	}
	for _, module := range m.GetAll() {
		if m.shouldIncludeClassArgument(class, module.Name()) {
			if c, ok := module.(modulecapabilities.ClassificationProvider); ok {
				for _, classifier := range c.Classifiers() {
					if classifier != nil && classifier.Name() == name {
						return classifier.ClassifyFn(params)
					}
				}
			}
		}
	}
	return nil, errors.Errorf("classifier %s not found", name)
}

// GetMeta returns meta information about modules
func (m *Provider) GetMeta() (map[string]interface{}, error) {
	metaInfos := map[string]interface{}{}
	for _, module := range m.GetAll() {
		if c, ok := module.(modulecapabilities.MetaProvider); ok {
			meta, err := c.MetaInfo()
			if err != nil {
				return nil, err
			}
			metaInfos[module.Name()] = meta
		}
	}
	return metaInfos, nil
}

func (m *Provider) getClass(className string) (*models.Class, error) {
	sch := m.schemaGetter.GetSchemaSkipAuth()
	class := sch.FindClassByName(schema.ClassName(className))
	if class == nil {
		return nil, errors.Errorf("class %q not found in schema", className)
	}
	return class, nil
}

func (m *Provider) HasMultipleVectorizers() bool {
	return m.hasMultipleVectorizers
}
