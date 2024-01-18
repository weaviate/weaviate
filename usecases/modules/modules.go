//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

var (
	internalSearchers = []string{
		"nearObject", "nearVector", "where", "group", "limit", "offset",
		"after", "groupBy", "bm25", "hybrid",
	}
	internalAdditionalProperties = []string{"classification", "certainty", "id", "distance", "group"}
)

type Provider struct {
	registered             map[string]modulecapabilities.Module
	altNames               map[string]string
	schemaGetter           schemaGetter
	hasMultipleVectorizers bool
}

type schemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

func NewProvider() *Provider {
	return &Provider{
		registered: map[string]modulecapabilities.Module{},
		altNames:   map[string]string{},
	}
}

func (p *Provider) Register(mod modulecapabilities.Module) {
	p.registered[mod.Name()] = mod
	if modHasAltNames, ok := mod.(modulecapabilities.ModuleHasAltNames); ok {
		for _, altName := range modHasAltNames.AltNames() {
			p.altNames[altName] = mod.Name()
		}
	}
}

func (p *Provider) GetByName(name string) modulecapabilities.Module {
	if mod, ok := p.registered[name]; ok {
		return mod
	}
	if origName, ok := p.altNames[name]; ok {
		return p.registered[origName]
	}
	return nil
}

func (p *Provider) GetAll() []modulecapabilities.Module {
	out := make([]modulecapabilities.Module, len(p.registered))
	i := 0
	for _, mod := range p.registered {
		out[i] = mod
		i++
	}

	return out
}

func (p *Provider) GetAllExclude(module string) []modulecapabilities.Module {
	filtered := []modulecapabilities.Module{}
	for _, mod := range p.GetAll() {
		if mod.Name() != module {
			filtered = append(filtered, mod)
		}
	}
	return filtered
}

func (p *Provider) SetSchemaGetter(sg schemaGetter) {
	p.schemaGetter = sg
}

func (p *Provider) Init(ctx context.Context,
	params moduletools.ModuleInitParams, logger logrus.FieldLogger,
) error {
	for i, mod := range p.GetAll() {
		if err := mod.Init(ctx, params); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		} else {
			logger.WithField("action", "startup").
				WithField("module", mod.Name()).
				Debug("initialized module")
		}
	}
	for i, mod := range p.GetAll() {
		if modExtension, ok := mod.(modulecapabilities.ModuleExtension); ok {
			if err := modExtension.InitExtension(p.GetAllExclude(mod.Name())); err != nil {
				return errors.Wrapf(err, "init module extension %d (%q)", i, mod.Name())
			} else {
				logger.WithField("action", "startup").
					WithField("module", mod.Name()).
					Debug("initialized module extension")
			}
		}
	}
	for i, mod := range p.GetAll() {
		if modDependency, ok := mod.(modulecapabilities.ModuleDependency); ok {
			if err := modDependency.InitDependency(p.GetAllExclude(mod.Name())); err != nil {
				return errors.Wrapf(err, "init module dependency %d (%q)", i, mod.Name())
			} else {
				logger.WithField("action", "startup").
					WithField("module", mod.Name()).
					Debug("initialized module dependency")
			}
		}
	}
	if err := p.validate(); err != nil {
		return errors.Wrap(err, "validate modules")
	}
	if p.HasMultipleVectorizers() {
		logger.Warn("Multiple vector spaces are present, GraphQL Explore and REST API list objects endpoint module include params has been disabled as a result.")
	}
	return nil
}

func (p *Provider) validate() error {
	searchers := map[string][]string{}
	additionalGraphQLProps := map[string][]string{}
	additionalRestAPIProps := map[string][]string{}
	for _, mod := range p.GetAll() {
		if module, ok := mod.(modulecapabilities.GraphQLArguments); ok {
			allArguments := []string{}
			for paraName, argument := range module.Arguments() {
				if argument.ExtractFunction != nil {
					allArguments = append(allArguments, paraName)
				}
			}
			searchers = p.scanProperties(searchers, allArguments, mod.Name())
		}
		if module, ok := mod.(modulecapabilities.AdditionalProperties); ok {
			allAdditionalRestAPIProps, allAdditionalGrapQLProps := p.getAdditionalProps(module.AdditionalProperties())
			additionalGraphQLProps = p.scanProperties(additionalGraphQLProps,
				allAdditionalGrapQLProps, mod.Name())
			additionalRestAPIProps = p.scanProperties(additionalRestAPIProps,
				allAdditionalRestAPIProps, mod.Name())
		}
	}

	var errorMessages []string
	errorMessages = append(errorMessages,
		p.validateModules("searcher", searchers, internalSearchers)...)
	errorMessages = append(errorMessages,
		p.validateModules("graphql additional property", additionalGraphQLProps, internalAdditionalProperties)...)
	errorMessages = append(errorMessages,
		p.validateModules("rest api additional property", additionalRestAPIProps, internalAdditionalProperties)...)
	if len(errorMessages) > 0 {
		return errors.Errorf("%v", errorMessages)
	}

	return nil
}

func (p *Provider) scanProperties(result map[string][]string, properties []string, module string) map[string][]string {
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

func (p *Provider) getAdditionalProps(additionalProps map[string]modulecapabilities.AdditionalProperty) ([]string, []string) {
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

func (p *Provider) validateModules(name string, properties map[string][]string, internalProperties []string) []string {
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
			p.hasMultipleVectorizers = true
		}
		for _, moduleName := range modules {
			moduleType := p.GetByName(moduleName).Type()
			if p.moduleProvidesMultipleVectorizers(moduleType) {
				p.hasMultipleVectorizers = true
			}
		}
	}
	return errorMessages
}

func (p *Provider) moduleProvidesMultipleVectorizers(moduleType modulecapabilities.ModuleType) bool {
	return moduleType == modulecapabilities.Text2MultiVec
}

func (p *Provider) isOnlyOneModuleEnabledOfAGivenType(moduleType modulecapabilities.ModuleType) bool {
	i := 0
	for _, mod := range p.registered {
		if mod.Type() == moduleType {
			i++
		}
	}
	return i == 1
}

func (p *Provider) isVectorizerModule(moduleType modulecapabilities.ModuleType) bool {
	switch moduleType {
	case modulecapabilities.Text2Vec,
		modulecapabilities.Img2Vec,
		modulecapabilities.Multi2Vec,
		modulecapabilities.Text2MultiVec,
		modulecapabilities.Ref2Vec:
		return true
	default:
		return false
	}
}

func (p *Provider) shouldIncludeClassArgument(class *models.Class, module string,
	moduleType modulecapabilities.ModuleType,
) bool {
	if p.isVectorizerModule(moduleType) {
		return class.Vectorizer == module
	}
	if moduleConfig, ok := class.ModuleConfig.(map[string]interface{}); ok {
		existsConfigForModule := moduleConfig[module] != nil
		if existsConfigForModule {
			return true
		}
	}
	// Allow Text2Text (Generative, QnA, Summarize, NER) modules to be registered to a given class
	// only if there's no configuration present and there's only one module of a given type enabled
	return p.isOnlyOneModuleEnabledOfAGivenType(moduleType)
}

func (p *Provider) shouldCrossClassIncludeClassArgument(class *models.Class, module string,
	moduleType modulecapabilities.ModuleType,
) bool {
	if class == nil {
		return !p.HasMultipleVectorizers()
	}
	return p.shouldIncludeClassArgument(class, module, moduleType)
}

func (p *Provider) shouldIncludeArgument(schema *models.Schema, module string,
	moduleType modulecapabilities.ModuleType,
) bool {
	for _, c := range schema.Classes {
		if p.shouldIncludeClassArgument(c, module, moduleType) {
			return true
		}
	}
	return false
}

// GetArguments provides GraphQL Get arguments
func (p *Provider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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

// AggregateArguments provides GraphQL Aggregate arguments
func (p *Provider) AggregateArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.Arguments() {
					if argument.AggregateArgumentsFunction != nil {
						arguments[name] = argument.AggregateArgumentsFunction(class.Class)
					}
				}
			}
		}
	}
	return arguments
}

// ExploreArguments provides GraphQL Explore arguments
func (p *Provider) ExploreArguments(schema *models.Schema) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeArgument(schema, module.Name(), module.Type()) {
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

// CrossClassExtractSearchParams extracts GraphQL arguments from modules without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (p *Provider) CrossClassExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	return p.extractSearchParams(arguments, nil)
}

// ExtractSearchParams extracts GraphQL arguments
func (p *Provider) ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	class, err := p.getClass(className)
	if err != nil {
		return exractedParams
	}
	return p.extractSearchParams(arguments, class)
}

func (p *Provider) extractSearchParams(arguments map[string]interface{}, class *models.Class) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	for _, module := range p.GetAll() {
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type()) {
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

// CrossClassValidateSearchParam validates module parameters without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (p *Provider) CrossClassValidateSearchParam(name string, value interface{}) error {
	return p.validateSearchParam(name, value, nil)
}

// ValidateSearchParam validates module parameters
func (p *Provider) ValidateSearchParam(name string, value interface{}, className string) error {
	class, err := p.getClass(className)
	if err != nil {
		return err
	}

	return p.validateSearchParam(name, value, class)
}

func (p *Provider) validateSearchParam(name string, value interface{}, class *models.Class) error {
	for _, module := range p.GetAll() {
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) ExtractAdditionalField(className, name string, params []*ast.Argument) interface{} {
	class, err := p.getClass(className)
	if err != nil {
		return err
	}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) GetObjectAdditionalExtend(ctx context.Context,
	in *search.Result, moduleParams map[string]interface{},
) (*search.Result, error) {
	resArray, err := p.additionalExtend(ctx, search.Results{*in}, moduleParams, nil, "ObjectGet", nil)
	if err != nil {
		return nil, err
	}
	return &resArray[0], nil
}

// ListObjectsAdditionalExtend extends rest api list queries with additional properties
func (p *Provider) ListObjectsAdditionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{},
) (search.Results, error) {
	return p.additionalExtend(ctx, in, moduleParams, nil, "ObjectList", nil)
}

// GetExploreAdditionalExtend extends graphql api get queries with additional properties
func (p *Provider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet", argumentModuleParams)
}

// ListExploreAdditionalExtend extends graphql api list queries with additional properties
func (p *Provider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{},
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, nil, "ExploreList", argumentModuleParams)
}

func (p *Provider) additionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	capability string, argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	toBeExtended := in
	if len(toBeExtended) > 0 {
		class, err := p.getClassFromSearchResult(toBeExtended)
		if err != nil {
			return nil, err
		}
		allAdditionalProperties := map[string]modulecapabilities.AdditionalProperty{}
		for _, module := range p.GetAll() {
			if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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
			if err := p.checkCapabilities(allAdditionalProperties, moduleParams, capability); err != nil {
				return nil, err
			}
			cfg := NewClassBasedModuleConfig(class, "", "")
			for name, value := range moduleParams {
				additionalPropertyFn := p.getAdditionalPropertyFn(allAdditionalProperties[name], capability)
				if additionalPropertyFn != nil && value != nil {
					searchValue := value
					if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector); ok {
						searchVectorValue.SetSearchVector(searchVector)
						searchValue = searchVectorValue
					}
					resArray, err := additionalPropertyFn(ctx, toBeExtended, searchValue, nil, argumentModuleParams, cfg)
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

func (p *Provider) getClassFromSearchResult(in []search.Result) (*models.Class, error) {
	if len(in) > 0 {
		return p.getClass(in[0].ClassName)
	}
	return nil, errors.Errorf("unknown class")
}

func (p *Provider) checkCapabilities(additionalProperties map[string]modulecapabilities.AdditionalProperty,
	moduleParams map[string]interface{}, capability string,
) error {
	for name := range moduleParams {
		additionalPropertyFn := p.getAdditionalPropertyFn(additionalProperties[name], capability)
		if additionalPropertyFn == nil {
			return errors.Errorf("unknown capability: %s", name)
		}
	}
	return nil
}

func (p *Provider) getAdditionalPropertyFn(
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
func (p *Provider) GraphQLAdditionalFieldNames() []string {
	additionalPropertiesNames := []string{}
	for _, module := range p.GetAll() {
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
func (p *Provider) RestApiAdditionalProperties(includeProp string, class *models.Class) map[string]interface{} {
	moduleParams := map[string]interface{}{}
	for _, module := range p.GetAll() {
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) VectorFromSearchParam(ctx context.Context,
	className string, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn, tenant string,
) ([]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}

	for _, mod := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, mod.Name(), mod.Type()) {
			var moduleName string
			var vectorSearches modulecapabilities.ArgumentVectorForParams
			if searcher, ok := mod.(modulecapabilities.Searcher); ok {
				moduleName = mod.Name()
				vectorSearches = searcher.VectorSearches()
			} else if searchers, ok := mod.(modulecapabilities.DependencySearcher); ok {
				if dependencySearchers := searchers.VectorSearches(); dependencySearchers != nil {
					moduleName = class.Vectorizer
					vectorSearches = dependencySearchers[class.Vectorizer]
				}
			}
			if vectorSearches != nil {
				if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
					cfg := NewClassBasedModuleConfig(class, moduleName, tenant)
					vector, err := searchVectorFn(ctx, params, class.Class, findVectorFn, cfg)
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
func (p *Provider) CrossClassVectorFromSearchParam(ctx context.Context,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn,
) ([]float32, error) {
	for _, mod := range p.GetAll() {
		if searcher, ok := mod.(modulecapabilities.Searcher); ok {
			if vectorSearches := searcher.VectorSearches(); vectorSearches != nil {
				if searchVectorFn := vectorSearches[param]; searchVectorFn != nil {
					cfg := NewCrossClassModuleConfig()
					vector, err := searchVectorFn(ctx, params, "", findVectorFn, cfg)
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

func (p *Provider) VectorFromInput(ctx context.Context,
	className string, input string,
) ([]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}

	for _, mod := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, mod.Name(), mod.Type()) {
			if vectorizer, ok := mod.(modulecapabilities.InputVectorizer); ok {
				// does not access any objects, therefore tenant is irrelevant
				cfg := NewClassBasedModuleConfig(class, mod.Name(), "")
				return vectorizer.VectorizeInput(ctx, input, cfg)
			}
		}
	}

	return nil, fmt.Errorf("VectorFromInput was called without vectorizer")
}

// ParseClassifierSettings parses and adds classifier specific settings
func (p *Provider) ParseClassifierSettings(name string,
	params *models.Classification,
) error {
	class, err := p.getClass(params.Class)
	if err != nil {
		return err
	}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) GetClassificationFn(className, name string,
	params modulecapabilities.ClassifyParams,
) (modulecapabilities.ClassifyItemFn, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type()) {
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
func (p *Provider) GetMeta() (map[string]interface{}, error) {
	metaInfos := map[string]interface{}{}
	for _, module := range p.GetAll() {
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

func (p *Provider) getClass(className string) (*models.Class, error) {
	sch := p.schemaGetter.GetSchemaSkipAuth()
	class := sch.FindClassByName(schema.ClassName(className))
	if class == nil {
		return nil, errors.Errorf("class %q not found in schema", className)
	}
	return class, nil
}

func (p *Provider) HasMultipleVectorizers() bool {
	return p.hasMultipleVectorizers
}

func (p *Provider) BackupBackend(backend string) (modulecapabilities.BackupBackend, error) {
	if module := p.GetByName(backend); module != nil {
		if module.Type() == modulecapabilities.Backup {
			if backend, ok := module.(modulecapabilities.BackupBackend); ok {
				return backend, nil
			}
		}
	}
	return nil, errors.Errorf("backup: %s not found", backend)
}
