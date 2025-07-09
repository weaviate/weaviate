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
	"regexp"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

var (
	internalSearchers = []string{
		"nearObject", "nearVector", "where", "group", "limit", "offset",
		"after", "groupBy", "bm25", "hybrid",
	}
	internalAdditionalProperties = []string{"classification", "certainty", "id", "distance", "group"}
)

type Provider struct {
	vectorsLock               sync.RWMutex
	registered                map[string]modulecapabilities.Module
	altNames                  map[string]string
	schemaGetter              schemaGetter
	hasMultipleVectorizers    bool
	targetVectorNameValidator *regexp.Regexp
	logger                    logrus.FieldLogger
	cfg                       config.Config
}

type schemaGetter interface {
	ReadOnlyClass(name string) *models.Class
}

func NewProvider(logger logrus.FieldLogger, cfg config.Config) *Provider {
	return &Provider{
		registered:                map[string]modulecapabilities.Module{},
		altNames:                  map[string]string{},
		targetVectorNameValidator: regexp.MustCompile(`^` + schema.TargetVectorNameRegex + `$`),
		logger:                    logger,
		cfg:                       cfg,
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
	return moduleType == modulecapabilities.Text2ManyVec
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

func (p *Provider) IsGenerative(modName string) bool {
	mod := p.GetByName(modName)
	if mod == nil {
		return false
	}
	return mod.Type() == modulecapabilities.Text2TextGenerative
}

func (p *Provider) IsReranker(modName string) bool {
	mod := p.GetByName(modName)
	if mod == nil {
		return false
	}
	return mod.Type() == modulecapabilities.Text2TextReranker
}

func (p *Provider) IsMultiVector(modName string) bool {
	mod := p.GetByName(modName)
	if mod == nil {
		return false
	}
	return mod.Type() == modulecapabilities.Text2Multivec
}

func (p *Provider) isVectorizerModule(moduleType modulecapabilities.ModuleType) bool {
	switch moduleType {
	case modulecapabilities.Text2Vec,
		modulecapabilities.Img2Vec,
		modulecapabilities.Multi2Vec,
		modulecapabilities.Text2ManyVec,
		modulecapabilities.Ref2Vec,
		modulecapabilities.Text2Multivec:
		return true
	default:
		return false
	}
}

func (p *Provider) isGenerativeModule(moduleType modulecapabilities.ModuleType) bool {
	return moduleType == modulecapabilities.Text2TextGenerative
}

func (p *Provider) shouldIncludeClassArgument(class *models.Class, module string,
	moduleType modulecapabilities.ModuleType, altNames []string,
) bool {
	if p.isVectorizerModule(moduleType) {
		for _, vectorConfig := range class.VectorConfig {
			if vectorizer, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok {
				if _, ok := vectorizer[module]; ok {
					return true
				} else if len(altNames) > 0 {
					for _, altName := range altNames {
						if _, ok := vectorizer[altName]; ok {
							return true
						}
					}
				}
			}
		}
		for _, altName := range altNames {
			if class.Vectorizer == altName {
				return true
			}
		}
		return class.Vectorizer == module
	}
	if moduleConfig, ok := class.ModuleConfig.(map[string]interface{}); ok {
		if _, ok := moduleConfig[module]; ok {
			return true
		} else if len(altNames) > 0 {
			for _, altName := range altNames {
				if _, ok := moduleConfig[altName]; ok {
					return true
				}
			}
		}
	}
	// Allow Text2Text (QnA, Generative, Summarize, NER) modules to be registered to a given class
	// only if there's no configuration present and there's only one module of a given type enabled
	return p.isOnlyOneModuleEnabledOfAGivenType(moduleType)
}

func (p *Provider) shouldCrossClassIncludeClassArgument(class *models.Class, module string,
	moduleType modulecapabilities.ModuleType, altNames []string,
) bool {
	if class == nil {
		return !p.HasMultipleVectorizers()
	}
	return p.shouldIncludeClassArgument(class, module, moduleType, altNames)
}

func (p *Provider) shouldIncludeArgument(schema *models.Schema, module string,
	moduleType modulecapabilities.ModuleType, altNames []string,
) bool {
	for _, c := range schema.Classes {
		if p.shouldIncludeClassArgument(c, module, moduleType, altNames) {
			return true
		}
	}
	return false
}

func (p *Provider) shouldAddGenericArgument(class *models.Class, moduleType modulecapabilities.ModuleType) bool {
	if p.isGenerativeModule(moduleType) {
		return true
	}
	return p.hasMultipleVectorizersConfig(class) && p.isVectorizerModule(moduleType)
}

func (p *Provider) hasMultipleVectorizersConfig(class *models.Class) bool {
	return len(class.VectorConfig) > 0
}

func (p *Provider) shouldCrossClassAddGenericArgument(schema *models.Schema, moduleType modulecapabilities.ModuleType) bool {
	for _, c := range schema.Classes {
		if p.shouldAddGenericArgument(c, moduleType) {
			return true
		}
	}
	return false
}

func (p *Provider) getGenericArgument(name, className string,
	argumentType modulecomponents.ArgumentType,
) *graphql.ArgumentConfig {
	var nearTextTransformer modulecapabilities.TextTransform
	if name == "nearText" {
		// nearText argument might be exposed with an extension, we need to check
		// if text transformers module is enabled if so then we need to init nearText
		// argument with this extension
		for _, mod := range p.GetAll() {
			if arg, ok := mod.(modulecapabilities.TextTransformers); ok {
				if arg != nil && arg.TextTransformers() != nil {
					nearTextTransformer = arg.TextTransformers()["nearText"]
					break
				}
			}
		}
	}
	return modulecomponents.GetGenericArgument(name, className, argumentType, nearTextTransformer)
}

func (p *Provider) getGenericAdditionalProperty(name string, class *models.Class) *modulecapabilities.AdditionalProperty {
	if p.hasMultipleVectorizersConfig(class) {
		return modulecomponents.GetGenericAdditionalProperty(name, class.Class)
	}
	return nil
}

// GetArguments provides GraphQL Get arguments
func (p *Provider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.Arguments() {
					if argument.GetArgumentsFunction != nil {
						if p.shouldAddGenericArgument(class, module.Type()) {
							if _, ok := arguments[name]; !ok {
								arguments[name] = p.getGenericArgument(name, class.Class, modulecomponents.Get)
							}
						} else {
							arguments[name] = argument.GetArgumentsFunction(class.Class)
						}
					}
				}
			}
		}
	}

	return arguments
}

func (p *Provider) getModuleAltNames(module modulecapabilities.Module) []string {
	if moduleWithAltNames, ok := module.(modulecapabilities.ModuleHasAltNames); ok {
		return moduleWithAltNames.AltNames()
	}
	return nil
}

func (p *Provider) isModuleNameEqual(module modulecapabilities.Module, targetModule string) bool {
	if module.Name() == targetModule {
		return true
	}
	if altNames := p.getModuleAltNames(module); len(altNames) > 0 {
		if slices.Contains(altNames, targetModule) {
			return true
		}
	}
	return false
}

// AggregateArguments provides GraphQL Aggregate arguments
func (p *Provider) AggregateArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	arguments := map[string]*graphql.ArgumentConfig{}
	for _, module := range p.GetAll() {
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.Arguments() {
					if argument.AggregateArgumentsFunction != nil {
						if p.shouldAddGenericArgument(class, module.Type()) {
							if _, ok := arguments[name]; !ok {
								arguments[name] = p.getGenericArgument(name, class.Class, modulecomponents.Aggregate)
							}
						} else {
							arguments[name] = argument.AggregateArgumentsFunction(class.Class)
						}
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
		if p.shouldIncludeArgument(schema, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if arg, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for name, argument := range arg.Arguments() {
					if argument.ExploreArgumentsFunction != nil {
						if p.shouldCrossClassAddGenericArgument(schema, module.Type()) {
							if _, ok := arguments[name]; !ok {
								arguments[name] = p.getGenericArgument(name, "", modulecomponents.Explore)
							}
						} else {
							arguments[name] = argument.ExploreArgumentsFunction()
						}
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
	// explore does not support target vectors
	params, _ := p.extractSearchParams(arguments, nil)
	return params
}

// ExtractSearchParams extracts GraphQL arguments
func (p *Provider) ExtractSearchParams(arguments map[string]interface{}, className string) (map[string]interface{}, map[string]*dto.TargetCombination) {
	class, err := p.getClass(className)
	if err != nil {
		return map[string]interface{}{}, nil
	}
	return p.extractSearchParams(arguments, class)
}

func (p *Provider) extractSearchParams(arguments map[string]interface{}, class *models.Class) (map[string]interface{}, map[string]*dto.TargetCombination) {
	exractedParams := map[string]interface{}{}
	exractedCombination := map[string]*dto.TargetCombination{}
	for _, module := range p.GetAll() {
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for paramName, argument := range args.Arguments() {
					if param, ok := arguments[paramName]; ok && argument.ExtractFunction != nil {
						extracted, combination, err := argument.ExtractFunction(param.(map[string]interface{}))
						if err != nil {
							continue
						}
						exractedParams[paramName] = extracted
						exractedCombination[paramName] = combination
					}
				}
			}
		}
	}
	return exractedParams, exractedCombination
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
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if args, ok := module.(modulecapabilities.GraphQLArguments); ok {
				for paramName, argument := range args.Arguments() {
					if paramName == name && argument.ValidateFunction != nil {
						return argument.ValidateFunction(value)
					}
				}
			}
		}
	}

	return fmt.Errorf("could not vectorize input for collection %v with search-type %v. Make sure a vectorizer module is configured for this collection", class.Class, name)
}

// GetAdditionalFields provides GraphQL Get additional fields
func (p *Provider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	additionalGenerativeDefaultProvider := ""
	additionalGenerativeParameters := map[string]modulecapabilities.GenerativeProperty{}
	for _, module := range p.GetAll() {
		if p.isGenerativeModule(module.Type()) {
			if arg, ok := module.(modulecapabilities.AdditionalGenerativeProperties); ok {
				for name, additionalGenerativeParameter := range arg.AdditionalGenerativeProperties() {
					additionalGenerativeParameters[name] = additionalGenerativeParameter
					if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
						additionalGenerativeDefaultProvider = name
					}
				}
			}
		} else if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
				for name, additionalProperty := range arg.AdditionalProperties() {
					if additionalProperty.GraphQLFieldFunction != nil {
						if genericAdditionalProperty := p.getGenericAdditionalProperty(name, class); genericAdditionalProperty != nil {
							if genericAdditionalProperty.GraphQLFieldFunction != nil {
								if _, ok := additionalProperties[name]; !ok {
									additionalProperties[name] = genericAdditionalProperty.GraphQLFieldFunction(class.Class)
								}
							}
						} else {
							additionalProperties[name] = additionalProperty.GraphQLFieldFunction(class.Class)
						}
					}
				}
			}
		}
	}
	if len(additionalGenerativeParameters) > 0 {
		if generateFn := modulecomponents.GetGenericGenerateProperty(class.Class, additionalGenerativeParameters, additionalGenerativeDefaultProvider, p.logger); generateFn != nil {
			additionalProperties[modulecomponents.AdditionalPropertyGenerate] = generateFn.GraphQLFieldFunction(class.Class)
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
	additionalGenerativeDefaultProvider := ""
	additionalGenerativeParameters := map[string]modulecapabilities.GenerativeProperty{}
	for _, module := range p.GetAll() {
		if name == modulecomponents.AdditionalPropertyGenerate {
			if p.isGenerativeModule(module.Type()) {
				if arg, ok := module.(modulecapabilities.AdditionalGenerativeProperties); ok {
					for name, additionalGenerativeParameter := range arg.AdditionalGenerativeProperties() {
						additionalGenerativeParameters[name] = additionalGenerativeParameter
						if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
							additionalGenerativeDefaultProvider = name
						}
					}
				}
			}
		} else if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
			if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
				if additionalProperties := arg.AdditionalProperties(); len(additionalProperties) > 0 {
					if additionalProperty, ok := additionalProperties[name]; ok {
						return additionalProperty.GraphQLExtractFunction(params, class)
					}
				}
			}
		}
	}
	if name == modulecomponents.AdditionalPropertyGenerate {
		if generateFn := modulecomponents.GetGenericGenerateProperty(class.Class, additionalGenerativeParameters, additionalGenerativeDefaultProvider, p.logger); generateFn != nil {
			return generateFn.GraphQLExtractFunction(params, class)
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
	moduleParams map[string]interface{}, searchVector models.Vector,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet", argumentModuleParams)
}

// ListExploreAdditionalExtend extends graphql api list queries with additional properties
func (p *Provider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result, moduleParams map[string]interface{}, argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, nil, "ExploreList", argumentModuleParams)
}

func (p *Provider) additionalExtend(ctx context.Context, in []search.Result, moduleParams map[string]interface{}, searchVector models.Vector, capability string, argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	toBeExtended := in
	if len(toBeExtended) > 0 {
		class, err := p.getClassFromSearchResult(toBeExtended)
		if err != nil {
			return nil, err
		}

		additionalGenerativeDefaultProvider := ""
		additionalGenerativeParameters := map[string]modulecapabilities.GenerativeProperty{}
		allAdditionalProperties := map[string]modulecapabilities.AdditionalProperty{}
		for _, module := range p.GetAll() {
			if p.isGenerativeModule(module.Type()) {
				if arg, ok := module.(modulecapabilities.AdditionalGenerativeProperties); ok {
					for name, additionalGenerativeParameter := range arg.AdditionalGenerativeProperties() {
						additionalGenerativeParameters[name] = additionalGenerativeParameter
						if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
							additionalGenerativeDefaultProvider = name
						}
					}
				}
			} else if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
				if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
					if arg != nil && arg.AdditionalProperties() != nil {
						for name, additionalProperty := range arg.AdditionalProperties() {
							allAdditionalProperties[name] = additionalProperty
						}
					}
				}
			}
		}
		if len(additionalGenerativeParameters) > 0 {
			if generateFn := modulecomponents.GetGenericGenerateProperty(class.Class, additionalGenerativeParameters, additionalGenerativeDefaultProvider, p.logger); generateFn != nil {
				allAdditionalProperties[modulecomponents.AdditionalPropertyGenerate] = *generateFn
			}
		}

		if len(allAdditionalProperties) > 0 {
			if err := p.checkCapabilities(allAdditionalProperties, moduleParams, capability); err != nil {
				return nil, err
			}
			cfg := NewClassBasedModuleConfig(class, "", "", "")
			for name, value := range moduleParams {
				additionalPropertyFn := p.getAdditionalPropertyFn(allAdditionalProperties[name], capability)
				if additionalPropertyFn != nil && value != nil {
					searchValue := value
					if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector[[]float32]); ok {
						if vec, ok := searchVector.([]float32); ok {
							searchVectorValue.SetSearchVector(vec)
							searchValue = searchVectorValue
						} else {
							return nil, errors.Errorf("extend %s: set search vector unrecongnized type: %T", name, searchVector)
						}
					} else if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector[[][]float32]); ok {
						if vec, ok := searchVector.([][]float32); ok {
							searchVectorValue.SetSearchVector(vec)
							searchValue = searchVectorValue
						} else {
							return nil, errors.Errorf("extend %s: set search multi vector unrecongnized type: %T", name, searchVector)
						}
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
	additionalPropertiesNames := map[string]struct{}{}
	for _, module := range p.GetAll() {
		if arg, ok := module.(modulecapabilities.AdditionalProperties); ok {
			for _, additionalProperty := range arg.AdditionalProperties() {
				for _, gqlName := range additionalProperty.GraphQLNames {
					additionalPropertiesNames[gqlName] = struct{}{}
				}
			}
		} else if _, ok := module.(modulecapabilities.AdditionalGenerativeProperties); ok {
			additionalPropertiesNames[modulecomponents.AdditionalPropertyGenerate] = struct{}{}
		}
	}
	var availableAdditionalPropertiesNames []string
	for gqlName := range additionalPropertiesNames {
		availableAdditionalPropertiesNames = append(availableAdditionalPropertiesNames, gqlName)
	}
	return availableAdditionalPropertiesNames
}

// RestApiAdditionalProperties get's all rest specific additional properties with their
// default values
func (p *Provider) RestApiAdditionalProperties(includeProp string, class *models.Class) map[string]interface{} {
	moduleParams := map[string]interface{}{}
	for _, module := range p.GetAll() {
		if p.shouldCrossClassIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
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

func (p *Provider) TargetsFromSearchParam(className string, params interface{}) ([]string, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}
	targetVectors, err := p.getTargetVector(class, params)
	if err != nil {
		return nil, err
	}

	return targetVectors, nil
}

func (p *Provider) IsTargetVectorMultiVector(className, targetVector string) (bool, error) {
	class, err := p.getClass(className)
	if err != nil {
		return false, err
	}
	targetModule := p.getModuleNameForTargetVector(class, targetVector)
	return p.IsMultiVector(targetModule), nil
}

// VectorFromSearchParam gets a vector for a given argument. This is used in
// Get { Class() } for example
func (p *Provider) VectorFromSearchParam(ctx context.Context, className, targetVector, tenant, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn[[]float32],
) ([]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}

	targetModule := p.getModuleNameForTargetVector(class, targetVector)

	for _, mod := range p.GetAll() {
		if found, vector, err := vectorFromSearchParam(ctx, class, mod, targetModule, targetVector, tenant, param, params, findVectorFn, p.isModuleNameEqual); found {
			return vector, err
		}
	}

	return nil, fmt.Errorf("could not vectorize input for collection %v with search-type %v, targetVector %v and parameters %v. Make sure a vectorizer module is configured for this class", className, param, targetVector, params)
}

// MultiVectorFromSearchParam gets a multi vector for a given argument. This is used in
// Get { Class() } for example
func (p *Provider) MultiVectorFromSearchParam(ctx context.Context, className, targetVector, tenant, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn[[][]float32],
) ([][]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}

	targetModule := p.getModuleNameForTargetVector(class, targetVector)

	for _, mod := range p.GetAll() {
		if found, vector, err := vectorFromSearchParam(ctx, class, mod, targetModule, targetVector, tenant, param, params, findVectorFn, p.isModuleNameEqual); found {
			return vector, err
		}
	}

	return nil, fmt.Errorf("could not vectorize input for collection %v with search-type %v, targetVector %v and parameters %v. Make sure a vectorizer module is configured for this class", className, param, targetVector, params)
}

// CrossClassVectorFromSearchParam gets a vector for a given argument without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (p *Provider) CrossClassVectorFromSearchParam(ctx context.Context,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn[[]float32],
) ([]float32, string, error) {
	for _, mod := range p.GetAll() {
		if found, vector, targetVector, err := crossClassVectorFromSearchParam(ctx, mod, param, params, findVectorFn, p.getTargetVector); found {
			return vector, targetVector, err
		}
	}

	return nil, "", fmt.Errorf("could not vectorize input for Explore with search-type %v and parameters %v. Make sure a vectorizer module is configured", param, params)
}

// MultiCrossClassVectorFromSearchParam gets a multi vector for a given argument without
// being specific to any one class and it's configuration. This is used in
// Explore() { } for example
func (p *Provider) MultiCrossClassVectorFromSearchParam(ctx context.Context,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn[[][]float32],
) ([][]float32, string, error) {
	for _, mod := range p.GetAll() {
		if found, vector, targetVector, err := crossClassVectorFromSearchParam(ctx, mod, param, params, findVectorFn, p.getTargetVector); found {
			return vector, targetVector, err
		}
	}

	return nil, "", fmt.Errorf("could not vectorize input for Explore with search-type %v and parameters %v. Make sure a vectorizer module is configured", param, params)
}

func (p *Provider) getTargetVector(class *models.Class, params interface{}) ([]string, error) {
	if nearParam, ok := params.(modulecapabilities.NearParam); ok && len(nearParam.GetTargetVectors()) >= 1 {
		return nearParam.GetTargetVectors(), nil
	}
	if class != nil {
		if modelsext.ClassHasLegacyVectorIndex(class) {
			return []string{""}, nil
		}

		if len(class.VectorConfig) > 1 {
			return nil, fmt.Errorf("multiple vectorizers configuration found, please specify target vector name")
		}

		if len(class.VectorConfig) == 1 {
			for name := range class.VectorConfig {
				return []string{name}, nil
			}
		}
	}
	return []string{""}, nil
}

func (p *Provider) getModuleNameForTargetVector(class *models.Class, targetVector string) string {
	if len(class.VectorConfig) > 0 {
		if vectorConfig, ok := class.VectorConfig[targetVector]; ok {
			if vectorizer, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok && len(vectorizer) == 1 {
				for moduleName := range vectorizer {
					return moduleName
				}
			}
		}
	}
	return class.Vectorizer
}

func (p *Provider) VectorFromInput(ctx context.Context,
	className, input, targetVector string,
) ([]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}
	targetModule := p.getModuleNameForTargetVector(class, targetVector)

	for _, mod := range p.GetAll() {
		if p.isModuleNameEqual(mod, targetModule) {
			if p.shouldIncludeClassArgument(class, mod.Name(), mod.Type(), p.getModuleAltNames(mod)) {
				if found, vector, err := vectorFromInput[[]float32](ctx, mod, class, input, targetVector); found {
					return vector, err
				}
			}
		}
	}

	return nil, fmt.Errorf("VectorFromInput was called without vectorizer on class %v for input %v", className, input)
}

func (p *Provider) MultiVectorFromInput(ctx context.Context,
	className, input, targetVector string,
) ([][]float32, error) {
	class, err := p.getClass(className)
	if err != nil {
		return nil, err
	}
	targetModule := p.getModuleNameForTargetVector(class, targetVector)

	for _, mod := range p.GetAll() {
		if p.isModuleNameEqual(mod, targetModule) {
			if p.shouldIncludeClassArgument(class, mod.Name(), mod.Type(), p.getModuleAltNames(mod)) {
				if found, vector, err := vectorFromInput[[][]float32](ctx, mod, class, input, targetVector); found {
					return vector, err
				}
			}
		}
	}

	return nil, fmt.Errorf("MultiVectorFromInput was called without vectorizer on class %v for input %v", className, input)
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
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
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
		if p.shouldIncludeClassArgument(class, module.Name(), module.Type(), p.getModuleAltNames(module)) {
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
				metaInfos[module.Name()] = map[string]interface{}{
					"error": err.Error(),
				}
			} else {
				metaInfos[module.Name()] = meta
			}
		}
	}
	return metaInfos, nil
}

func (p *Provider) getClass(className string) (*models.Class, error) {
	class := p.schemaGetter.ReadOnlyClass(className)
	if class == nil {
		return nil, errors.Errorf("class %q not found in schema", className)
	}
	return class, nil
}

func (p *Provider) HasMultipleVectorizers() bool {
	return p.hasMultipleVectorizers
}

func (p *Provider) BackupBackend(backend string) (modulecapabilities.BackupBackend, error) {
	module := p.GetByName(backend)
	if module != nil {
		if module.Type() == modulecapabilities.Backup {
			module_backend, ok := module.(modulecapabilities.BackupBackend)
			if ok {
				return module_backend, nil
			} else {
				return nil, errors.Errorf("backup: %s is not a backup backend (actual type: %T)", backend, module)
			}
		} else {
			return nil, errors.Errorf("backup: %s is not a backup backend type", backend)
		}
	}
	return nil, errors.Errorf("backup: %s not found", backend)
}

func (p *Provider) OffloadBackend(backend string) (modulecapabilities.OffloadCloud, bool) {
	if module := p.GetByName(backend); module != nil {
		if module.Type() == modulecapabilities.Offload {
			if backend, ok := module.(modulecapabilities.OffloadCloud); ok {
				return backend, true
			}
		}
	}
	return nil, false
}

func (p *Provider) EnabledBackupBackends() []modulecapabilities.BackupBackend {
	var backends []modulecapabilities.BackupBackend
	for _, mod := range p.GetAll() {
		if backend, ok := mod.(modulecapabilities.BackupBackend); ok &&
			mod.Type() == modulecapabilities.Backup {
			backends = append(backends, backend)
		}
	}
	return backends
}
