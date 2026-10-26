//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package get

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	moduleadditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/language/ast"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

func (b *classBuilder) primitiveField(propertyType schema.PropertyDataType,
	property *models.Property, className string,
) *graphql.Field {
	switch propertyType.AsPrimitive() {
	case schema.DataTypeText:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String,
		}
	case schema.DataTypeInt:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Int,
		}
	case schema.DataTypeNumber:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Float,
		}
	case schema.DataTypeBoolean:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.Boolean,
		}
	case schema.DataTypeDate:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String, // String since no graphql date datatype exists
		}
	case schema.DataTypeGeoCoordinates:
		obj := newGeoCoordinatesObject(className, property.Name)

		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        obj,
			Resolve:     resolveGeoCoordinates,
		}
	case schema.DataTypePhoneNumber:
		obj := newPhoneNumberObject(className, property.Name)

		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        obj,
			Resolve:     resolvePhoneNumber,
		}
	case schema.DataTypeBlob, schema.DataTypeBlobHash:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String,
		}
	case schema.DataTypeTextArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.String),
		}
	case schema.DataTypeIntArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.Int),
		}
	case schema.DataTypeNumberArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.Float),
		}
	case schema.DataTypeBooleanArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.Boolean),
		}
	case schema.DataTypeDateArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.String), // String since no graphql date datatype exists
		}
	case schema.DataTypeUUIDArray:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.NewList(graphql.String), // Always return UUID as string representation to the user
		}
	case schema.DataTypeUUID:
		return &graphql.Field{
			Description: property.Description,
			Name:        property.Name,
			Type:        graphql.String, // Always return UUID as string representation to the user
		}
	default:
		panic(fmt.Sprintf("buildGetClass: unknown primitive type for %s.%s; %s",
			className, property.Name, propertyType.AsPrimitive()))
	}
}

func newGeoCoordinatesObject(className string, propertyName string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Description: "GeoCoordinates as latitude and longitude in decimal form",
		Name:        fmt.Sprintf("%s%sGeoCoordinatesObj", className, propertyName),
		Fields: graphql.Fields{
			"latitude": &graphql.Field{
				Name:        "Latitude",
				Description: "The Latitude of the point in decimal form.",
				Type:        graphql.Float,
			},
			"longitude": &graphql.Field{
				Name:        "Longitude",
				Description: "The Longitude of the point in decimal form.",
				Type:        graphql.Float,
			},
		},
	})
}

func newPhoneNumberObject(className string, propertyName string) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Description: "PhoneNumber in various parsed formats",
		Name:        fmt.Sprintf("%s%sPhoneNumberObj", className, propertyName),
		Fields: graphql.Fields{
			"input": &graphql.Field{
				Name:        "Input",
				Description: "The raw phone number as put in by the user prior to parsing",
				Type:        graphql.String,
			},
			"internationalFormatted": &graphql.Field{
				Name:        "Input",
				Description: "The parsed phone number in the international format",
				Type:        graphql.String,
			},
			"nationalFormatted": &graphql.Field{
				Name:        "Input",
				Description: "The parsed phone number in the national format",
				Type:        graphql.String,
			},
			"national": &graphql.Field{
				Name:        "Input",
				Description: "The parsed phone number in the national format",
				Type:        graphql.Int,
			},
			"valid": &graphql.Field{
				Name:        "Input",
				Description: "Whether the phone number could be successfully parsed and was considered valid by the parser",
				Type:        graphql.Boolean,
			},
			"countryCode": &graphql.Field{
				Name:        "Input",
				Description: "The parsed country code, i.e. the leading numbers identifing the country in an international format",
				Type:        graphql.Int,
			},
			"defaultCountry": &graphql.Field{
				Name:        "Input",
				Description: "The defaultCountry as put in by the user. (This is used to help parse national numbers into an international format)",
				Type:        graphql.String,
			},
		},
	})
}

func buildGetClassField(classObject *graphql.Object,
	class *models.Class, modulesProvider ModulesProvider, fusionEnum *graphql.Enum, authorizer authorization.Authorizer,
) graphql.Field {
	field := graphql.Field{
		Type:        graphql.NewList(classObject),
		Description: class.Description,
		Args: graphql.FieldConfigArgument{
			"after": &graphql.ArgumentConfig{
				Description: descriptions.AfterID,
				Type:        graphql.String,
			},
			"limit": &graphql.ArgumentConfig{
				Description: descriptions.Limit,
				Type:        graphql.Int,
			},
			"offset": &graphql.ArgumentConfig{
				Description: descriptions.After,
				Type:        graphql.Int,
			},
			"autocut": &graphql.ArgumentConfig{
				Description: "Cut off number of results after the Nth extrema. Off by default, negative numbers mean off.",
				Type:        graphql.Int,
			},

			"sort":       sortArgument(class.Class),
			"nearVector": nearVectorArgument(class.Class),
			"nearObject": nearObjectArgument(class.Class),
			"where":      whereArgument(class.Class),
			"group":      groupArgument(class.Class),
			"groupBy":    groupByArgument(class.Class),
		},
		Resolve: newResolver(authorizer, modulesProvider).makeResolveGetClass(class.Class),
	}

	field.Args["bm25"] = bm25Argument(class.Class)
	field.Args["hybrid"] = hybridArgument(classObject, class, modulesProvider, fusionEnum)

	if modulesProvider != nil {
		for name, argument := range modulesProvider.GetArguments(class) {
			field.Args[name] = argument
		}
	}

	if replicationEnabled(class) {
		field.Args["consistencyLevel"] = consistencyLevelArgument(class)
	}

	if schema.MultiTenancyEnabled(class) {
		field.Args["tenant"] = tenantArgument()
	}

	return field
}

func resolveGeoCoordinates(p graphql.ResolveParams) (interface{}, error) {
	field := p.Source.(map[string]interface{})[p.Info.FieldName]
	if field == nil {
		return nil, nil
	}

	geo, ok := field.(*models.GeoCoordinates)
	if !ok {
		return nil, fmt.Errorf("expected a *models.GeoCoordinates, but got: %T", field)
	}

	return map[string]interface{}{
		"latitude":  geo.Latitude,
		"longitude": geo.Longitude,
	}, nil
}

func resolvePhoneNumber(p graphql.ResolveParams) (interface{}, error) {
	field := p.Source.(map[string]interface{})[p.Info.FieldName]
	if field == nil {
		return nil, nil
	}

	phone, ok := field.(*models.PhoneNumber)
	if !ok {
		return nil, fmt.Errorf("expected a *models.PhoneNumber, but got: %T", field)
	}

	return map[string]interface{}{
		"input":                  phone.Input,
		"internationalFormatted": phone.InternationalFormatted,
		"nationalFormatted":      phone.NationalFormatted,
		"national":               phone.National,
		"valid":                  phone.Valid,
		"countryCode":            phone.CountryCode,
		"defaultCountry":         phone.DefaultCountry,
	}, nil
}

func whereArgument(className string) *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: descriptions.GetWhere,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("GetObjects%sWhereInpObj", className),
				Fields:      common_filters.BuildNew(fmt.Sprintf("GetObjects%s", className)),
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

type resolver struct {
	authorizer      authorization.Authorizer
	modulesProvider ModulesProvider
}

func newResolver(authorizer authorization.Authorizer, modulesProvider ModulesProvider) *resolver {
	return &resolver{authorizer, modulesProvider}
}

func (r *resolver) makeResolveGetClass(className string) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		result, err := r.resolveGet(p, className)
		if err != nil {
			return result, enterrors.NewErrGraphQLUser(err, "Get", className)
		}
		return result, nil
	}
}

func (r *resolver) resolveGet(p graphql.ResolveParams, className string) (interface{}, error) {
	principal := restCtx.GetPrincipalFromContext(p.Context)

	source, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected graphql root to be a map, but was %T", p.Source)
	}

	resolver, ok := source["Resolver"].(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source map to have a usable Resolver, but got %#v", source["Resolver"])
	}

	var tenant string
	if tk, ok := p.Args["tenant"]; ok {
		tenant = tk.(string)
	}

	if err := r.authorizer.Authorize(p.Context, principal, authorization.READ, authorization.ShardsData(className, tenant)...); err != nil {
		return nil, err
	}

	pagination, err := filters.ExtractPaginationFromArgs(p.Args)
	if err != nil {
		return nil, err
	}

	cursor, err := filters.ExtractCursorFromArgs(p.Args)
	if err != nil {
		return nil, err
	}

	// There can only be exactly one ast.Field; it is the class name.
	if len(p.Info.FieldASTs) != 1 {
		panic("Only one Field expected here")
	}

	selectionsOfClass := p.Info.FieldASTs[0].SelectionSet

	properties, addlProps, groupByProperties, err := extractProperties(
		className, selectionsOfClass, &p.Info, r.modulesProvider)
	if err != nil {
		return nil, err
	}
	allPropsToAuthorize := append(properties, groupByProperties...)
	for _, property := range allPropsToAuthorize {
		if err := common_filters.AuthorizeProperty(p.Context, r.authorizer, &property, principal); err != nil {
			return nil, err
		}
	}

	var sort []filters.Sort
	if sortArg, ok := p.Args["sort"]; ok {
		sort = filters.ExtractSortFromArgs(sortArg.([]interface{}))
	}

	filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
	if err != nil {
		return nil, fmt.Errorf("could not extract filters: %w", err)
	}
	if filters != nil {
		if err = common_filters.AuthorizeFilters(p.Context, r.authorizer, filters.Root, principal); err != nil {
			return nil, err
		}
	}

	var targetVectorCombination *dto.TargetCombination
	var nearVectorParams *searchparams.NearVector
	if nearVector, ok := p.Args["nearVector"]; ok {
		p, targetCombination, err := common_filters.ExtractNearVector(nearVector.(map[string]interface{}), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to extract nearVector params: %w", err)
		}
		nearVectorParams = &p
		targetVectorCombination = targetCombination
	}

	var nearObjectParams *searchparams.NearObject
	if nearObject, ok := p.Args["nearObject"]; ok {
		p, targetCombination, err := common_filters.ExtractNearObject(nearObject.(map[string]interface{}))
		if err != nil {
			return nil, fmt.Errorf("failed to extract nearObject params: %w", err)
		}
		nearObjectParams = &p
		targetVectorCombination = targetCombination
	}

	var moduleParams map[string]interface{}
	if r.modulesProvider != nil {
		extractedParams, extractedCombinations := r.modulesProvider.ExtractSearchParams(p.Args, className)
		if len(extractedParams) > 0 {
			moduleParams = extractedParams
		}
		if len(extractedCombinations) == 1 {
			for _, val := range extractedCombinations {
				targetVectorCombination = val
			}
		}
	}

	// extracts bm25 (sparseSearch) from the query
	var keywordRankingParams *searchparams.KeywordRanking
	if bm25, ok := p.Args["bm25"]; ok {
		if len(sort) > 0 {
			return nil, fmt.Errorf("bm25 search is not compatible with sort")
		}
		p := common_filters.ExtractBM25(bm25.(map[string]interface{}), addlProps.ExplainScore)
		keywordRankingParams = &p
	}

	// Extract hybrid search params from the processed query
	// Everything hybrid can go in another namespace AFTER modulesprovider is
	// refactored
	var hybridParams *searchparams.HybridSearch
	if hybrid, ok := p.Args["hybrid"]; ok {
		if len(sort) > 0 {
			return nil, fmt.Errorf("hybrid search is not compatible with sort")
		}
		p, targetCombination, err := common_filters.ExtractHybridSearch(hybrid.(map[string]interface{}), addlProps.ExplainScore)
		if err != nil {
			return nil, fmt.Errorf("failed to extract hybrid params: %w", err)
		}
		hybridParams = p
		targetVectorCombination = targetCombination
	}

	var replProps *additional.ReplicationProperties
	var level string
	if cl, ok := p.Args["consistencyLevel"]; ok {
		level = cl.(string)
		replProps = &additional.ReplicationProperties{
			ConsistencyLevel: level,
		}
	}
	monitoring.GetMetrics().IncConsistencyLevelRequest(monitoring.ConsistencyLevelRead, level)

	group := extractGroup(p.Args)

	var groupByParams *searchparams.GroupBy
	if groupBy, ok := p.Args["groupBy"]; ok {
		p := common_filters.ExtractGroupBy(groupBy.(map[string]interface{}))
		p.Properties = groupByProperties
		groupByParams = &p
	}

	params := dto.GetParams{
		Filters:                 filters,
		ClassName:               className,
		Pagination:              pagination,
		Cursor:                  cursor,
		Properties:              properties,
		Sort:                    sort,
		NearVector:              nearVectorParams,
		NearObject:              nearObjectParams,
		Group:                   group,
		ModuleParams:            moduleParams,
		AdditionalProperties:    addlProps,
		KeywordRanking:          keywordRankingParams,
		HybridSearch:            hybridParams,
		ReplicationProperties:   replProps,
		GroupBy:                 groupByParams,
		Tenant:                  tenant,
		TargetVectorCombination: targetVectorCombination,
	}

	// need to perform vector search by distance
	// under certain conditions
	setLimitBasedOnVectorSearchParams(&params)

	return func() (interface{}, error) {
		result, err := resolver.GetClass(p.Context, principal, params)
		if err != nil {
			if errors.Is(err, queryadmission.ErrOverloaded) {
				// GraphQL has no typed status code; mirror the global rate
				// limiter's 429 message so clients can detect overload.
				err = enterrors.NewErrRateLimit()
			}
			return result, enterrors.NewErrGraphQLUser(err, "Get", params.ClassName)
		}
		return result, nil
	}, nil
}

// the limit needs to be set according to the vector search parameters.
// for example, if a certainty is provided by any of the near* options,
// and no limit was provided, weaviate will want to execute a vector
// search by distance. it knows to do this by watching for a limit
// flag, specifically filters.LimitFlagSearchByDistance
func setLimitBasedOnVectorSearchParams(params *dto.GetParams) {
	setLimit := func(params *dto.GetParams) {
		if params.Pagination == nil {
			// limit was omitted entirely, implicitly
			// indicating to do unlimited search
			params.Pagination = &filters.Pagination{
				Limit: filters.LimitFlagSearchByDist,
			}
		} else if params.Pagination.Limit < 0 {
			// a negative limit was set, explicitly
			// indicating to do unlimited search
			params.Pagination.Limit = filters.LimitFlagSearchByDist
		}
	}

	if params.NearVector != nil &&
		(params.NearVector.Certainty != 0 || params.NearVector.WithDistance) {
		setLimit(params)
		return
	}

	if params.NearObject != nil &&
		(params.NearObject.Certainty != 0 || params.NearObject.WithDistance) {
		setLimit(params)
		return
	}

	for _, param := range params.ModuleParams {
		nearParam, ok := param.(modulecapabilities.NearParam)
		if ok && nearParam.SimilarityMetricProvided() {
			setLimit(params)
			return
		}
	}
}

func extractGroup(args map[string]interface{}) *dto.GroupParams {
	group, ok := args["group"]
	if !ok {
		return nil
	}

	asMap := group.(map[string]interface{}) // guaranteed by graphql
	strategy := asMap["type"].(string)
	force := asMap["force"].(float64)
	return &dto.GroupParams{
		Strategy: strategy,
		Force:    float32(force),
	}
}

func isPrimitive(selectionSet *ast.SelectionSet, fragments map[string]ast.Definition) bool {
	if selectionSet == nil {
		return true
	}

	// if there is a selection set it could either be a cross-ref or a map-type
	// field like GeoCoordinates or PhoneNumber
	for _, subSelection := range selectionSet.Selections {
		switch s := subSelection.(type) {
		case *ast.Field:
			if fieldNameIsOfObjectButNonReferenceType(s.Name.Value) {
				return true
			}
		case *ast.InlineFragment:
			if s.TypeCondition == nil {
				if isPrimitive(s.SelectionSet, fragments) {
					return true
				}
			} else if typeNameIsOfObjectButNonReferenceType(s.TypeCondition.Name.Value) {
				return true
			}
		case *ast.FragmentSpread:
			// fragment fields can't tell: a referenced class may have a property
			// named like a geo or phone field, so look at the fragment's type
			if def, ok := fragments[s.Name.Value].(*ast.FragmentDefinition); ok &&
				typeNameIsOfObjectButNonReferenceType(def.TypeCondition.Name.Value) {
				return true
			}
		}
	}

	// must be a ref field
	return false
}

type additionalCheck struct {
	modulesProvider ModulesProvider
}

func (ac *additionalCheck) isAdditional(parentName, name string) bool {
	if parentName == "_additional" {
		if name == "classification" || name == "certainty" ||
			name == "distance" || name == "id" || name == "vector" || name == "vectors" ||
			name == "creationTimeUnix" || name == "lastUpdateTimeUnix" ||
			name == "score" || name == "explainScore" || name == "isConsistent" ||
			name == "group" || name == "queryProfile" {
			return true
		}
		if ac.isModuleAdditional(name) {
			return true
		}
	}
	return false
}

func (ac *additionalCheck) isModuleAdditional(name string) bool {
	if ac.modulesProvider != nil {
		if len(ac.modulesProvider.GraphQLAdditionalFieldNames()) > 0 {
			for _, moduleAdditionalProperty := range ac.modulesProvider.GraphQLAdditionalFieldNames() {
				if name == moduleAdditionalProperty {
					return true
				}
			}
		}
	}
	return false
}

func fieldNameIsOfObjectButNonReferenceType(field string) bool {
	switch field {
	case "latitude", "longitude":
		// must be a geo prop
		return true
	case "input", "internationalFormatted", "nationalFormatted", "national",
		"valid", "countryCode", "defaultCountry":
		// must be a phone number
		return true
	default:
		return false
	}
}

// typeNameIsOfObjectButNonReferenceType matches the GraphQL type names built
// for geo coordinates and phone number properties.
func typeNameIsOfObjectButNonReferenceType(typeName string) bool {
	return strings.HasSuffix(typeName, "GeoCoordinatesObj") || strings.HasSuffix(typeName, "PhoneNumberObj")
}

// selectedFields expands fragments like common_filters.SelectedFields and then
// folds fields selected more than once into one field holding all their
// selections, like graphql's executor does. Extracting every repeat on its own
// would be exponential in the nesting depth of repeated reference selections.
func selectedFields(set *ast.SelectionSet, info *graphql.ResolveInfo) []*ast.Field {
	var fields []*ast.Field
	positions := map[string]int{}
	for _, field := range common_filters.SelectedFields(set, info.Fragments, info.VariableValues) {
		i, ok := positions[field.Name.Value]
		if !ok {
			positions[field.Name.Value] = len(fields)
			fields = append(fields, field)
			continue
		}
		if field.SelectionSet == nil {
			continue
		}
		// copy instead of modifying the query's AST
		merged := *fields[i]
		merged.SelectionSet = &ast.SelectionSet{}
		if fields[i].SelectionSet != nil {
			merged.SelectionSet.Selections = append(merged.SelectionSet.Selections, fields[i].SelectionSet.Selections...)
		}
		merged.SelectionSet.Selections = append(merged.SelectionSet.Selections, field.SelectionSet.Selections...)
		fields[i] = &merged
	}
	return fields
}

func extractProperties(className string, selections *ast.SelectionSet,
	info *graphql.ResolveInfo, modulesProvider ModulesProvider,
) ([]search.SelectProperty, additional.Properties, []search.SelectProperty, error) {
	var properties []search.SelectProperty
	var additionalGroupHitProperties []search.SelectProperty
	var additionalProps additional.Properties
	additionalCheck := &additionalCheck{modulesProvider}

	for _, field := range selectedFields(selections, info) {
		name := field.Name.Value
		if name == "_additional" {
			// _additional is an object, not a reference: its fragments group
			// fields instead of selecting a target class
			for _, s := range selectedFields(field.SelectionSet, info) {
				additionalProperty := s.Name.Value
				if !additionalCheck.isAdditional(name, additionalProperty) {
					continue
				}
				if additionalProperty == "classification" {
					additionalProps.Classification = true
					continue
				}
				if additionalProperty == "certainty" {
					additionalProps.Certainty = true
					continue
				}
				if additionalProperty == "distance" {
					additionalProps.Distance = true
					continue
				}
				if additionalProperty == "id" {
					additionalProps.ID = true
					continue
				}
				if additionalProperty == "vector" {
					additionalProps.Vector = true
					continue
				}
				if additionalProperty == "vectors" {
					for _, vector := range common_filters.SelectedFields(s.SelectionSet, info.Fragments, info.VariableValues) {
						if !slices.Contains(additionalProps.Vectors, vector.Name.Value) {
							additionalProps.Vectors = append(additionalProps.Vectors, vector.Name.Value)
						}
					}
					continue
				}
				if additionalProperty == "creationTimeUnix" {
					additionalProps.CreationTimeUnix = true
					continue
				}
				if additionalProperty == "score" {
					additionalProps.Score = true
					continue
				}
				if additionalProperty == "explainScore" {
					additionalProps.ExplainScore = true
					continue
				}
				if additionalProperty == "queryProfile" {
					additionalProps.QueryProfile = true
					continue
				}
				if additionalProperty == "lastUpdateTimeUnix" {
					additionalProps.LastUpdateTimeUnix = true
					continue
				}
				if additionalProperty == "isConsistent" {
					additionalProps.IsConsistent = true
					continue
				}
				if additionalProperty == "group" {
					additionalProps.Group = true
					var err error
					additionalGroupHitProperties, err = extractGroupHitProperties(className, s, info, modulesProvider)
					if err != nil {
						return nil, additionalProps, nil, err
					}
					continue
				}
				if modulesProvider != nil {
					if additionalCheck.isModuleAdditional(additionalProperty) {
						additionalProps.ModuleParams = getModuleParams(additionalProps.ModuleParams)
						extracted := modulesProvider.ExtractAdditionalField(className, additionalProperty, s.Arguments)
						if extractor, ok := extracted.(moduleadditional.PropertyExtractor); ok {
							extractedProperties := extractor.GetPropertiesToExtract()
							for _, extractedProperty := range extractedProperties {
								properties = append(properties, search.SelectProperty{Name: extractedProperty})
							}
						}
						additionalProps.ModuleParams[additionalProperty] = extracted
						continue
					}
				}
			}
			continue
		}

		property := search.SelectProperty{Name: name}
		property.IsPrimitive = isPrimitive(field.SelectionSet, info.Fragments)
		if !property.IsPrimitive {
			if err := extractRefs(&property, className, field.SelectionSet, info, modulesProvider); err != nil {
				return nil, additionalProps, nil, err
			}
		}
		properties = append(properties, property)
	}

	return properties, additionalProps, additionalGroupHitProperties, nil
}

func extractGroupHitProperties(className string, group *ast.Field,
	info *graphql.ResolveInfo, modulesProvider ModulesProvider,
) ([]search.SelectProperty, error) {
	additionalGroupProperties := []search.SelectProperty{}
	for _, groupField := range selectedFields(group.SelectionSet, info) {
		if groupField.Name.Value != "hits" {
			continue
		}
		for _, hitField := range selectedFields(groupField.SelectionSet, info) {
			hitProperty := search.SelectProperty{Name: hitField.Name.Value}
			if hitField.SelectionSet == nil {
				additionalGroupProperties = append(additionalGroupProperties, hitProperty)
				continue
			}
			if err := extractRefs(&hitProperty, className, hitField.SelectionSet, info, modulesProvider); err != nil {
				return nil, err
			}
			if len(hitProperty.Refs) > 0 {
				additionalGroupProperties = append(additionalGroupProperties, hitProperty)
			}
		}
	}
	return additionalGroupProperties, nil
}

func getModuleParams(moduleParams map[string]interface{}) map[string]interface{} {
	if moduleParams == nil {
		return map[string]interface{}{}
	}
	return moduleParams
}

// extractRefs adds the target classes selected on a reference property, one
// per class however many fragments select it. A fragment on the reference's
// union type, or one without a type condition, only groups class fragments,
// so it is expanded instead of being taken for a target class. Like
// common_filters.SelectedFields it honors @skip/@include and expands each
// named fragment once.
func extractRefs(property *search.SelectProperty, className string, selections *ast.SelectionSet,
	info *graphql.ResolveInfo, modulesProvider ModulesProvider,
) error {
	var classes []string
	selectionsByClass := map[string]*ast.SelectionSet{}
	visited := map[string]bool{}

	var collect func(set *ast.SelectionSet) error
	collect = func(set *ast.SelectionSet) error {
		for _, selection := range set.Selections {
			var typeCondition *ast.Named
			var fragmentSelections *ast.SelectionSet
			switch s := selection.(type) {
			case *ast.Field:
				// any other field belongs to an object / object array property
				if s.Name.Value == "__typename" && common_filters.IsIncluded(s.Directives, info.VariableValues) {
					property.IncludeTypeName = true
				}
				continue
			case *ast.InlineFragment:
				if !common_filters.IsIncluded(s.Directives, info.VariableValues) {
					continue
				}
				typeCondition, fragmentSelections = s.TypeCondition, s.SelectionSet
			case *ast.FragmentSpread:
				if visited[s.Name.Value] || !common_filters.IsIncluded(s.Directives, info.VariableValues) {
					continue
				}
				visited[s.Name.Value] = true
				def, ok := info.Fragments[s.Name.Value].(*ast.FragmentDefinition)
				if !ok {
					return fmt.Errorf("spread fragment '%s' refers to unknown fragment", s.Name.Value)
				}
				typeCondition, fragmentSelections = def.TypeCondition, def.SelectionSet
			default:
				return fmt.Errorf("unrecoginzed type in subs-selection: %T", selection)
			}

			if typeCondition == nil || isUnion(info, typeCondition) {
				if err := collect(fragmentSelections); err != nil {
					return err
				}
				continue
			}
			if isNestedObject(info, typeCondition) {
				// it selects fields of an object / object array property
				continue
			}
			class := typeCondition.Name.Value
			classSelections, ok := selectionsByClass[class]
			if !ok {
				classes = append(classes, class)
				classSelections = &ast.SelectionSet{}
				selectionsByClass[class] = classSelections
			}
			classSelections.Selections = append(classSelections.Selections, fragmentSelections.Selections...)
		}
		return nil
	}
	if err := collect(selections); err != nil {
		return err
	}

	for _, class := range classes {
		ref, err := extractRef(className, class, selectionsByClass[class], info, modulesProvider)
		if err != nil {
			return err
		}
		property.Refs = append(property.Refs, ref)
	}
	return nil
}

func isUnion(info *graphql.ResolveInfo, typeCondition *ast.Named) bool {
	_, ok := info.Schema.Type(typeCondition.Name.Value).(*graphql.Union)
	return ok
}

// isNestedObject tells the object types of nested properties apart from class
// types, which always have an _additional field.
func isNestedObject(info *graphql.ResolveInfo, typeCondition *ast.Named) bool {
	object, ok := info.Schema.Type(typeCondition.Name.Value).(*graphql.Object)
	if !ok {
		return false
	}
	_, isClass := object.Fields()["_additional"]
	return !isClass
}

func extractRef(className, refClass string, selections *ast.SelectionSet,
	info *graphql.ResolveInfo, modulesProvider ModulesProvider,
) (search.SelectClass, error) {
	var result search.SelectClass

	// class names never start with "_", so this is a class named like a
	// GraphQL scalar, see getClassObjectName
	refClass = strings.TrimPrefix(refClass, "_")

	// a "__" type is a helper type for a network ref, don't validate it
	if !strings.Contains(refClass, "__") {
		if _, err := schema.ValidateClassName(refClass); err != nil {
			return result, fmt.Errorf("the fragment type name '%s' is not a valid class name", refClass)
		}
	}

	if refClass == "Beacon" {
		return result, errors.New("retrieving cross-refs by beacon is not supported yet - coming soon!")
	}

	subProperties, additionalProperties, _, err := extractProperties(className, selections, info, modulesProvider)
	if err != nil {
		return result, err
	}

	result.ClassName = refClass
	result.RefProperties = subProperties
	result.AdditionalProperties = additionalProperties
	return result, nil
}
