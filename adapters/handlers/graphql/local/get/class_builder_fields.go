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

package get

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
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
	case schema.DataTypeBlob:
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
	class *models.Class, modulesProvider ModulesProvider, fusionEnum *graphql.Enum,
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
		Resolve: newResolver(modulesProvider).makeResolveGetClass(class.Class),
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
	modulesProvider ModulesProvider
}

func newResolver(modulesProvider ModulesProvider) *resolver {
	return &resolver{modulesProvider}
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
	source, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected graphql root to be a map, but was %T", p.Source)
	}

	resolver, ok := source["Resolver"].(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source map to have a usable Resolver, but got %#v", source["Resolver"])
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

	properties, addlProps, err := extractProperties(
		className, selectionsOfClass, p.Info.Fragments, r.modulesProvider)
	if err != nil {
		return nil, err
	}

	var sort []filters.Sort
	if sortArg, ok := p.Args["sort"]; ok {
		sort = filters.ExtractSortFromArgs(sortArg.([]interface{}))
	}

	filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
	if err != nil {
		return nil, fmt.Errorf("could not extract filters: %s", err)
	}

	var nearVectorParams *searchparams.NearVector
	if nearVector, ok := p.Args["nearVector"]; ok {
		p, err := common_filters.ExtractNearVector(nearVector.(map[string]interface{}))
		if err != nil {
			return nil, fmt.Errorf("failed to extract nearVector params: %s", err)
		}
		nearVectorParams = &p
	}

	var nearObjectParams *searchparams.NearObject
	if nearObject, ok := p.Args["nearObject"]; ok {
		p, err := common_filters.ExtractNearObject(nearObject.(map[string]interface{}))
		if err != nil {
			return nil, fmt.Errorf("failed to extract nearObject params: %s", err)
		}
		nearObjectParams = &p
	}

	var moduleParams map[string]interface{}
	if r.modulesProvider != nil {
		extractedParams := r.modulesProvider.ExtractSearchParams(p.Args, className)
		if len(extractedParams) > 0 {
			moduleParams = extractedParams
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
		p, err := common_filters.ExtractHybridSearch(hybrid.(map[string]interface{}), addlProps.ExplainScore)
		if err != nil {
			return nil, fmt.Errorf("failed to extract hybrid params: %w", err)
		}
		hybridParams = p
	}

	var replProps *additional.ReplicationProperties
	if cl, ok := p.Args["consistencyLevel"]; ok {
		replProps = &additional.ReplicationProperties{
			ConsistencyLevel: cl.(string),
		}
	}

	group := extractGroup(p.Args)

	var groupByParams *searchparams.GroupBy
	if groupBy, ok := p.Args["groupBy"]; ok {
		p := common_filters.ExtractGroupBy(groupBy.(map[string]interface{}))
		groupByParams = &p
	}

	var tenant string
	if tk, ok := p.Args["tenant"]; ok {
		tenant = tk.(string)
	}

	params := dto.GetParams{
		Filters:               filters,
		ClassName:             className,
		Pagination:            pagination,
		Cursor:                cursor,
		Properties:            properties,
		Sort:                  sort,
		NearVector:            nearVectorParams,
		NearObject:            nearObjectParams,
		Group:                 group,
		ModuleParams:          moduleParams,
		AdditionalProperties:  addlProps,
		KeywordRanking:        keywordRankingParams,
		HybridSearch:          hybridParams,
		ReplicationProperties: replProps,
		GroupBy:               groupByParams,
		Tenant:                tenant,
	}

	// need to perform vector search by distance
	// under certain conditions
	setLimitBasedOnVectorSearchParams(&params)

	return func() (interface{}, error) {
		result, err := resolver.GetClass(p.Context, principalFromContext(p.Context), params)
		if err != nil {
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

func principalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value("principal")
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}

func isPrimitive(selectionSet *ast.SelectionSet) bool {
	if selectionSet == nil {
		return true
	}

	// if there is a selection set it could either be a cross-ref or a map-type
	// field like GeoCoordinates or PhoneNumber
	for _, subSelection := range selectionSet.Selections {
		if subsectionField, ok := subSelection.(*ast.Field); ok {
			if fieldNameIsOfObjectButNonReferenceType(subsectionField.Name.Value) {
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
			name == "distance" || name == "id" || name == "vector" ||
			name == "creationTimeUnix" || name == "lastUpdateTimeUnix" ||
			name == "score" || name == "explainScore" || name == "isConsistent" ||
			name == "group" {
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

func extractProperties(className string, selections *ast.SelectionSet,
	fragments map[string]ast.Definition,
	modulesProvider ModulesProvider,
) ([]search.SelectProperty, additional.Properties, error) {
	var properties []search.SelectProperty
	var additionalProps additional.Properties
	additionalCheck := &additionalCheck{modulesProvider}

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property := search.SelectProperty{Name: name}

		property.IsPrimitive = isPrimitive(field.SelectionSet)
		if !property.IsPrimitive {
			// We can interpret this property in different ways
			for _, subSelection := range field.SelectionSet.Selections {
				switch s := subSelection.(type) {
				case *ast.Field:
					// Is it a field with the name __typename?
					if s.Name.Value == "__typename" {
						property.IncludeTypeName = true
						continue
					} else if additionalCheck.isAdditional(name, s.Name.Value) {
						additionalProperty := s.Name.Value
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
							additionalGroupHitProperties, err := extractGroupHitProperties(className, additionalProps, subSelection, fragments, modulesProvider)
							if err != nil {
								return nil, additionalProps, err
							}
							properties = append(properties, additionalGroupHitProperties...)
							continue
						}
						if modulesProvider != nil {
							if additionalCheck.isModuleAdditional(additionalProperty) {
								additionalProps.ModuleParams = getModuleParams(additionalProps.ModuleParams)
								additionalProps.ModuleParams[additionalProperty] = modulesProvider.ExtractAdditionalField(className, additionalProperty, s.Arguments)
								continue
							}
						}
					} else {
						// It's an object / object array property
						continue
					}

				case *ast.FragmentSpread:
					ref, err := extractFragmentSpread(className, s, fragments, modulesProvider)
					if err != nil {
						return nil, additionalProps, err
					}

					property.Refs = append(property.Refs, ref)

				case *ast.InlineFragment:
					ref, err := extractInlineFragment(className, s, fragments, modulesProvider)
					if err != nil {
						return nil, additionalProps, err
					}

					property.Refs = append(property.Refs, ref)

				default:
					return nil, additionalProps, fmt.Errorf("unrecoginzed type in subs-selection: %T", subSelection)
				}
			}
		}

		if name == "_additional" {
			continue
		}

		properties = append(properties, property)
	}

	return properties, additionalProps, nil
}

func extractGroupHitProperties(
	className string,
	additionalProps additional.Properties,
	subSelection ast.Selection,
	fragments map[string]ast.Definition,
	modulesProvider ModulesProvider,
) ([]search.SelectProperty, error) {
	additionalGroupProperties := []search.SelectProperty{}
	if subSelection != nil {
		if selectionSet := subSelection.GetSelectionSet(); selectionSet != nil {
			for _, groupSubSelection := range selectionSet.Selections {
				if groupSubSelection != nil {
					if groupSubSelectionField, ok := groupSubSelection.(*ast.Field); ok {
						if groupSubSelectionField.Name.Value == "hits" && groupSubSelectionField.SelectionSet != nil {
							for _, groupHitsSubSelection := range groupSubSelectionField.SelectionSet.Selections {
								if hf, ok := groupHitsSubSelection.(*ast.Field); ok {
									if hf.SelectionSet != nil {
										for _, ss := range hf.SelectionSet.Selections {
											if inlineFrag, ok := ss.(*ast.InlineFragment); ok {
												ref, err := extractInlineFragment(className, inlineFrag, fragments, modulesProvider)
												if err != nil {
													return nil, err
												}

												additionalGroupHitProp := search.SelectProperty{Name: fmt.Sprintf("_additional:group:hits:%v", hf.Name.Value)}
												additionalGroupHitProp.Refs = append(additionalGroupHitProp.Refs, ref)
												additionalGroupProperties = append(additionalGroupProperties, additionalGroupHitProp)
											}
										}
									}
								}
							}
						}
					}
				}
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

func extractInlineFragment(class string, fragment *ast.InlineFragment,
	fragments map[string]ast.Definition,
	modulesProvider ModulesProvider,
) (search.SelectClass, error) {
	var className schema.ClassName
	var err error
	var result search.SelectClass

	if strings.Contains(fragment.TypeCondition.Name.Value, "__") {
		// is a helper type for a network ref
		// don't validate anything as of now
		className = schema.ClassName(fragment.TypeCondition.Name.Value)
	} else {
		className, err = schema.ValidateClassName(fragment.TypeCondition.Name.Value)
		if err != nil {
			return result, fmt.Errorf("the inline fragment type name '%s' is not a valid class name", fragment.TypeCondition.Name.Value)
		}
	}

	if className == "Beacon" {
		return result, fmt.Errorf("retrieving cross-refs by beacon is not supported yet - coming soon!")
	}

	subProperties, additionalProperties, err := extractProperties(class, fragment.SelectionSet, fragments, modulesProvider)
	if err != nil {
		return result, err
	}

	result.ClassName = string(className)
	result.RefProperties = subProperties
	result.AdditionalProperties = additionalProperties
	return result, nil
}

func extractFragmentSpread(class string, spread *ast.FragmentSpread,
	fragments map[string]ast.Definition,
	modulesProvider ModulesProvider,
) (search.SelectClass, error) {
	var result search.SelectClass
	name := spread.Name.Value

	def, ok := fragments[name]
	if !ok {
		return result, fmt.Errorf("spread fragment '%s' refers to unknown fragment", name)
	}

	className, err := hackyWorkaroundToExtractClassName(def, name)
	if err != nil {
		return result, err
	}

	subProperties, additionalProperties, err := extractProperties(class, def.GetSelectionSet(), fragments, modulesProvider)
	if err != nil {
		return result, err
	}

	result.ClassName = string(className)
	result.RefProperties = subProperties
	result.AdditionalProperties = additionalProperties
	return result, nil
}

// It seems there's no proper way to extract this info unfortunately:
// https://github.com/tailor-inc/graphql/issues/455
func hackyWorkaroundToExtractClassName(def ast.Definition, name string) (string, error) {
	loc := def.GetLoc()
	raw := loc.Source.Body[loc.Start:loc.End]
	r := regexp.MustCompile(fmt.Sprintf(`fragment\s*%s\s*on\s*(\w*)\s*{`, name))
	matches := r.FindSubmatch(raw)
	if len(matches) < 2 {
		return "", fmt.Errorf("could not extract a className from fragment")
	}

	return string(matches[1]), nil
}
