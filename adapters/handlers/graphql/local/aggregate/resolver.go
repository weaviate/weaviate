//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Package aggregate provides the local aggregate graphql endpoint for Weaviate
package aggregate

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
)

// GroupedByFieldName is a special graphQL field that appears alongside the
// to-be-aggregated props, but doesn't require any processing by the connectors
// itself, as it just displays meta info about the overall aggregation.
const GroupedByFieldName = "groupedBy"

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the Meta feature must implement this interface.
type Resolver interface {
	Aggregate(ctx context.Context, principal *models.Principal, info *aggregation.Params) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func makeResolveClass(modulesProvider ModulesProvider, class *models.Class) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		className := schema.ClassName(p.Info.FieldName)
		source, ok := p.Source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected source to be a map, but was %t", p.Source)
		}

		resolver, ok := source["Resolver"].(Resolver)
		if !ok {
			return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %t", p.Source)
		}

		// There can only be exactly one ast.Field; it is the class name.
		if len(p.Info.FieldASTs) != 1 {
			panic("Only one Field expected here")
		}

		selections := p.Info.FieldASTs[0].SelectionSet
		properties, includeMeta, err := extractProperties(selections)
		if err != nil {
			return nil, fmt.Errorf("could not extract properties for class '%s': %s", className, err)
		}

		groupBy, err := extractGroupBy(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract groupBy path: %s", err)
		}

		limit, err := extractLimit(p.Args)
		if err != nil {
			return nil, fmt.Errorf("could not extract limit: %s", err)
		}

		objectLimit, err := extractObjectLimit(p.Args)
		if objectLimit != nil && *objectLimit <= 0 {
			return nil, fmt.Errorf("objectLimit must be a positive integer")
		}
		if err != nil {
			return nil, fmt.Errorf("could not extract objectLimit: %s", err)
		}

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		var nearVectorParams *searchparams.NearVector
		if nearVector, ok := p.Args["nearVector"]; ok {
			p := common_filters.ExtractNearVector(nearVector.(map[string]interface{}))
			nearVectorParams = &p
		}

		var nearObjectParams *searchparams.NearObject
		if nearObject, ok := p.Args["nearObject"]; ok {
			p := common_filters.ExtractNearObject(nearObject.(map[string]interface{}))
			nearObjectParams = &p
		}

		var moduleParams map[string]interface{}
		if modulesProvider != nil {
			extractedParams := modulesProvider.ExtractSearchParams(p.Args, class.Class)
			if len(extractedParams) > 0 {
				moduleParams = extractedParams
			}
		}

		params := &aggregation.Params{
			Filters:          filters,
			ClassName:        className,
			Properties:       properties,
			GroupBy:          groupBy,
			IncludeMetaCount: includeMeta,
			Limit:            limit,
			ObjectLimit:      objectLimit,
			NearVector:       nearVectorParams,
			NearObject:       nearObjectParams,
			ModuleParams:     moduleParams,
		}

		// we might support objectLimit without nearMedia filters later, e.g. with sort
		if params.ObjectLimit != nil && !areNearMediaFiltersIncluded(params) {
			return nil, fmt.Errorf("objectLimit can only be used with a near<Media> filter")
		}

		res, err := resolver.Aggregate(p.Context, principalFromContext(p.Context), params)
		if err != nil {
			return nil, err
		}

		switch parsed := res.(type) {
		case *aggregation.Result:
			return parsed.Groups, nil
		default:
			return res, nil
		}
	}
}

func extractProperties(selections *ast.SelectionSet) ([]aggregation.ParamProperty, bool, error) {
	properties := []aggregation.ParamProperty{}
	var includeMeta bool

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		if name == GroupedByFieldName {
			// in the graphQL API we show the "groupedBy" field alongside various
			// properties, however, we don't have to include it here, as we don't
			// wont to perform aggregations on it.
			// If we didn't exclude it we'd run into errors down the line, because
			// the connector would look for a "groupedBy" prop on the specific class
			// which doesn't exist.

			continue
		}

		if name == "meta" {
			includeMeta = true
			continue
		}

		if name == "__typename" {
			continue
		}

		name = strings.ToLower(string(name[0:1])) + string(name[1:])
		property := aggregation.ParamProperty{Name: schema.PropertyName(name)}
		aggregators, err := extractAggregators(field.SelectionSet)
		if err != nil {
			return nil, false, err
		}

		property.Aggregators = aggregators
		properties = append(properties, property)
	}

	return properties, includeMeta, nil
}

func extractAggregators(selections *ast.SelectionSet) ([]aggregation.Aggregator, error) {
	if selections == nil {
		return nil, nil
	}
	analyses := []aggregation.Aggregator{}
	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		if name == "__typename" {
			continue
		}
		property, err := aggregation.ParseAggregatorProp(name)
		if err != nil {
			return nil, err
		}

		if property.String() == aggregation.NewTopOccurrencesAggregator(nil).String() {
			// a top occurrence, so we need to check if we have a limit argument
			if overwrite := extractLimitFromArgs(field.Arguments); overwrite != nil {
				property.Limit = overwrite
			}
		}

		analyses = append(analyses, property)
	}

	return analyses, nil
}

func extractGroupBy(args map[string]interface{}, rootClass string) (*filters.Path, error) {
	groupBy, ok := args["groupBy"]
	if !ok {
		// not set means the user is not intersted in grouping (former Meta)
		return nil, nil
	}

	pathSegments, ok := groupBy.([]interface{})
	if !ok {
		return nil, fmt.Errorf("no groupBy must be a list, instead got: %#v", groupBy)
	}

	return filters.ParsePath(pathSegments, rootClass)
}

func principalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value("principal")
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}

func extractLimit(args map[string]interface{}) (*int, error) {
	limit, ok := args["limit"]
	if !ok {
		// not set means the user is not intersted and the UC should use a reasonable default
		return nil, nil
	}

	limitInt, ok := limit.(int)
	if !ok {
		return nil, fmt.Errorf("limit must be an int, instead got: %#v", limit)
	}

	return &limitInt, nil
}

func extractObjectLimit(args map[string]interface{}) (*int, error) {
	objectLimit, ok := args["objectLimit"]
	if !ok {
		return nil, nil
	}

	objectLimitInt, ok := objectLimit.(int)
	if !ok {
		return nil, fmt.Errorf("objectLimit must be an int, instead got: %#v", objectLimit)
	}

	return &objectLimitInt, nil
}

func extractLimitFromArgs(args []*ast.Argument) *int {
	for _, arg := range args {
		if arg.Name.Value != "limit" {
			continue
		}

		v, ok := arg.Value.GetValue().(string)
		if ok {
			asInt, _ := strconv.Atoi(v)
			return &asInt
		}
	}

	return nil
}

func areNearMediaFiltersIncluded(params *aggregation.Params) bool {
	return params.NearObject != nil ||
		params.NearVector != nil ||
		len(params.ModuleParams) > 0
}
