//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Package aggregate provides the local aggregate graphql endpoint for Weaviate
package aggregate

import (
	"context"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/semi-technologies/weaviate/usecases/traverser"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
)

// GroupedByFieldName is a special graphQL field that appears alongside the
// to-be-aggregated props, but doesn't require any processing by the connectors
// itself, as it just displays meta info about the overall aggregation.
const GroupedByFieldName = "groupedBy"

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the Meta feature must implement this interface.
type Resolver interface {
	Aggregate(ctx context.Context, principal *models.Principal, info *traverser.AggregateParams) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

func makeResolveClass(kind kind.Kind) graphql.FieldResolveFn {
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

		cfg, ok := source["Config"].(config.Config)
		if !ok {
			return nil, fmt.Errorf("expected source to contain a config, but was %t", p.Source)
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
			return nil, fmt.Errorf("could not extract limits: %s", err)
		}

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		requestsLog, ok := source["RequestsLog"].(RequestsLog)
		if !ok {
			return nil, fmt.Errorf("expected source to contain a usable RequestsLog, but was %t", p.Source)
		}
		for _, property := range properties {
			for _, aggregator := range property.Aggregators {
				serviceID := fmt.Sprintf("%s[%s]", telemetry.LocalQuery, aggregator)
				go func() {
					requestsLog.Register(telemetry.TypeGQL, serviceID)
				}()

			}
		}
		analytics, err := common_filters.ExtractAnalyticsProps(p.Args, cfg.AnalyticsEngine)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		params := &traverser.AggregateParams{
			Kind:             kind,
			Filters:          filters,
			ClassName:        className,
			Properties:       properties,
			GroupBy:          groupBy,
			Analytics:        analytics,
			IncludeMetaCount: includeMeta,
			Limit:            limit,
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

func extractProperties(selections *ast.SelectionSet) ([]traverser.AggregateProperty, bool, error) {
	properties := []traverser.AggregateProperty{}
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
		property := traverser.AggregateProperty{Name: schema.PropertyName(name)}
		aggregators, err := extractAggregators(field.SelectionSet)
		if err != nil {
			return nil, false, err
		}

		property.Aggregators = aggregators
		properties = append(properties, property)
	}

	return properties, includeMeta, nil
}

func extractAggregators(selections *ast.SelectionSet) ([]traverser.Aggregator, error) {
	if selections == nil {
		return nil, nil
	}
	analyses := []traverser.Aggregator{}
	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		if name == "__typename" {
			continue
		}
		property, err := traverser.ParseAggregatorProp(name)
		if err != nil {
			return nil, err
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
		return nil, fmt.Errorf("limit must be a int, instead got: %#v", limit)
	}

	return &limitInt, nil
}
