/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

// Package aggregate provides the local aggregate graphql endpoint for Weaviate
package aggregate

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/telemetry"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/semi-technologies/weaviate/entities/filters"
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
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalAggregate(ctx context.Context, info *kinds.AggregateParams) (interface{}, error)
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
		properties, err := extractProperties(selections)
		if err != nil {
			return nil, fmt.Errorf("could not extract properties for class '%s': %s", className, err)
		}

		groupBy, err := extractGroupBy(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract groupBy path: %s", err)
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

		params := &kinds.AggregateParams{
			Kind:       kind,
			Filters:    filters,
			ClassName:  className,
			Properties: properties,
			GroupBy:    groupBy,
			Analytics:  analytics,
		}

		return resolver.LocalAggregate(p.Context, params)
	}
}

func extractProperties(selections *ast.SelectionSet) ([]kinds.AggregateProperty, error) {
	properties := []kinds.AggregateProperty{}

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

		property := kinds.AggregateProperty{Name: schema.PropertyName(name)}
		aggregators, err := extractAggregators(field.SelectionSet)
		if err != nil {
			return nil, err
		}

		property.Aggregators = aggregators
		properties = append(properties, property)
	}

	return properties, nil
}

func extractAggregators(selections *ast.SelectionSet) ([]kinds.Aggregator, error) {
	analyses := []kinds.Aggregator{}
	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property, err := kinds.ParseAggregatorProp(name)
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
		return nil, fmt.Errorf("no groupBy present in args")
	}

	pathSegments, ok := groupBy.([]interface{})
	if !ok {
		return nil, fmt.Errorf("no groupBy must be a list, instead got: %#v", groupBy)
	}

	return filters.ParsePath(pathSegments, rootClass)
}
