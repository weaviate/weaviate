/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

// Package aggregate provides the local aggregate graphql endpoint for Weaviate
package aggregate

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/telemetry"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// GroupedByFieldName is a special graphQL field that appears alongside the
// to-be-aggregated props, but doesn't require any processing by the connectors
// itself, as it just displays meta info about the overall aggregation.
const GroupedByFieldName = "groupedBy"

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalAggregate(info *Params) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.Get queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
}

// Params to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type Params struct {
	Kind       kind.Kind
	Filters    *common_filters.LocalFilter
	Analytics  common_filters.AnalyticsProps
	ClassName  schema.ClassName
	Properties []Property
	GroupBy    *common_filters.Path
}

// Aggregator is the desired computation that the database connector
// should perform on this property
type Aggregator string

const (
	// Count the occurence of this property
	Count Aggregator = "count"

	// Sum of all the values of the prop (i.e. sum of all Ints or Numbers)
	Sum Aggregator = "sum"

	// Mean calculates the mean of an Int or Number
	Mean Aggregator = "mean"

	// Mode calculates the mode (most occurring value) of an Int or Number
	Mode Aggregator = "mode"

	// Median calculates the median (most occurring value) of an Int or Number
	Median Aggregator = "median"

	// Maximum selects the maximum value of an Int or Number
	Maximum Aggregator = "maximum"

	// Minimum selects the maximum value of an Int or Number
	Minimum Aggregator = "minimum"
)

// Property is any property of a class that we want to retrieve meta
// information about
type Property struct {
	Name        schema.PropertyName
	Aggregators []Aggregator
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

		cfg, ok := source["Config"].(config.Environment)
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

		params := &Params{
			Kind:       kind,
			Filters:    filters,
			ClassName:  className,
			Properties: properties,
			GroupBy:    groupBy,
			Analytics:  analytics,
		}

		return resolver.LocalAggregate(params)
	}
}

func extractProperties(selections *ast.SelectionSet) ([]Property, error) {
	properties := []Property{}

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

		property := Property{Name: schema.PropertyName(name)}
		aggregators, err := extractAggregators(field.SelectionSet)
		if err != nil {
			return nil, err
		}

		property.Aggregators = aggregators
		properties = append(properties, property)
	}

	return properties, nil
}

func extractAggregators(selections *ast.SelectionSet) ([]Aggregator, error) {
	analyses := []Aggregator{}
	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property, err := parseAnalysisProp(name)
		if err != nil {
			return nil, err
		}

		analyses = append(analyses, property)
	}

	return analyses, nil
}

func parseAnalysisProp(name string) (Aggregator, error) {
	switch name {
	case string(Mean):
		return Mean, nil
	case string(Median):
		return Median, nil
	case string(Mode):
		return Mode, nil
	case string(Maximum):
		return Maximum, nil
	case string(Minimum):
		return Minimum, nil
	case string(Count):
		return Count, nil
	case string(Sum):
		return Sum, nil
	default:
		return "", fmt.Errorf("unrecognized aggregator prop '%s'", name)
	}
}

func extractGroupBy(args map[string]interface{}, rootClass string) (*common_filters.Path, error) {
	groupBy, ok := args["groupBy"]
	if !ok {
		return nil, fmt.Errorf("no groupBy present in args")
	}

	pathSegments, ok := groupBy.([]interface{})
	if !ok {
		return nil, fmt.Errorf("no groupBy must be a list, instead got: %#v", groupBy)
	}

	return common_filters.ParsePath(pathSegments, rootClass)
}
