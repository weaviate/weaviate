/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package getmeta provides the local get meta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalGetMeta(info *Params) (interface{}, error)
}

// Params to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type Params struct {
	Kind             kind.Kind
	Filters          *common_filters.LocalFilter
	ClassName        schema.ClassName
	Properties       []MetaProperty
	IncludeMetaCount bool
}

// StatisticalAnalysis is the desired computation that the database connector
// should perform on this property
type StatisticalAnalysis string

const (
	// Count the occurence of this property
	Count StatisticalAnalysis = "count"

	// Sum of all the values of the prop (i.e. sum of all Ints or Numbers)
	Sum StatisticalAnalysis = "sum"

	// Average calculates the average of an Int or Number
	Average StatisticalAnalysis = "average"

	// Highest selects the highest value of an Int or Number
	Highest StatisticalAnalysis = "highest"

	// Lowest selects the highest value of an Int or Number
	Lowest StatisticalAnalysis = "lowest"

	// TotalTrue is the sum of all boolean fields, that are true
	TotalTrue StatisticalAnalysis = "totalTrue"

	// TotalFalse is the sum of all boolean fields, that are false
	TotalFalse StatisticalAnalysis = "totalFalse"

	// PercentageTrue is the percentage of all boolean fields, that are true
	PercentageTrue StatisticalAnalysis = "percentageTrue"

	// PercentageFalse is the percentage of all boolean fields, that are false
	PercentageFalse StatisticalAnalysis = "percentageFalse"

	// PointingTo is the list of all classes that this reference prop points to
	PointingTo StatisticalAnalysis = "pointingTo"

	// TopOccurrences of strings, selection can be made more specific with
	// TopOccurrencesValues for now. In the future there might also be other
	// sub-props.
	TopOccurrences StatisticalAnalysis = "topOccurrences"

	// TopOccurrencesValues is a sub-prop of TopOccurrences
	TopOccurrencesValues StatisticalAnalysis = "values"
)

// MetaProperty is any property of a class that we want to retrieve meta
// information about
type MetaProperty struct {
	Name                schema.PropertyName
	StatisticalAnalyses []StatisticalAnalysis
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

		// There can only be exactly one ast.Field; it is the class name.
		if len(p.Info.FieldASTs) != 1 {
			panic("Only one Field expected here")
		}

		selections := p.Info.FieldASTs[0].SelectionSet
		properties, err := extractMetaProperties(selections)
		if err != nil {
			return nil, fmt.Errorf("could not extract properties for class '%s': %s", className, err)
		}

		params := &Params{
			Kind:       kind,
			Filters:    nil,
			ClassName:  className,
			Properties: properties,
		}
		return resolver.LocalGetMeta(params)
	}
}

func extractMetaProperties(selections *ast.SelectionSet) ([]MetaProperty, error) {
	properties := make([]MetaProperty, len(selections.Selections), len(selections.Selections))

	for i, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property := MetaProperty{Name: schema.PropertyName(name)}
		analysesProps, err := extractPropertyAnalyses(field.SelectionSet)
		if err != nil {
			return nil, err
		}

		property.StatisticalAnalyses = analysesProps
		properties[i] = property
	}

	return properties, nil
}

func extractPropertyAnalyses(selections *ast.SelectionSet) ([]StatisticalAnalysis, error) {
	analyses := make([]StatisticalAnalysis, len(selections.Selections), len(selections.Selections))
	for i, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		property, err := parseAnalysisProp(name)
		if err != nil {
			return nil, err
		}

		if property == TopOccurrences {
			// TopOccurrences is the only nested prop for now and it has exactly one
			// option (values). Knowing this we don't actually have to parse the
			// subprops. If either another nested prop is added at some point - or
			// topOccurrences gets a second prop, then we actually need to start
			// parsing them.
			analyses[i] = TopOccurrencesValues
			continue
		}

		analyses[i] = property
	}

	return analyses, nil
}

func parseAnalysisProp(name string) (StatisticalAnalysis, error) {
	switch name {
	case string(Average):
		return Average, nil
	case string(Highest):
		return Highest, nil
	case string(Lowest):
		return Lowest, nil
	case string(Count):
		return Count, nil
	case string(Sum):
		return Sum, nil
	case string(TotalTrue):
		return TotalTrue, nil
	case string(TotalFalse):
		return TotalFalse, nil
	case string(PercentageTrue):
		return PercentageTrue, nil
	case string(PercentageFalse):
		return PercentageFalse, nil
	case string(PointingTo):
		return PointingTo, nil
	case string(TopOccurrences):
		return TopOccurrences, nil
	default:
		return "", fmt.Errorf("unrecognized statistical prop '%s'", name)
	}
}
