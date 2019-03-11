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

// Package getmeta provides the local get meta graphql endpoint for Weaviate
package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/davecgh/go-spew/spew"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalGetMeta(info *Params) (interface{}, error)
}

// RequestsLog is a local abstraction on the RequestsLog that needs to be
// provided to the graphQL API in order to log Local.GetMeta queries.
type RequestsLog interface {
	Register(requestType string, identifier string)
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
	// Type can be applied to any field and will return the type of the field,
	// such as "int" or "string"
	Type StatisticalAnalysis = "type"

	// Count the occurence of this property
	Count StatisticalAnalysis = "count"

	// Sum of all the values of the prop (i.e. sum of all Ints or Numbers)
	Sum StatisticalAnalysis = "sum"

	// Mean calculates the mean of an Int or Number
	Mean StatisticalAnalysis = "mean"

	// Maximum selects the maximum value of an Int or Number
	Maximum StatisticalAnalysis = "maximum"

	// Minimum selects the maximum value of an Int or Number
	Minimum StatisticalAnalysis = "minimum"

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

	// TopOccurrencesValue is a sub-prop of TopOccurrences
	TopOccurrencesValue StatisticalAnalysis = "value"

	// TopOccurrencesOccurs is a sub-prop of TopOccurrences
	TopOccurrencesOccurs StatisticalAnalysis = "occurs"
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

		filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		if err != nil {
			return nil, fmt.Errorf("could not extract filters: %s", err)
		}

		params := &Params{
			Kind:       kind,
			Filters:    filters,
			ClassName:  className,
			Properties: properties,
		}

		// Log the request
		requestsLog := source["RequestsLog"].(RequestsLog)
		requestsLog.Register(telemetry.TypeGQL, telemetry.LocalQueryMeta)

		return resolver.LocalGetMeta(params)
	}
}

func extractMetaProperties(selections *ast.SelectionSet) ([]MetaProperty, error) {
	var properties []MetaProperty

	for _, selection := range selections.Selections {
		field := selection.(*ast.Field)
		name := field.Name.Value
		if name == "__typename" {
			continue
		}

		property := MetaProperty{Name: schema.PropertyName(name)}
		analysesProps, err := extractPropertyAnalyses(field.SelectionSet)
		if err != nil {
			return nil, err
		}

		if len(analysesProps) == 0 {
			// this could be the case if the user only asked for __typename, but
			// nothing else, we then don't want to include this property and forwared
			// to the db connector.
			continue
		}

		property.StatisticalAnalyses = analysesProps
		properties = append(properties, property)
	}

	return properties, nil
}

func extractPropertyAnalyses(selections *ast.SelectionSet) ([]StatisticalAnalysis, error) {
	analyses := []StatisticalAnalysis{}
	for _, selection := range selections.Selections {
		spew.Dump(selection)
		field := selection.(*ast.Field)
		name := field.Name.Value

		if name == "__typename" {
			// skip, we want to let graphql serve this internatl meta field, not pass
			// this on to the resolve
			continue
		}

		property, err := parseAnalysisProp(name)
		if err != nil {
			return nil, err
		}

		if property == TopOccurrences {
			// TopOccurrences is the only nested prop for now. It does have two
			// subprops which we predict to be computed in the same query with
			// neglible additional cost. In this case, we can save the effort of
			// actually parsing the subprops and just always return both subprops. If
			// we find this to be too slow (unlikely) and find out that the user
			// always only wants one of the two props (unlikely, as one is
			// meaningless without the other), then we can improve this and actually
			// parse the values.
			analyses = append(analyses, TopOccurrencesValue, TopOccurrencesOccurs)
			continue
		}

		analyses = append(analyses, property)
	}

	return analyses, nil
}

func parseAnalysisProp(name string) (StatisticalAnalysis, error) {
	switch name {
	case string(Type):
		return Type, nil
	case string(Mean):
		return Mean, nil
	case string(Maximum):
		return Maximum, nil
	case string(Minimum):
		return Minimum, nil
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
