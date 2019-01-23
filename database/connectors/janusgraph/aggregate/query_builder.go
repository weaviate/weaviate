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
package aggregate

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
)

type Query struct {
	params       *aggregate.Params
	nameSource   nameSource
	typeSource   typeSource
	filterSource filterSource
}

func NewQuery(params *aggregate.Params, nameSource nameSource, typeSource typeSource,
	filterSource filterSource) *Query {
	return &Query{
		params:       params,
		nameSource:   nameSource,
		typeSource:   typeSource,
		filterSource: filterSource,
	}
}

type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
}

type typeSource interface {
	GetProperty(kind kind.Kind, className schema.ClassName,
		propName schema.PropertyName) (error, *models.SemanticSchemaClassProperty)
	FindPropertyDataType(dataType []string) (schema.PropertyDataType, error)
}

type filterSource interface {
	String() (string, error)
}

func (b *Query) String() (string, error) {
	q := gremlin.New()

	filterQuery, err := b.filterSource.String()
	if err != nil {
		return "", fmt.Errorf("could not extract filters from GetMeta query: %s", err)
	}

	q = q.Raw(filterQuery)

	// add grouping
	// for now pretend we can only group by primitive props
	q = q.Group().By(b.mappedPropertyName(b.params.GroupBy.Class, b.params.GroupBy.Property))

	// add aggregation
	aggregationQuery, err := b.aggregationProperties()
	if err != nil {
		return "", fmt.Errorf("could not build aggregation query: %s", err)
	}

	q = q.Raw(aggregationQuery.String())

	return idempotentLeadWithDot(q), nil
}

func (b *Query) aggregationProperties() (*gremlin.Query, error) {
	props := b.params.Properties
	matchQueries := []*gremlin.Query{}
	propsProcessed := 0
	selectQueries := gremlin.New()
	for _, prop := range props {
		propAggregation, err := b.prop(prop)
		if err != nil {
			return nil, fmt.Errorf("could not aggregate prop '%s': %s", prop.Name, err)
		}

		if propAggregation != nil {
			matchQueries = append(matchQueries, propAggregation.matchQueryFragment)

			// Inner selection per aggregation
			selectQueries = selectQueries.Select(propAggregation.selections)
			if len(propAggregation.selections) == 1 {
				// just one selection/aggregation prop is a special case, because in
				// multiple cases we are using select(<1>,<2>, ...<n>), this means we
				// will receive a map that has the selections as keys. However, if we
				// only ask for a single prop, Gremlin doesn't see a need to return a
				// map and simply returns the primitive value. This will of course
				// either break our post-processing or will not have the format the
				// graphql API expects. We thus need to add an additional
				// .by(project("label")) step to wrap the primtive prop in a map - but
				// only if it's only a single analysis prop.
				selectQueries = selectQueries.ByQuery(gremlin.New().Project(propAggregation.selections[0]))
			}

			// Group and select inner props into outer selection per Class Property
			selectQueries = selectQueries.
				As(string(prop.Name)).
				Select([]string{string(prop.Name)})
			propsProcessed++
		}
	}

	if propsProcessed == 1 {
		// similarly to what we did on the inner selection we must add an
		// additional projection if there is only a single property
		selectQueries = selectQueries.ByQuery(gremlin.New().Project(string(props[0].Name)))
	}

	return gremlin.New().ByQuery(
		gremlin.New().Fold().Match(matchQueries...).
			Raw(selectQueries.String()),
	), nil
}

func idempotentLeadWithDot(q *gremlin.Query) string {
	stringified := q.String()
	if string(stringified[0]) == "." {
		return stringified
	}

	return fmt.Sprintf(".%s", stringified)
}

// we need to end up with a query like so (it's pseudo-gremlin to illustrate
// what we need):
//
// group().by().match(
//  <prop1Match1>,
//  <prop1Match2>,
//  <prop2Match1>,
//  <prop2Match2>,
// )
// .select(prop1Match1, prop1Match2).as(prop1)
// .select(prop2Match1, prop2Match2).as(prop2)
//
// This means match spans all props, therefore it must be constructed at root
// level. However, additionally, each prop provides n selections, which have to
// be combined outside of the match query. Therefore it is not enough for one
// property method to just provide a matchQueryFragment, it must also provide
// the correct selections.
type propertyAggregation struct {
	matchQueryFragment *gremlin.Query
	selections         []string
}

func (b *Query) prop(prop aggregate.Property) (*propertyAggregation, error) {
	err, parsed := b.typeSource.GetProperty(b.params.Kind, b.params.ClassName, untitle(prop.Name))
	if err != nil {
		return nil, fmt.Errorf("could not find property '%s' in schema: %s", prop.Name, err)
	}

	dataType, err := b.typeSource.FindPropertyDataType(parsed.AtDataType)
	if err != nil {
		return nil, fmt.Errorf("could not find data type of prop '%s': %s", prop.Name, err)
	}

	// if !dataType.IsPrimitive() {
	// 	return b.crefProp(prop)
	// }

	switch dataType.AsPrimitive() {
	// case schema.DataTypeBoolean:
	// 	return b.booleanProp(prop)
	// case schema.DataTypeString, schema.DataTypeDate:
	// 	return b.stringProp(prop)
	case schema.DataTypeInt, schema.DataTypeNumber:
		return b.numericalProp(prop)
	default:
		return nil, fmt.Errorf("unsupported primitive property data type: %s", dataType.AsPrimitive())
	}
}

func (b *Query) mappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) string {
	if b.nameSource == nil {
		return string(propName)
	}

	return string(b.nameSource.GetMappedPropertyName(className, propName))
}

func untitle(propName schema.PropertyName) schema.PropertyName {
	asString := string(propName)
	return schema.PropertyName(strings.ToLower(string(asString[0])) + asString[1:])
}
