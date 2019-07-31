//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/state"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type Query struct {
	params       *traverser.AggregateParams
	nameSource   nameSource
	typeSource   typeSource
	filterSource filterSource
}

func NewQuery(params *traverser.AggregateParams, nameSource nameSource, typeSource typeSource,
	filterSource filterSource) *Query {
	return &Query{
		params:       params,
		nameSource:   nameSource,
		typeSource:   typeSource,
		filterSource: filterSource,
	}
}

type nameSource interface {
	MustGetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
	MustGetMappedClassName(className schema.ClassName) state.MappedClassName
}

type typeSource interface {
	GetProperty(kind kind.Kind, className schema.ClassName,
		propName schema.PropertyName) (error, *models.Property)
	FindPropertyDataType(dataType []string) (schema.PropertyDataType, error)
}

type filterSource interface {
	String() (string, error)
}

func (b *Query) String() (string, error) {
	q := gremlin.New()

	filterQuery, err := b.filterSource.String()
	if err != nil {
		return "", fmt.Errorf("could not extract filters from Meta query: %s", err)
	}

	q = q.Raw(filterQuery)

	q = q.Raw(b.groupByQuery().String())

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
	propNames := []string{}
	selectQueries := gremlin.New()
	for _, prop := range props {
		propAggregation, err := b.prop(prop)
		if err != nil {
			return nil, fmt.Errorf("could not aggregate prop '%s': %s", prop.Name, err)
		}

		if propAggregation != nil {
			matchQueries = append(matchQueries, propAggregation.matchQueryFragment)

			// Inner selection per aggregation
			selectQueries = selectQueries.Select(propAggregation.selections).
				As(string(prop.Name))

			// Save the prop name for use in the final selection query
			propNames = append(propNames, string(prop.Name))
		}
	}

	if len(propNames) == 1 {
		// similarly to what we did on the inner selection we must add an
		// additional projection if there is only a single property
		selectQueries = selectQueries.Project(string(props[0].Name))
	} else {
		// One final select to group all props together
		selectQueries = selectQueries.Select(propNames)
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

func (b *Query) prop(prop traverser.AggregateProperty) (*propertyAggregation, error) {
	err, parsed := b.typeSource.GetProperty(b.params.Kind, b.params.ClassName, untitle(prop.Name))
	if err != nil {
		return nil, fmt.Errorf("could not find property '%s' in schema: %s", prop.Name, err)
	}

	dataType, err := b.typeSource.FindPropertyDataType(parsed.DataType)
	if err != nil {
		return nil, fmt.Errorf("could not find data type of prop '%s': %s", prop.Name, err)
	}

	if !dataType.IsPrimitive() {
		return b.crefProp(prop)
	}

	switch dataType.AsPrimitive() {
	case schema.DataTypeBoolean, schema.DataTypeString, schema.DataTypeDate:
		return b.nonNumericalProp(prop)
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

	return string(b.nameSource.MustGetMappedPropertyName(className, propName))
}

func (b *Query) mappedClassName(className schema.ClassName) string {
	if b.nameSource == nil {
		return string(className)
	}

	return string(b.nameSource.MustGetMappedClassName(className))
}

func untitle(propName schema.PropertyName) schema.PropertyName {
	asString := string(propName)
	return schema.PropertyName(strings.ToLower(string(asString[0])) + asString[1:])
}
