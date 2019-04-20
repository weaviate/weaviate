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
package meta

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/adapters/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

type Query struct {
	params       *kinds.GetMetaParams
	nameSource   nameSource
	typeSource   typeSource
	filterSource filterSource
}

func NewQuery(params *kinds.GetMetaParams, nameSource nameSource, typeSource typeSource,
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

	props := b.params.Properties
	propQueries := []*gremlin.Query{}

	for _, prop := range props {
		propQuery, err := b.prop(prop)
		if err != nil {
			return "", fmt.Errorf("could not build get meta query for prop '%s': %s", prop.Name, err)
		}

		if propQuery != nil {
			propQueries = append(propQueries, propQuery)
		}
	}

	filterQuery, err := b.filterSource.String()
	if err != nil {
		return "", fmt.Errorf("could not extract filters from GetMeta query: %s", err)
	}

	q = q.Raw(filterQuery)
	q = q.Union(propQueries...).
		Group().ByQuery(gremlin.New().SelectKeys().Unfold()).ByQuery(
		gremlin.New().SelectValues().Unfold().Group().
			ByQuery(gremlin.New().SelectKeys().Unfold()).
			ByQuery(gremlin.New().SelectValues().Unfold()),
	)
	return idempotentLeadWithDot(q), nil
}

func idempotentLeadWithDot(q *gremlin.Query) string {
	stringified := q.String()
	if string(stringified[0]) == "." {
		return stringified
	}

	return fmt.Sprintf(".%s", stringified)
}

func (b *Query) prop(prop kinds.MetaProperty) (*gremlin.Query, error) {
	if prop.Name == MetaProp {
		return b.metaProp(prop)
	}

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
	case schema.DataTypeBoolean:
		return b.booleanProp(prop)
	case schema.DataTypeString, schema.DataTypeDate, schema.DataTypeText:
		return b.stringProp(prop)
	case schema.DataTypeInt, schema.DataTypeNumber:
		return b.intProp(prop)
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

func untitle(propName schema.PropertyName) schema.PropertyName {
	asString := string(propName)
	return schema.PropertyName(strings.ToLower(string(asString[0])) + asString[1:])
}
