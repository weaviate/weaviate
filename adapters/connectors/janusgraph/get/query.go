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

package get

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/filters"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/state"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// Query prepares a Local->Fetch Query. Can be built with String(). Create with
// NewQuery() to be sure that all required properties are set
type Query struct {
	params     traverser.LocalGetParams
	nameSource nameSource
	typeSource typeSource
	defaults   config.QueryDefaults
}

var isNetworkRef *regexp.Regexp

func init() {
	isNetworkRef = regexp.MustCompile("^\\w*__")
}

// NewQuery is the preferred way to create a query
func NewQuery(p traverser.LocalGetParams, ns nameSource, ts typeSource, d config.QueryDefaults) *Query {
	return &Query{
		params:     p,
		nameSource: ns,
		typeSource: ts,
		defaults:   d,
	}
}

// nameSource can only be used after verifying that the property exists, if in
// doubt use typeSource first
type nameSource interface {
	MustGetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
	MustGetPropertyNameFromMapped(className schema.ClassName, mappedPropName state.MappedPropertyName) schema.PropertyName
	GetPropertyNameFromMapped(className schema.ClassName, mappedPropName state.MappedPropertyName) (schema.PropertyName, error)
	MustGetMappedClassName(className schema.ClassName) state.MappedClassName
	GetClassNameFromMapped(className state.MappedClassName) schema.ClassName
}

type typeSource interface {
	GetProperty(kind kind.Kind, className schema.ClassName,
		propName schema.PropertyName) (error, *models.SemanticSchemaClassProperty)
	FindPropertyDataType(dataType []string) (schema.PropertyDataType, error)
}

// String builds the query and returns it as a string
func (b *Query) String() (string, error) {
	var filterQuery string
	var refPropQueries string
	var err error
	var limit = int(b.defaults.Limit)

	if b.params.Pagination != nil {
		limit = b.params.Pagination.Limit
	}

	if b.params.Filters != nil {
		if filterQuery, err = filters.New(b.params.Filters, b.nameSource).String(); err != nil {
			return "", fmt.Errorf("could not build filter query: %s", err)
		}
	}

	if refPropQueries, err = b.refPropQueryWrapper(); err != nil {
		return "", fmt.Errorf("could not build ref prop queries: %s", err)
	}

	q := gremlin.New().Raw("g.V()").
		HasString("kind", b.params.Kind.Name()).
		HasLabel(string(b.nameSource.MustGetMappedClassName(schema.ClassName(b.params.ClassName)))).
		Raw(filterQuery).
		Raw(refPropQueries).
		Limit(limit).
		Path().
		ByQuery(gremlin.New().Raw("valueMap()"))

	return q.String(), nil
}

func (b *Query) refPropQueryWrapper() (string, error) {

	queries := b.refPropQueries(b.params.Properties, b.params.ClassName)
	if len(queries) == 0 {
		return "", nil
	}

	return "." + gremlin.New().Union(queries...).String(), nil
}

func (b *Query) refPropQueries(props []traverser.SelectProperty, className string) []*gremlin.Query {
	var queries []*gremlin.Query
	for _, prop := range props {
		if propQueries := b.refPropQuery(prop, className); propQueries != nil {
			queries = append(queries, propQueries...)
		}
	}

	return queries
}

func (b *Query) refPropQuery(prop traverser.SelectProperty, className string) []*gremlin.Query {
	if prop.IsPrimitive {
		return nil
	}

	propName := string(b.nameSource.MustGetMappedPropertyName(schema.ClassName(className),
		untitle(prop.Name)))

	var queries []*gremlin.Query

	var q *gremlin.Query
	for _, refClass := range prop.Refs {
		if isNetworkRef.MatchString(refClass.ClassName) {
			q = gremlin.New().Optional(gremlin.New().OutEWithLabel(propName).InV())
		} else {
			className := string(b.nameSource.MustGetMappedClassName(schema.ClassName(refClass.ClassName)))
			q = gremlin.New().Optional(gremlin.New().OutEWithLabel(propName).InV().HasLabel(className))
		}

		nestedQueries := b.refPropQueries(refClass.RefProperties, refClass.ClassName)
		if len(nestedQueries) == 0 {
			queries = append(queries, q)
			continue

		}

		// now we need to combine every possible nested query with what we have so far
		var innerQueries []*gremlin.Query
		for _, nestedQ := range nestedQueries {
			innerQueries = append(innerQueries, gremlin.New().Raw(q.String()).Raw(".").Raw(nestedQ.String()))
		}

		queries = append(queries, innerQueries...)

	}

	return queries
}

func untitle(propName string) schema.PropertyName {
	return schema.PropertyName(strings.ToLower(string(propName[0])) + propName[1:])
}
