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
 */package fetch

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Query prepares a Local->Fetch Query. Can be built with String(). Create with
// NewQuery() to be sure that all required properties are set
type Query struct {
	params     fetch.Params
	nameSource nameSource
	typeSource typeSource
}

// NewQuery is the preferred way to create a query
func NewQuery(p fetch.Params, ns nameSource, ts typeSource) *Query {
	return &Query{
		params:     p,
		nameSource: ns,
		typeSource: ts,
	}
}

// nameSource can only be used after verifying that the property exists, if in
// doubt use typeSource first
type nameSource interface {
	MustGetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
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
	properties, err := b.properties()
	if err != nil {
		return "", err
	}

	if len(properties) == 0 {
		return "", fmt.Errorf("could not find a viable combination of class names, properties and search filters, " +
			"try using different classNames or properties, lowering the certainty on either of them or change the " +
			"specified filtering requirements (operator or value<Type>)")
	}

	q := gremlin.New().
		Raw("g.V()").
		HasString("kind", b.params.Kind.Name()).
		And(properties...).
		Raw(`.valueMap("uuid", "classId")`)

	return q.String(), nil
}

func (b *Query) properties() ([]*gremlin.Query, error) {
	var queries []*gremlin.Query

	for _, prop := range b.params.Properties {
		q, err := b.property(prop)
		if err != nil {
			return nil, fmt.Errorf("could not build property '%v': %s", prop, err)
		}

		if q == nil {
			continue
		}

		queries = append(queries, q)
	}

	if len(queries) == 0 {
		return nil, nil
	}

	return queries, nil
}

func (b *Query) property(prop fetch.Property) (*gremlin.Query, error) {
	var filterAlternatives []*gremlin.Query

	for _, className := range b.params.PossibleClassNames.Results {
		alternative := b.combineClassWithPropNames(className.Name, prop.PossibleNames.Results, prop.Match)
		if alternative == nil {
			continue
		}

		filterAlternatives = append(filterAlternatives, alternative)
	}

	if len(filterAlternatives) == 0 {
		return nil, nil
	}

	q := gremlin.New().
		Or(filterAlternatives...)

	return q, nil
}

func (b *Query) combineClassWithPropNames(className string, propNames []contextionary.SearchResult,
	match fetch.PropertyMatch) *gremlin.Query {
	var combinations []string

	for _, propName := range propNames {
		combination := b.combineClassWithPropName(className, propName.Name, match)
		if combination == nil {
			continue
		}

		combinations = append(combinations, combination.String())
	}

	if len(combinations) == 0 {
		return nil
	}

	return gremlin.New().Raw(strings.Join(combinations, ", "))
}

func (b *Query) combineClassWithPropName(className string, propName string,
	match fetch.PropertyMatch) *gremlin.Query {
	class := schema.ClassName(className)
	prop := schema.PropertyName(propName)

	err, schemaProp := b.typeSource.GetProperty(b.params.Kind, class, prop)
	if err != nil {
		// this class property combination does not exist, simply skip it
		return nil
	}

	propType, err := b.typeSource.FindPropertyDataType(schemaProp.DataType)
	if err != nil {
		// this class property combination does not exist, simply skip it
		return nil
	}

	if !propType.IsPrimitive() {
		// skip reference props
		return nil
	}

	if propType.AsPrimitive() != match.Value.Type {
		// this prop is of a different type than the match we provided. we have to
		// skip it our we would run into type errors in the db
		return nil
	}

	mappedClass := b.nameSource.MustGetMappedClassName(class)
	mappedProp := b.nameSource.MustGetMappedPropertyName(class, prop)

	condition, err := b.conditionQuery(match)
	if err != nil {
		// if the combination is not possible, don't include this alternative
		return nil
	}

	return gremlin.New().
		HasString("classId", string(mappedClass)).
		Has(string(mappedProp), condition)
}
