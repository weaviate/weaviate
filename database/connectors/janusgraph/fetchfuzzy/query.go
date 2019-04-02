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
 */package fetchfuzzy

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Query prepares a Local->Fetch Query. Can be built with String(). Create with
// NewQuery() to be sure that all required properties are set
type Query struct {
	params     []string
	nameSource nameSource
	typeSource typeSource
}

// NewQuery is the preferred way to create a query
func NewQuery(p []string, ns nameSource, ts typeSource) *Query {
	return &Query{
		params:     p,
		nameSource: ns,
		typeSource: ts,
	}
}

type nameSource interface {
	GetMappedPropertyNames(rawProps []schema.ClassAndProperty) ([]state.MappedPropertyName, error)
}

type typeSource interface {
	GetPropsOfType(string) []schema.ClassAndProperty
}

// String builds the query and returns it as a string
func (b *Query) String() (string, error) {

	predicates, err := b.predicates()
	if err != nil {
		return "", err
	}

	q := gremlin.New().
		Raw("g.V()").
		Or(predicates...).
		Limit(20).
		Raw(`.valueMap("uuid", "kind")`)

	return q.String(), nil
}

func (b *Query) predicates() ([]*gremlin.Query, error) {
	var result []*gremlin.Query
	props := b.typeSource.GetPropsOfType("string")

	mappedProps, err := b.nameSource.GetMappedPropertyNames(props)
	if err != nil {
		return nil, fmt.Errorf("could not get mapped names: %v", err)
	}

	// janusgraph does not allow more than 253 arguments, so we must abort once
	// we hit too many
	argsCounter := 0
	limit := 120

outer:
	for _, prop := range mappedProps {
		for _, searchterm := range b.params {
			if argsCounter >= limit {
				break outer
			}

			result = append(result,
				gremlin.New().Has(string(prop), gremlin.New().TextContainsFuzzy(searchterm)),
			)

			argsCounter++
		}
	}

	return result, nil
}
