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

package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalFetchKindClass(info *Params) (interface{}, error)
}

// Contextionary is a local abstraction on the contextionary that needs to be
// provided to the graphQL API in order to resolve Local.Fetch queries.
type Contextionary interface {
	SchemaSearch(p contextionary.SearchParams) (contextionary.SearchResults, error)
}

// Params to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type Params struct {
	Kind               kind.Kind
	PossibleClassNames contextionary.SearchResults
	Properties         []Property
}

// Property is a combination of possible names to use for the property as well
// as a match object to perform filtering actions in the db connector based on
// this property
type Property struct {
	PossibleNames contextionary.SearchResults
	Match         PropertyMatch
}

// PropertyMatch defines how in the db connector this property should be used
// as a filter
type PropertyMatch struct {
	Operator string
	Value    interface{}
}

func makeResolveClass(kind kind.Kind) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		resources, err := newResources(p.Source)
		if err != nil {
			return nil, err
		}

		where, err := parseWhere(p.Args)
		if err != nil {
			return nil, fmt.Errorf("invalid where filter: %s", err)
		}

		possibleClasses, err := resources.contextionary.SchemaSearch(where.class)
		if err != nil {
			return nil, err
		}

		properties, err := addPossibleNamesToProperties(where.properties, resources.contextionary)
		if err != nil {
			return nil, err
		}

		params := &Params{
			PossibleClassNames: possibleClasses,
			Properties:         properties,
		}

		return func() (interface{}, error) {
			return resources.resolver.LocalFetchKindClass(params)
		}, nil
	}
}

type resources struct {
	resolver      Resolver
	contextionary Contextionary
}

func newResources(s interface{}) (*resources, error) {
	source, ok := s.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected source to be a map, but was %T", source)
	}

	resolver, ok := source["Resolver"].(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %T", source)
	}

	contextionary, ok := source["Contextionary"].(Contextionary)
	if !ok {
		return nil, fmt.Errorf("expected source to contain a usable Resolver, but was %T", source)
	}

	return &resources{
		resolver:      resolver,
		contextionary: contextionary,
	}, nil
}

func addPossibleNamesToProperties(whereProperties []whereProperty,
	contextionary Contextionary) ([]Property, error) {
	properties := make([]Property, len(whereProperties), len(whereProperties))
	for i, whereProp := range whereProperties {
		possibleNames, err := contextionary.SchemaSearch(whereProp.search)
		if err != nil {
			return nil, err
		}
		properties[i] = Property{
			PossibleNames: possibleNames,
			Match:         whereProp.match,
		}
	}

	return properties, nil
}

type whereFilter struct {
	class      contextionary.SearchParams
	properties []whereProperty
}

type whereProperty struct {
	search contextionary.SearchParams
	match  PropertyMatch
}

func parseWhere(args map[string]interface{}) (*whereFilter, error) {
	// the structure is already guaranteed by graphQL, we can therefore make
	// plenty of assertions without having to check. If required fields are not
	// set, graphQL will error before already and we won't get here.
	where := args["where"].(map[string]interface{})
	classMap := where["class"].(map[string]interface{})
	classKeywords := extractKeywords(classMap["keywords"])

	propertiesRaw := where["properties"].([]interface{})
	properties := make([]whereProperty, len(propertiesRaw), len(propertiesRaw))

	for i, prop := range propertiesRaw {
		propertiesMap := prop.(map[string]interface{})
		propertiesKeywords := extractKeywords(propertiesMap["keywords"])
		search := contextionary.SearchParams{
			SearchType: contextionary.SearchTypeProperty,
			Name:       propertiesMap["name"].(string),
			Certainty:  float32(propertiesMap["certainty"].(float64)),
			Keywords:   propertiesKeywords,
		}

		properties[i] = whereProperty{
			search: search,
		}
	}

	return &whereFilter{
		class: contextionary.SearchParams{
			SearchType: contextionary.SearchTypeClass,
			Name:       classMap["name"].(string),
			Certainty:  float32(classMap["certainty"].(float64)),
			Keywords:   classKeywords,
		},
		properties: properties,
	}, nil
}

func extractKeywords(kw interface{}) models.SemanticSchemaKeywords {
	if kw == nil {
		return nil
	}

	asSlice := kw.([]interface{})
	result := make(models.SemanticSchemaKeywords, len(asSlice), len(asSlice))
	for i, keyword := range asSlice {
		keywordMap := keyword.(map[string]interface{})
		result[i] = &models.SemanticSchemaKeywordsItems0{
			Keyword: keywordMap["value"].(string),
			Weight:  float32(keywordMap["weight"].(float64)),
		}
	}

	return result
}
