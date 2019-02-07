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
	Kind                  kind.Kind
	PossibleClassNames    contextionary.SearchResults
	PossiblePropertyNames contextionary.SearchResults
	PropertyMatch         PropertyMatch
}

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

		possibleProperties, err := resources.contextionary.SchemaSearch(where.property)
		if err != nil {
			return nil, err
		}

		params := &Params{
			PossibleClassNames:    possibleClasses,
			PossiblePropertyNames: possibleProperties,
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

type whereFilter struct {
	class         contextionary.SearchParams
	property      contextionary.SearchParams
	propertyMatch PropertyMatch
}

func parseWhere(args map[string]interface{}) (*whereFilter, error) {
	// the structure is already guaranteed by graphQL, we can therefore make
	// plenty of assertions without having to check. If required fields are not
	// set, graphQL will error before already and we won't get here.
	where := args["where"].(map[string]interface{})

	class, _ := where["class"].([]interface{})
	if len(class) > 1 {
		panic("only one class supported for now")
	}
	classMap := class[0].(map[string]interface{})
	classKeywords := extractKeywords(classMap["keywords"])

	properties, _ := where["properties"].([]interface{})
	if len(properties) > 1 {
		panic("only one property supported for now")
	}
	propertiesMap := properties[0].(map[string]interface{})
	propertiesKeywords := extractKeywords(propertiesMap["keywords"])

	return &whereFilter{
		class: contextionary.SearchParams{
			SearchType: contextionary.SearchTypeClass,
			Name:       classMap["name"].(string),
			Certainty:  float32(classMap["certainty"].(float64)),
			Keywords:   classKeywords,
		},
		property: contextionary.SearchParams{
			SearchType: contextionary.SearchTypeProperty,
			Name:       propertiesMap["name"].(string),
			Certainty:  float32(propertiesMap["certainty"].(float64)),
			Keywords:   propertiesKeywords,
		},
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
