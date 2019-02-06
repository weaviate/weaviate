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
	"github.com/graphql-go/graphql"
)

// Resolver is a local interface that can be composed with other interfaces to
// form the overall GraphQL API main interface. All data-base connectors that
// want to support the GetMeta feature must implement this interface.
type Resolver interface {
	LocalFetchKindClass(info *Params) (interface{}, error)
}

// Params to describe the Local->GetMeta->Kind->Class query. Will be passed to
// the individual connector methods responsible for resolving the GetMeta
// query.
type Params struct {
	Kind                  kind.Kind
	PossibleClassNames    contextionary.SearchResult
	PossiblePropertyNames contextionary.SearchResult
	PropertyMatch         PropertyMatch
}

type PropertyMatch struct {
	Operator string
	Value    interface{}
}

func makeResolveClass(kind kind.Kind) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
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

		return func() (interface{}, error) {
			return resolver.LocalFetchKindClass(&Params{})
		}, nil

		// selections := p.Info.FieldASTs[0].SelectionSet
		// properties, err := extractMetaProperties(selections)
		// if err != nil {
		// 	return nil, fmt.Errorf("could not extract properties for class '%s': %s", className, err)
		// }

		// filters, err := common_filters.ExtractFilters(p.Args, p.Info.FieldName)
		// if err != nil {
		// 	return nil, fmt.Errorf("could not extract filters: %s", err)
		// }

		// params := &Params{
		// 	Kind:       kind,
		// 	Filters:    filters,
		// 	ClassName:  className,
		// 	Properties: properties,
		// }
		// return resolver.LocalGetMeta(params)
	}
}
