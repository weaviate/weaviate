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

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network/common"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
)

// Resolver describes the dependencies of this package
type Resolver interface {
	ProxyFetch(query common.SubQuery) ([]*models.GraphQLResponse, error)
}

func makeResolveKind(k kind.Kind) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		resolver, ok := p.Source.(Resolver)
		if !ok {
			return nil, fmt.Errorf("expected source to be a Resolver, but was \n%#v",
				p.Source)
		}

		astLoc := p.Info.FieldASTs[0].GetLoc()
		rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
		subquery := common.ParseSubQuery(rawSubQuery).
			WrapInFetchQuery().
			WrapInLocalQuery().
			WrapInBraces()

		graphQLResponses, err := resolver.ProxyFetch(subquery)
		if err != nil {
			return nil, fmt.Errorf("could not proxy to remote instance: %s", err)
		}

		return extractKindsResults(k, graphQLResponses)
	}
}

func extractKindsResults(k kind.Kind, responses []*models.GraphQLResponse) ([]interface{}, error) {
	kind := fmt.Sprintf("%ss", k.TitleizedName())
	results := []interface{}{}

	for _, response := range responses {
		local, ok := response.Data["Local"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local to be map[string]interface{}, but response was %#v",
				response.Data["Local"])
		}

		fetch, ok := local["Fetch"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local.Fetch to be map[string]interface{}, but response was %#v",
				local["Fetch"])
		}

		peerResults, ok := fetch[kind].([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local.Fetch.%s to be a slice, but response was %#v",
				kind, fetch[kind])
		}

		results = append(results, peerResults...)
	}

	return results, nil
}

func resolveFuzzy(p graphql.ResolveParams) (interface{}, error) {
	resolver, ok := p.Source.(Resolver)
	if !ok {
		return nil, fmt.Errorf("expected source to be a Resolver, but was \n%#v",
			p.Source)
	}

	astLoc := p.Info.FieldASTs[0].GetLoc()
	rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
	subquery := common.ParseSubQuery(rawSubQuery).
		WrapInFetchQuery().
		WrapInLocalQuery().
		WrapInBraces()

	graphQLResponses, err := resolver.ProxyFetch(subquery)
	if err != nil {
		return nil, fmt.Errorf("could not proxy to remote instance: %s", err)
	}

	return extractFuzzyResults(graphQLResponses)
}

func extractFuzzyResults(responses []*models.GraphQLResponse) ([]interface{}, error) {
	results := []interface{}{}

	for _, response := range responses {
		local, ok := response.Data["Local"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local to be map[string]interface{}, but response was %#v",
				response.Data["Local"])
		}

		fetch, ok := local["Fetch"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local.Fetch to be map[string]interface{}, but response was %#v",
				local["Fetch"])
		}

		peerResults, ok := fetch["Fuzzy"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local.Fetch.Fuzzy to be a slice, but response was %#v",
				fetch["Fuzzy"])
		}

		results = append(results, peerResults...)
	}

	return results, nil
}
