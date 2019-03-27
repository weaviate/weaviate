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
	"github.com/davecgh/go-spew/spew"
	"github.com/graphql-go/graphql"
)

// Resolver describes the dependencies of this package
type Resolver interface {
	ProxyFetch(query common.SubQuery) (*models.GraphQLResponse, error)
}

func makeResolve(k kind.Kind) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		resolver, ok := p.Source.(Resolver)
		if !ok {
			return nil, fmt.Errorf("expected source to be a Resolver, but was \n%#v",
				p.Source)
		}

		astLoc := p.Info.FieldASTs[0].GetLoc()
		rawSubQuery := astLoc.Source.Body[astLoc.Start:astLoc.End]
		subquery := common.ParseSubQuery(rawSubQuery)

		graphQLResponse, err := resolver.ProxyFetch(subquery)
		if err != nil {
			return nil, fmt.Errorf("could not proxy to remote instance: %s", err)
		}

		local, ok := graphQLResponse.Data["Local"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local to be map[string]interface{}, but response was %#v",
				graphQLResponse.Data["Local"])
		}

		fetch, ok := local["Fetch"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected response.data.Local.Fetch to be map[string]interface{}, but response was %#v",
				graphQLResponse.Data["Local"])
		}

		spew.Dump(subquery)
		return fetch[fmt.Sprintf("%ss", k.TitleizedName())], nil
	}
}
