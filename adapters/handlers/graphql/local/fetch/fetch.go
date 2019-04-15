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

// Package fetch provides the Local->Fetch GraphQL API
package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common/fetch"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/graphql-go/graphql"
)

// Build builds the object containing the Local->Fetch Fields, such as Things/Actions
func Build() *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateLocalFetch",
		Description: descriptions.LocalFetch,
		Type:        fetchObj(),
		Resolve:     bubbleUpResolver,
	}
}

func fetchObj() *graphql.Object {

	fields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalFetchActions",
			Description: descriptions.LocalFetchActions,
			Type:        graphql.NewList(kindFieldsObj(kind.ACTION_KIND)),
			Args: graphql.FieldConfigArgument{
				"where": fetch.NewFilterBuilder(kind.ACTION_KIND, "WeaviateLocal").Build(),
			},
			Resolve: makeResolveClass(kind.ACTION_KIND),
		},

		"Things": &graphql.Field{
			Name:        "WeaviateLocalFetchThings",
			Description: descriptions.LocalFetchThings,
			Type:        graphql.NewList(kindFieldsObj(kind.THING_KIND)),
			Args: graphql.FieldConfigArgument{
				"where": fetch.NewFilterBuilder(kind.THING_KIND, "WeaviateLocal").Build(),
			},
			Resolve: makeResolveClass(kind.THING_KIND),
		},

		"Fuzzy": &graphql.Field{
			Name:        "WeaviateNetworkFetchFuzzy",
			Description: descriptions.NetworkFetchFuzzy,
			Type:        graphql.NewList(fuzzyFieldsObj()),
			Args: graphql.FieldConfigArgument{
				"value": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyValue,
					Type:        graphql.NewNonNull(graphql.String),
				},
				"certainty": &graphql.ArgumentConfig{
					Description: descriptions.FetchFuzzyCertainty,
					Type:        graphql.NewNonNull(graphql.Float),
				},
			},
			Resolve: resolveFuzzy,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateLocalFetchObj",
		Fields:      fields,
		Description: descriptions.LocalFetchObj,
	})
}

func kindFieldsObj(k kind.Kind) *graphql.Object {
	fields := graphql.Fields{
		"className": &graphql.Field{
			Name:        fmt.Sprintf("WeaviateLocalFetch%sClassName", k.TitleizedName()),
			Description: descriptions.LocalFetchClassName,
			Type:        graphql.String,
		},

		"beacon": &graphql.Field{
			Name:        fmt.Sprintf("WeaviateLocalFetch%sBeacon", k.TitleizedName()),
			Description: descriptions.LocalFetchBeacon,
			Type:        graphql.String,
		},

		"certainty": &graphql.Field{
			Name:        fmt.Sprintf("WeaviateLocalFetch%sCertainty", k.TitleizedName()),
			Description: descriptions.LocalFetchCertainty,
			Type:        graphql.Float,
		},
	}

	var desc string
	switch k {
	case kind.THING_KIND:
		desc = descriptions.LocalFetchThingsObj
	case kind.ACTION_KIND:
		desc = descriptions.LocalFetchActionsObj
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalFetch%sObj", k.TitleizedName()),
		Fields:      fields,
		Description: desc,
	})
}

func fuzzyFieldsObj() *graphql.Object {
	getLocalFetchFuzzyFields := graphql.Fields{
		"className": &graphql.Field{
			Name:        "WeaviateLocalFetchFuzzyClassName",
			Description: descriptions.LocalFetchFuzzyClassName,
			Type:        graphql.String,
		},

		"beacon": &graphql.Field{
			Name:        "WeaviateLocalFetchFuzzyBeacon",
			Description: descriptions.LocalFetchFuzzyBeacon,
			Type:        graphql.String,
		},

		"certainty": &graphql.Field{
			Name:        "WeaviateLocalFetchFuzzyCertainty",
			Description: descriptions.LocalFetchFuzzyCertainty,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
	}

	getLocalFetchFuzzyFieldsObject := graphql.ObjectConfig{
		Name:        "WeaviateLocalFetchFuzzyObj",
		Fields:      getLocalFetchFuzzyFields,
		Description: descriptions.LocalFetchFuzzyObj,
	}

	return graphql.NewObject(getLocalFetchFuzzyFieldsObject)
}

func bubbleUpResolver(p graphql.ResolveParams) (interface{}, error) {
	return p.Source, nil
}
