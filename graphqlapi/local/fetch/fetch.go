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

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/utils"
	"github.com/graphql-go/graphql"
)

// Build builds the object containing the Local->Fetch Fields, such as Things/Actions
func Build() *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateLocalFetch",
		Description: descriptions.LocalFetch,
		Type:        fetchObj(nil),
		Resolve:     bubbleUpResolver,
	}
}

func fetchObj(filterContainer *utils.FilterContainer) *graphql.Object {

	fields := graphql.Fields{
		"Actions": &graphql.Field{
			Name:        "WeaviateLocalFetchActions",
			Description: descriptions.LocalFetchActions,
			Type:        graphql.NewList(kindFieldsObj(kind.ACTION_KIND)),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterField(kind.ACTION_KIND),
			},
			Resolve: makeResolveClass(kind.ACTION_KIND),
		},

		"Things": &graphql.Field{
			Name:        "WeaviateLocalFetchThings",
			Description: descriptions.LocalFetchThings,
			Type:        graphql.NewList(kindFieldsObj(kind.THING_KIND)),
			Args: graphql.FieldConfigArgument{
				"where": whereFilterField(kind.THING_KIND),
			},
			Resolve: makeResolveClass(kind.THING_KIND),
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

func whereFilterField(k kind.Kind) *graphql.ArgumentConfig {
	whereFilterFields := &graphql.ArgumentConfig{
		Description: descriptions.FetchWhereFilterFields,
		Type: graphql.NewNonNull(graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("WeaviateLocalFetch%sWhereInpObj", k.TitleizedName()),
				Fields:      whereFilterFields(k),
				Description: descriptions.FetchWhereFilterFieldsInpObj,
			},
		)),
	}

	return whereFilterFields
}

func bubbleUpResolver(p graphql.ResolveParams) (interface{}, error) {
	return p.Source, nil
}
