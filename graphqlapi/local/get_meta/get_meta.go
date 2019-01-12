/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package get_meta provides the local get meta graphql endpoint for Weaviate
package get_meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/graphql-go/graphql"
)

// Build the local queries from the database schema.
func Build(dbSchema *schema.Schema) (*graphql.Field, error) {

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("There are no Actions or Things classes defined yet.")
	}

	getMetaKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) > 0 {
		classParentTypeIsAction := true
		localGetMetaActions, localGetMetaErr := classFields(dbSchema.Actions.Classes, classParentTypeIsAction)
		if localGetMetaErr != nil {
			return nil, fmt.Errorf("failed to generate action fields from schema for local MetaGet because: %v", localGetMetaErr)
		}

		getMetaKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalGetMetaActions",
			Description: descriptions.LocalGetMetaActionsDesc,
			Type:        localGetMetaActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// bubble up root resolver
				return p.Source, nil
			},
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		classParentTypeIsAction := false
		localGetMetaThings, localGetMetaErr := classFields(dbSchema.Things.Classes, classParentTypeIsAction)
		if localGetMetaErr != nil {
			return nil, fmt.Errorf("failed to generate thing fields from schema for local MetaGet because: %v", localGetMetaErr)
		}

		getMetaKinds["Things"] = &graphql.Field{
			Name:        "WeaviateLocalGetMetaThings",
			Description: descriptions.LocalGetMetaThingsDesc,
			Type:        localGetMetaThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// bubble up root resolver
				return p.Source, nil
			},
		}
	}

	getMetaObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        "WeaviateLocalGetMetaObj",
		Fields:      getMetaKinds,
		Description: descriptions.LocalGetObjDesc,
	})

	localField := &graphql.Field{
		Name:        "WeaviateLocalGetMeta",
		Type:        getMetaObj,
		Description: descriptions.LocalGetMetaDesc,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			// bubble up root resolver
			return p.Source, nil
		},
	}

	return localField, nil
}
