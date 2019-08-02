//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

type GetAPI struct {
	Field       *graphql.Field
	Classes     map[string]*graphql.Object
	RefClasses  refclasses.ByNetworkClass
	BeaconClass *graphql.Object
}

// Build the Local.Get part of the graphql tree
func Build(schema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger) (*GetAPI, error) {
	getKinds := graphql.Fields{}

	if len(schema.Actions.Classes) == 0 && len(schema.Things.Classes) == 0 {
		return nil, fmt.Errorf("there are no Actions or Things classes defined yet")
	}

	cb := newClassBuilder(schema, peers, logger)

	if len(schema.Actions.Classes) > 0 {
		actions, err := cb.actions()
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "GetActions",
			Description: descriptions.LocalGetActions,
			Type:        actions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	if len(schema.Things.Classes) > 0 {
		things, err := cb.things()
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "GetThings",
			Description: descriptions.LocalGetThings,
			Type:        things,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	field := graphql.Field{
		Name:        "Get",
		Description: descriptions.LocalGet,
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "GetObj",
			Fields:      getKinds,
			Description: descriptions.LocalGetObj,
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return p.Source, nil
		},
	}

	return &GetAPI{
		Field:       &field,
		Classes:     cb.knownClasses,
		RefClasses:  cb.knownRefClasses,
		BeaconClass: cb.beaconClass,
	}, nil
}
