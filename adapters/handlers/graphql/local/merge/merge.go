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

package merge

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

// Build the Local.Merge part of the graphql tree
func Build(schema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger,
	classes map[string]*graphql.Object, refClasses refclasses.ByNetworkClass, beaconClass *graphql.Object) (*graphql.Field, error) {
	mergeKinds := graphql.Fields{}

	if len(schema.Actions.Classes) == 0 && len(schema.Things.Classes) == 0 {
		return nil, fmt.Errorf("there are no Actions or Things classes defined yet")
	}

	cb := newClassBuilder(schema, peers, logger, classes, refClasses, beaconClass)

	if len(schema.Actions.Classes) > 0 {
		actions, err := cb.actions()
		if err != nil {
			return nil, err
		}

		mergeKinds["Actions"] = &graphql.Field{
			Name:        "MergeActions",
			Description: descriptions.LocalMergeActions,
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

		mergeKinds["Things"] = &graphql.Field{
			Name:        "MergeThings",
			Description: descriptions.LocalMergeThings,
			Type:        things,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	field := graphql.Field{
		Name:        "Merge",
		Description: descriptions.LocalMerge,
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "MergeObj",
			Fields:      mergeKinds,
			Description: descriptions.LocalMergeObj,
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return p.Source, nil
		},
	}

	return &field, nil
}
