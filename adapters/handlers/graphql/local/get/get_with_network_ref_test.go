/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package get

import (
	"testing"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func TestLocalGetWithNetworkRef(t *testing.T) {
	t.Parallel()

	peers := peers.Peers{
		peers.Peer{
			Name: "OtherInstance",
			Schema: schema.Schema{
				Things: &models.SemanticSchema{
					Classes: []*models.SemanticSchemaClass{
						&models.SemanticSchemaClass{
							Class: "SomeRemoteClass",
							Properties: []*models.SemanticSchemaClassProperty{
								&models.SemanticSchemaClassProperty{
									DataType: []string{"string"},
									Name:     "bestString",
								},
							},
						},
					},
				},
			},
		},
	}
	resolver := newMockResolver(peers)

	expectedParams := &traverser.LocalGetParams{
		Kind:      kind.Thing,
		ClassName: "SomeThing",
		Properties: []traverser.SelectProperty{
			{
				Name:        "NetworkRefField",
				IsPrimitive: false,
				Refs: []traverser.SelectClass{
					{
						ClassName: "OtherInstance__SomeRemoteClass",
						RefProperties: []traverser.SelectProperty{
							{
								Name:        "bestString",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(helper.EmptyListThunk(), nil).Once()

	query := "{ Get { Things { SomeThing { NetworkRefField { ... on OtherInstance__SomeRemoteClass { bestString } } } } } }"
	resolver.AssertResolve(t, query)

}
