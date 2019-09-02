//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

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

func TestGetWithNetworkRef(t *testing.T) {
	t.Parallel()

	peers := peers.Peers{
		peers.Peer{
			Name: "OtherInstance",
			Schema: schema.Schema{
				Things: &models.Schema{
					Classes: []*models.Class{
						&models.Class{
							Class: "SomeRemoteClass",
							Properties: []*models.Property{
								&models.Property{
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

	expectedParams := &traverser.GetParams{
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

	resolver.On("GetClass", expectedParams).
		Return(helper.EmptyListThunk(), nil).Once()

	query := "{ Get { Things { SomeThing { NetworkRefField { ... on OtherInstance__SomeRemoteClass { bestString } } } } } }"
	resolver.AssertResolve(t, query)

}
