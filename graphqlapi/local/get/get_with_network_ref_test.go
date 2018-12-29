package local_get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
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
									AtDataType: []string{"string"},
									Name:       "bestString",
								},
							},
						},
					},
				},
			},
		},
	}
	resolver := newMockResolver(peers)

	expectedParams := &LocalGetClassParams{
		Kind:      kind.THING_KIND,
		ClassName: "SomeThing",
		Properties: []SelectProperty{
			{
				Name:        "NetworkRefField",
				IsPrimitive: false,
				Refs: []SelectClass{
					{
						ClassName: "OtherInstance__SomeRemoteClass",
						RefProperties: []SelectProperty{
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
