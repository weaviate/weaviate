//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package get

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
)

func TestGetWithNetworkRefResolvesCorrectly(t *testing.T) {
	t.Parallel()
	server := newFakePeerServer(t)

	happyPathHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body := models.Thing{
			ID:    "best-id",
			Class: "SomeRemoteClass",
			Schema: map[string]interface{}{
				"bestString": "someValue",
			},
		}
		json.NewEncoder(w).Encode(body)
	}
	server.matchers = []http.HandlerFunc{happyPathHandler}

	peers := peers.Peers{
		peers.Peer{
			URI:  strfmt.URI(server.server.URL),
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

	expectedParams := traverser.GetParams{
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
							{
								Name:        "uuid",
								IsPrimitive: true,
							},
						},
					},
				},
			},
		},
	}

	resolverResponse := []interface{}{
		map[string]interface{}{
			"NetworkRefField": []interface{}{
				NetworkRef{
					NetworkKind: crossrefs.NetworkKind{
						PeerName: "OtherInstance",
						ID:       "best-id",
						Kind:     "thing",
					},
				},
			},
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverResponse, nil).Once()

	query := "{ Get { Things { SomeThing { NetworkRefField { ... on OtherInstance__SomeRemoteClass { bestString uuid } } } } } }"
	result := resolver.AssertResolve(t, query).Result

	expectedResult := map[string]interface{}{
		"Get": map[string]interface{}{
			"Things": map[string]interface{}{
				"SomeThing": []interface{}{
					map[string]interface{}{
						"NetworkRefField": []interface{}{
							map[string]interface{}{
								"bestString": "someValue",
								"uuid":       "best-id",
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expectedResult, result, "should resolve the network cross-ref correctly")

}

func TestGetNoNetworkRequestIsMadeWhenUserDoesntWantNetworkRef(t *testing.T) {
	t.Parallel()
	server := newFakePeerServer(t)

	failTestIfCalled := func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("the remote peer server should never have been called")
	}
	server.matchers = []http.HandlerFunc{failTestIfCalled}

	peers := peers.Peers{
		peers.Peer{
			URI:  strfmt.URI(server.server.URL),
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

	expectedParams := traverser.GetParams{
		Kind:      kind.Thing,
		ClassName: "SomeThing",
		Properties: []traverser.SelectProperty{
			{
				Name:        "uuid",
				IsPrimitive: true,
			},
		},
	}

	resolverResponse := []interface{}{
		map[string]interface{}{
			"uuid": "some-uuid-for-the-local-class",
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverResponse, nil).Once()

	query := "{ Get { Things { SomeThing { uuid } } } }"
	result := resolver.AssertResolve(t, query).Result

	expectedResult := map[string]interface{}{
		"Get": map[string]interface{}{
			"Things": map[string]interface{}{
				"SomeThing": []interface{}{
					map[string]interface{}{
						"uuid": "some-uuid-for-the-local-class",
					},
				},
			},
		},
	}

	assert.Equal(t, expectedResult, result, "should resolve the network cross-ref correctly")

}
