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

package get

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/crossrefs"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestLocalGetWithNetworkRefResolvesCorrectly(t *testing.T) {
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

	expectedParams := &kinds.LocalGetParams{
		Kind:      kind.Thing,
		ClassName: "SomeThing",
		Properties: []kinds.SelectProperty{
			{
				Name:        "NetworkRefField",
				IsPrimitive: false,
				Refs: []kinds.SelectClass{
					{
						ClassName: "OtherInstance__SomeRemoteClass",
						RefProperties: []kinds.SelectProperty{
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

	resolver.On("LocalGetClass", expectedParams).
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

func TestLocalGetNoNetworkRequestIsMadeWhenUserDoesntWantNetworkRef(t *testing.T) {
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

	expectedParams := &kinds.LocalGetParams{
		Kind:      kind.Thing,
		ClassName: "SomeThing",
		Properties: []kinds.SelectProperty{
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

	resolver.On("LocalGetClass", expectedParams).
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
