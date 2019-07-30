//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package fetch

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testCase struct {
	name                   string
	query                  string
	resolverReturnData     interface{}
	resolverReturnPeerName string
	resolverMethod         string
	expectedResults        []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func TestNetworkFetch(t *testing.T) {

	tests := testCases{
		testCase{
			name: "network fetch things happy path",
			query: `
			{
				Fetch {
					Things(where: {
						class: {
							name: "bestclass"
							certainty: 0.8
							keywords: [{value: "foo", weight: 0.9}]
						},
						properties: {
							name: "bestproperty"
							certainty: 0.8
							keywords: [{value: "bar", weight: 0.9}]
							operator: Equal
							valueString: "some-value"
						},
					}) {
						beacon certainty className
					}
				}
			}`,
			resolverMethod:         "ProxyFetch",
			resolverReturnPeerName: "bestpeer",
			resolverReturnData: map[string]interface{}{
				"Things": []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/0d0551d8-a27b-4d52-91ac-e0006553039e",
						"className": "Superclass",
						"certainty": json.Number("0.5"),
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Fetch", "Things"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://bestpeer/things/0d0551d8-a27b-4d52-91ac-e0006553039e",
						"className": "Superclass",
						"certainty": 0.5,
					},
				},
			}},
		},

		testCase{
			name: "network fetch fuzzy happy path",
			query: `
			{
				Fetch {
					Fuzzy(value:"mysearchterm", certainty: 0.5) {
						beacon certainty className
					}
				}
			}`,
			resolverMethod:         "ProxyFetch",
			resolverReturnPeerName: "bestpeer",
			resolverReturnData: map[string]interface{}{
				"Fuzzy": []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/c74621ea-049b-410a-813f-bd93a3ba9a68",
						"className": "Superclass",
						"certainty": json.Number("0.5"),
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Fetch", "Fuzzy"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://bestpeer/things/c74621ea-049b-410a-813f-bd93a3ba9a68",
						"className": "Superclass",
						"certainty": 0.5,
					},
				},
			}},
		},
	}

	tests.Assert(t)
}

func (tests testCases) Assert(t *testing.T) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver()

			resolverReturn := []Response{
				Response{
					GraphQL: &models.GraphQLResponse{
						Data: map[string]models.JSONObject{
							"Local": map[string]interface{}{
								"Fetch": testCase.resolverReturnData,
							},
						},
					},
					PeerName: testCase.resolverReturnPeerName,
				},
			}

			resolver.On(testCase.resolverMethod, mock.AnythingOfType("SubQuery")).
				Return(resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}

type mockResolver struct {
	helper.MockResolver
}

func newMockResolver() *mockResolver {
	fetch := New()

	fetchField := &graphql.Field{
		Name: "Peers",
		Type: fetch,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			resolver, ok := p.Source.(map[string]interface{})["NetworkResolver"].(Resolver)
			if !ok {
				return nil, fmt.Errorf("source does not contain a NetworkResolver, but \n%#v", p.Source)
			}

			return resolver, nil
		},
	}

	mocker := &mockResolver{}
	mocker.RootFieldName = "Fetch"
	mocker.RootField = fetchField
	mocker.RootObject = map[string]interface{}{"NetworkResolver": Resolver(mocker)}
	return mocker
}

func (m *mockResolver) ProxyFetch(query common.SubQuery) ([]Response, error) {
	args := m.Called(query)
	return args.Get(0).([]Response), args.Error(1)
}
