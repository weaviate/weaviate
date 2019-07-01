/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package aggregate

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
	name            string
	query           string
	resolverReturn  interface{}
	expectedResults []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func TestNetworkAggregate(t *testing.T) {

	tests := testCases{
		testCase{
			name: "network aggregate happy path",
			query: `{ Aggregate { PeerA { 
				Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean } } } }
			} }`,
			resolverReturn: map[string]interface{}{
				"Things": map[string]interface{}{
					"Car": []interface{}{
						map[string]interface{}{
							"horsepower": map[string]interface{}{
								"mean": json.Number("275.7773"),
							},
						},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "PeerA", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{"mean": 275.7773},
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

			resolverReturn := &models.GraphQLResponse{
				Data: map[string]models.JSONObject{
					"Local": map[string]interface{}{
						"Aggregate": testCase.resolverReturn,
					},
				},
			}

			resolver.On("ProxyAggregateInstance", mock.AnythingOfType("Params")).
				Return(resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {

}

type mockResolver struct {
	helper.MockResolver
}

func newMockResolver() *mockResolver {
	peerA, err := New("PeerA", helper.CarSchema).PeerField()
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}

	peerField := &graphql.Field{
		Name: "Peers",
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:   "PeerAObj",
			Fields: graphql.Fields{"PeerA": peerA},
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return p.Source, nil
		},
	}

	mocker := &mockResolver{}
	mocker.RootFieldName = "Aggregate"
	mocker.RootField = peerField
	mocker.RootObject = map[string]interface{}{"NetworkResolver": Resolver(mocker), "RequestsLog": &mockRequestsLog{}}
	return mocker
}

func (m *mockResolver) ProxyAggregateInstance(params common.Params) (*models.GraphQLResponse, error) {
	args := m.Called(params)
	return args.Get(0).(*models.GraphQLResponse), args.Error(1)
}
