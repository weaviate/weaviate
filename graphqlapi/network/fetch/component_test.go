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

package fetch

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/network/common"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
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

func TestNetworkFetch(t *testing.T) {

	tests := testCases{
		testCase{
			name: "network fetch happy path",
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
						beacon certainty
					}
				}
			}`,
			resolverReturn: map[string]interface{}{
				"Things": []interface{}{
					map[string]interface{}{
						"beacon":    "foobar",
						"certainty": json.Number("0.5"),
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Fetch", "Things"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "foobar",
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

			resolverReturn := []*models.GraphQLResponse{
				&models.GraphQLResponse{
					Data: map[string]models.JSONObject{
						"Local": map[string]interface{}{
							"Fetch": testCase.resolverReturn,
						},
					},
				},
			}

			resolver.On("ProxyFetch", mock.AnythingOfType("SubQuery")).
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

func (m *mockResolver) ProxyFetch(query common.SubQuery) ([]*models.GraphQLResponse, error) {
	args := m.Called(query)
	return args.Get(0).([]*models.GraphQLResponse), args.Error(1)
}
