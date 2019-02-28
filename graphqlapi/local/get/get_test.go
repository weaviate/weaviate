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
// These tests verify that the parameters to the resolver are properly extracted from a GraphQL query.

package get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/stretchr/testify/assert"
)

func TestSimpleFieldParamsOK(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := &Params{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	resolver.AssertResolve(t, "{ Get { Actions { SomeAction { intField } } } }")
}

func TestExtractIntField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := &Params{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractGeoCoordinatesField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := &Params{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "location", IsPrimitive: true}},
	}

	resolverReturn := []interface{}{
		map[string]interface{}{
			"location": &models.GeoCoordinates{Latitude: 0.5, Longitude: 0.6},
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(resolverReturn, nil).Once()

	query := "{ Get { Actions { SomeAction { location { latitude longitude } } } } }"
	result := resolver.AssertResolve(t, query)

	expectedLocation := map[string]interface{}{
		"location": map[string]interface{}{
			"latitude":  float32(0.5),
			"longitude": float32(0.6),
		},
	}

	assert.Equal(t, expectedLocation, result.Get("Get", "Actions", "SomeAction").Result.([]interface{})[0])
}

func TestExtractPagination(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := &Params{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &common.Pagination{
			First: 10,
			After: 20,
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction(first:10, after: 20) { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestGetRelation(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := &Params{
		Kind:      kind.ACTION_KIND,
		ClassName: "SomeAction",
		Properties: []SelectProperty{
			{
				Name:        "HasAction",
				IsPrimitive: false,
				Refs: []SelectClass{
					{
						ClassName: "SomeAction",
						RefProperties: []SelectProperty{
							{
								Name:        "intField",
								IsPrimitive: true,
							},
							{
								Name:        "HasAction",
								IsPrimitive: false,
								Refs: []SelectClass{
									{
										ClassName: "SomeAction",
										RefProperties: []SelectProperty{
											{
												Name:        "intField",
												IsPrimitive: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction { HasAction { ... on SomeAction { intField, HasAction { ... on SomeAction { intField } } } } } } } }"
	resolver.AssertResolve(t, query)
}
