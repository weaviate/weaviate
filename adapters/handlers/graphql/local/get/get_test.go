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

// These tests verify that the parameters to the resolver are properly extracted from a GraphQL query.

package get

import (
	"testing"

	test_helper "github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
)

func TestSimpleFieldParamsOK(t *testing.T) {
	t.Parallel()
	resolver := newMockResolver(emptyPeers())
	expectedParams := traverser.GetParams{
		Kind:       kind.Action,
		ClassName:  "SomeAction",
		Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	resolver.AssertResolve(t, "{ Get { Actions { SomeAction { intField } } } }")
}

func TestExtractIntField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := traverser.GetParams{
		Kind:       kind.Action,
		ClassName:  "SomeAction",
		Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractGeoCoordinatesField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := traverser.GetParams{
		Kind:       kind.Action,
		ClassName:  "SomeAction",
		Properties: []traverser.SelectProperty{{Name: "location", IsPrimitive: true}},
	}

	resolverReturn := []interface{}{
		map[string]interface{}{
			"location": &models.GeoCoordinates{Latitude: ptFloat32(0.5), Longitude: ptFloat32(0.6)},
		},
	}

	resolver.On("GetClass", expectedParams).
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

func TestExtractPhoneNumberField(t *testing.T) {
	// We need to explicitly test all cases of asking for just one sub-property
	// at a time, because the AST-parsing uses known fields of known props to
	// distinguish a complex primitive prop from a reference prop
	//
	// See "isPrimitive()" and "fieldNameIsOfObjectButNonReferenceType" in
	// class_builder_fields.go for more details

	type test struct {
		name           string
		query          string
		expectedParams traverser.GetParams
		resolverReturn interface{}
		expectedResult interface{}
	}

	tests := []test{
		test{
			name:  "with only input requested",
			query: "{ Get { Actions { SomeAction { phone { input } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{Input: "+49 171 1234567"},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"input": "+49 171 1234567",
				},
			},
		},
		test{
			name:  "with only internationalFormatted requested",
			query: "{ Get { Actions { SomeAction { phone { internationalFormatted } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{InternationalFormatted: "+49 171 1234567"},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"internationalFormatted": "+49 171 1234567",
				},
			},
		},
		test{
			name:  "with only nationalFormatted requested",
			query: "{ Get { Actions { SomeAction { phone { nationalFormatted } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{NationalFormatted: "0171 1234567"},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"nationalFormatted": "0171 1234567",
				},
			},
		},
		test{
			name:  "with only national requested",
			query: "{ Get { Actions { SomeAction { phone { national } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{National: 01711234567},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"national": 01711234567,
				},
			},
		},
		test{
			name:  "with only valid requested",
			query: "{ Get { Actions { SomeAction { phone { valid } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{Valid: true},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"valid": true,
				},
			},
		},
		test{
			name:  "with only countryCode requested",
			query: "{ Get { Actions { SomeAction { phone { countryCode } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{CountryCode: 49},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"countryCode": 49,
				},
			},
		},
		test{
			name:  "with only defaultCountry requested",
			query: "{ Get { Actions { SomeAction { phone { defaultCountry } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{DefaultCountry: "DE"},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"defaultCountry": "DE",
				},
			},
		},
		test{
			name: "with multiple fields set",
			query: "{ Get { Actions { SomeAction { phone { input internationalFormatted " +
				"nationalFormatted defaultCountry national countryCode valid } } } } }",
			expectedParams: traverser.GetParams{
				Kind:       kind.Action,
				ClassName:  "SomeAction",
				Properties: []traverser.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{
						DefaultCountry:         "DE",
						CountryCode:            49,
						NationalFormatted:      "0171 123456",
						InternationalFormatted: "+49 171 123456",
						National:               171123456,
						Input:                  "0171123456",
						Valid:                  true,
					},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"defaultCountry":         "DE",
					"countryCode":            49,
					"nationalFormatted":      "0171 123456",
					"internationalFormatted": "+49 171 123456",
					"national":               171123456,
					"input":                  "0171123456",
					"valid":                  true,
				},
			},
		},
	}

	for _, test := range tests {
		resolver := newMockResolver(emptyPeers())

		resolver.On("GetClass", test.expectedParams).
			Return(test.resolverReturn, nil).Once()
		result := resolver.AssertResolve(t, test.query)
		assert.Equal(t, test.expectedResult, result.Get("Get", "Actions", "SomeAction").Result.([]interface{})[0])
	}
}

func TestExploreRanker(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	t.Run("for actions", func(t *testing.T) {
		query := `{ Get { Actions { SomeAction(explore: {
                concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
        			}) { intField } } } }`

		expectedParams := traverser.GetParams{
			Kind:       kind.Action,
			ClassName:  "SomeAction",
			Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Explore: &traverser.ExploreParams{
				Values: []string{"c1", "c2", "c3"},
				MoveTo: traverser.ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
				},
				MoveAwayFrom: traverser.ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
				},
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty set", func(t *testing.T) {
		query := `{ Get { Things { SomeThing(explore: {
                concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
        			}) { intField } } } }`

		expectedParams := traverser.GetParams{
			Kind:       kind.Thing,
			ClassName:  "SomeThing",
			Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Explore: &traverser.ExploreParams{
				Values:    []string{"c1", "c2", "c3"},
				Certainty: 0.4,
				MoveTo: traverser.ExploreMove{
					Values: []string{"positive"},
					Force:  0.5,
				},
				MoveAwayFrom: traverser.ExploreMove{
					Values: []string{"epic"},
					Force:  0.25,
				},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

}

func TestExtractPagination(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := traverser.GetParams{
		Kind:       kind.Action,
		ClassName:  "SomeAction",
		Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &filters.Pagination{
			Limit: 10,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction(limit: 10) { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractGroupParams(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(emptyPeers())

	expectedParams := traverser.GetParams{
		Kind:       kind.Action,
		ClassName:  "SomeAction",
		Properties: []traverser.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Group: &traverser.GroupParams{
			Strategy: "closest",
			Force:    0.3,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { Actions { SomeAction(group: {type: closest, force: 0.3}) { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestGetRelation(t *testing.T) {
	t.Parallel()

	t.Run("without using custom fragments", func(t *testing.T) {
		resolver := newMockResolver(emptyPeers())

		expectedParams := traverser.GetParams{
			Kind:      kind.Action,
			ClassName: "SomeAction",
			Properties: []traverser.SelectProperty{
				{
					Name:        "HasAction",
					IsPrimitive: false,
					Refs: []traverser.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []traverser.SelectProperty{
								{
									Name:        "intField",
									IsPrimitive: true,
								},
								{
									Name:        "HasAction",
									IsPrimitive: false,
									Refs: []traverser.SelectClass{
										{
											ClassName: "SomeAction",
											RefProperties: []traverser.SelectProperty{
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

		resolver.On("GetClass", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := "{ Get { Actions { SomeAction { HasAction { ... on SomeAction { intField, HasAction { ... on SomeAction { intField } } } } } } } }"
		resolver.AssertResolve(t, query)
	})

	t.Run("with a custom fragment one level deep", func(t *testing.T) {
		resolver := newMockResolver(emptyPeers())

		expectedParams := traverser.GetParams{
			Kind:      kind.Action,
			ClassName: "SomeAction",
			Properties: []traverser.SelectProperty{
				{
					Name:        "HasAction",
					IsPrimitive: false,
					Refs: []traverser.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []traverser.SelectProperty{
								{
									Name:        "intField",
									IsPrimitive: true,
								},
							},
						},
					},
				},
			},
		}

		resolver.On("GetClass", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := "fragment actionFragment on SomeAction { intField } { Get { Actions { SomeAction { HasAction { ...actionFragment } } } } }"
		resolver.AssertResolve(t, query)
	})

	t.Run("with a custom fragment multiple levels deep", func(t *testing.T) {
		resolver := newMockResolver(emptyPeers())

		expectedParams := traverser.GetParams{
			Kind:      kind.Action,
			ClassName: "SomeAction",
			Properties: []traverser.SelectProperty{
				{
					Name:        "HasAction",
					IsPrimitive: false,
					Refs: []traverser.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []traverser.SelectProperty{
								{
									Name:        "intField",
									IsPrimitive: true,
								},
								{
									Name:        "HasAction",
									IsPrimitive: false,
									Refs: []traverser.SelectClass{
										{
											ClassName: "SomeAction",
											RefProperties: []traverser.SelectProperty{
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

		resolver.On("GetClass", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := `
			fragment innerFragment on SomeAction { intField }
			fragment actionFragment on SomeAction { intField HasAction { ...innerFragment } } 
			
			{ Get { Actions { SomeAction { HasAction { ...actionFragment } } } } }`
		resolver.AssertResolve(t, query)
	})
}

func ptFloat32(in float32) *float32 {
	return &in
}
