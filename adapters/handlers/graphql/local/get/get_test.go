//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// These tests verify that the parameters to the resolver are properly extracted from a GraphQL query.

package get

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql/language/ast"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	helper "github.com/weaviate/weaviate/test/helper"
)

func TestSimpleFieldParamsOK(t *testing.T) {
	t.Parallel()
	resolver := newMockResolver()
	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	resolver.AssertResolve(t, "{ Get { SomeAction { intField } } }")
}

func TestExtractIntField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { SomeAction { intField } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractGeoCoordinatesField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "location", IsPrimitive: true}},
	}

	resolverReturn := []interface{}{
		map[string]interface{}{
			"location": &models.GeoCoordinates{Latitude: ptFloat32(0.5), Longitude: ptFloat32(0.6)},
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverReturn, nil).Once()

	query := "{ Get { SomeAction { location { latitude longitude } } } }"
	result := resolver.AssertResolve(t, query)

	expectedLocation := map[string]interface{}{
		"location": map[string]interface{}{
			"latitude":  float32(0.5),
			"longitude": float32(0.6),
		},
	}

	assert.Equal(t, expectedLocation, result.Get("Get", "SomeAction").Result.([]interface{})[0])
}

func TestExtractUUIDField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "uuidField", IsPrimitive: true}},
	}

	id := uuid.New()

	resolverReturn := []interface{}{
		map[string]interface{}{
			"uuidField": id,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverReturn, nil).Once()

	query := "{ Get { SomeAction { uuidField } } }"
	result := resolver.AssertResolve(t, query)

	expectedProps := map[string]interface{}{
		"uuidField": id.String(),
	}

	assert.Equal(t, expectedProps, result.Get("Get", "SomeAction").Result.([]interface{})[0])
}

func TestExtractUUIDArrayField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "uuidArrayField", IsPrimitive: true}},
	}

	id1 := uuid.New()
	id2 := uuid.New()

	resolverReturn := []interface{}{
		map[string]interface{}{
			"uuidArrayField": []uuid.UUID{id1, id2},
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(resolverReturn, nil).Once()

	query := "{ Get { SomeAction { uuidArrayField } } }"
	result := resolver.AssertResolve(t, query)

	expectedProps := map[string]interface{}{
		"uuidArrayField": []any{id1.String(), id2.String()},
	}

	assert.Equal(t, expectedProps, result.Get("Get", "SomeAction").Result.([]interface{})[0])
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
		expectedParams dto.GetParams
		resolverReturn interface{}
		expectedResult interface{}
	}

	tests := []test{
		{
			name:  "with only input requested",
			query: "{ Get { SomeAction { phone { input } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name:  "with only internationalFormatted requested",
			query: "{ Get { SomeAction { phone { internationalFormatted } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name:  "with only nationalFormatted requested",
			query: "{ Get { SomeAction { phone { nationalFormatted } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name:  "with only national requested",
			query: "{ Get { SomeAction { phone { national } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"phone": &models.PhoneNumber{National: 0o1711234567},
				},
			},
			expectedResult: map[string]interface{}{
				"phone": map[string]interface{}{
					"national": 0o1711234567,
				},
			},
		},
		{
			name:  "with only valid requested",
			query: "{ Get { SomeAction { phone { valid } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name:  "with only countryCode requested",
			query: "{ Get { SomeAction { phone { countryCode } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name:  "with only defaultCountry requested",
			query: "{ Get { SomeAction { phone { defaultCountry } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		{
			name: "with multiple fields set",
			query: "{ Get { SomeAction { phone { input internationalFormatted " +
				"nationalFormatted defaultCountry national countryCode valid } } } }",
			expectedParams: dto.GetParams{
				ClassName:  "SomeAction",
				Properties: []search.SelectProperty{{Name: "phone", IsPrimitive: true}},
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
		t.Run(test.name, func(t *testing.T) {
			resolver := newMockResolver()

			resolver.On("GetClass", test.expectedParams).
				Return(test.resolverReturn, nil).Once()
			result := resolver.AssertResolve(t, test.query)
			assert.Equal(t, test.expectedResult, result.Get("Get", "SomeAction").Result.([]interface{})[0])
		})
	}
}

func TestExtractAdditionalFields(t *testing.T) {
	// We don't need to explicitly test every subselection as we did on
	// phoneNumber as these fields have fixed keys. So we can simply check for
	// the prop

	type test struct {
		name           string
		query          string
		expectedParams dto.GetParams
		resolverReturn interface{}
		expectedResult interface{}
	}

	// To facilitate testing timestamps
	nowString := fmt.Sprint(time.Now().UnixNano() / int64(time.Millisecond))

	tests := []test{
		{
			name:  "with _additional distance",
			query: "{ Get { SomeAction { _additional { distance } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					Distance: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"distance": helper.CertaintyToDist(t, 0.69),
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"distance": helper.CertaintyToDist(t, 0.69),
				},
			},
		},
		{
			name:  "with _additional certainty",
			query: "{ Get { SomeAction { _additional { certainty } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					Certainty: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"certainty": 0.69,
						"distance":  helper.CertaintyToDist(t, 0.69),
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"certainty": 0.69,
				},
			},
		},
		{
			name:  "with _additional vector",
			query: "{ Get { SomeAction { _additional { vector } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					Vector: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"vector": []float32{0.1, -0.3},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"vector": []interface{}{float32(0.1), float32(-0.3)},
				},
			},
		},
		{
			name:  "with _additional creationTimeUnix",
			query: "{ Get { SomeAction { _additional { creationTimeUnix } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					CreationTimeUnix: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"creationTimeUnix": nowString,
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"creationTimeUnix": nowString,
				},
			},
		},
		{
			name:  "with _additional lastUpdateTimeUnix",
			query: "{ Get { SomeAction { _additional { lastUpdateTimeUnix } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					LastUpdateTimeUnix: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"lastUpdateTimeUnix": nowString,
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"lastUpdateTimeUnix": nowString,
				},
			},
		},
		{
			name:  "with _additional classification",
			query: "{ Get { SomeAction { _additional { classification { id completed classifiedFields scope basedOn }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					Classification: true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": models.AdditionalProperties{
						"classification": &additional.Classification{
							ID:               "12345",
							BasedOn:          []string{"primitiveProp"},
							Scope:            []string{"refprop1", "refprop2", "refprop3"},
							ClassifiedFields: []string{"refprop3"},
							Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"classification": map[string]interface{}{
						"id":               "12345",
						"basedOn":          []interface{}{"primitiveProp"},
						"scope":            []interface{}{"refprop1", "refprop2", "refprop3"},
						"classifiedFields": []interface{}{"refprop3"},
						"completed":        "2006-01-02T15:04:05.000Z",
					},
				},
			},
		},
		{
			name:  "with _additional interpretation",
			query: "{ Get { SomeAction { _additional { interpretation { source { concept weight occurrence } }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"interpretation": true,
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "foo",
									Weight:     0.6,
									Occurrence: 1200,
								},
								{
									Concept:    "bar",
									Weight:     0.9,
									Occurrence: 800,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
						"source": []interface{}{
							map[string]interface{}{
								"concept":    "foo",
								"weight":     0.6,
								"occurrence": 1200,
							},
							map[string]interface{}{
								"concept":    "bar",
								"weight":     0.9,
								"occurrence": 800,
							},
						},
					},
				},
			},
		},
		{
			name:  "with _additional nearestNeighbors",
			query: "{ Get { SomeAction { _additional { nearestNeighbors { neighbors { concept distance } }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"nearestNeighbors": true,
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
								{
									Concept:  "bar",
									Distance: 0.2,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"nearestNeighbors": map[string]interface{}{
						"neighbors": []interface{}{
							map[string]interface{}{
								"concept":  "foo",
								"distance": float32(0.1),
							},
							map[string]interface{}{
								"concept":  "bar",
								"distance": float32(0.2),
							},
						},
					},
				},
			},
		},
		{
			name:  "with _additional featureProjection without any optional parameters",
			query: "{ Get { SomeAction { _additional { featureProjection { vector }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": extractAdditionalParam("featureProjection", nil),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": models.AdditionalProperties{
						"featureProjection": &FeatureProjection{
							Vector: []float32{0.0, 1.1, 2.2},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"featureProjection": map[string]interface{}{
						"vector": []interface{}{float32(0.0), float32(1.1), float32(2.2)},
					},
				},
			},
		},
		{
			name:  "with _additional featureProjection with optional parameters",
			query: `{ Get { SomeAction { _additional { featureProjection(algorithm: "tsne", dimensions: 3, learningRate: 15, iterations: 100, perplexity: 10) { vector }  } } } }`,
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": extractAdditionalParam("featureProjection",
							[]*ast.Argument{
								createArg("algorithm", "tsne"),
								createArg("dimensions", "3"),
								createArg("iterations", "100"),
								createArg("learningRate", "15"),
								createArg("perplexity", "10"),
							},
						),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"featureProjection": &FeatureProjection{
							Vector: []float32{0.0, 1.1, 2.2},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"featureProjection": map[string]interface{}{
						"vector": []interface{}{float32(0.0), float32(1.1), float32(2.2)},
					},
				},
			},
		},
		{
			name:  "with _additional semanticPath set",
			query: `{ Get { SomeAction { _additional { semanticPath { path { concept distanceToQuery distanceToResult distanceToPrevious distanceToNext } } } } } }`,
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"semanticPath": extractAdditionalParam("semanticPath", nil),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": models.AdditionalProperties{
						"semanticPath": &SemanticPath{
							Path: []*SemanticPathElement{
								{
									Concept:            "foo",
									DistanceToNext:     ptFloat32(0.5),
									DistanceToPrevious: nil,
									DistanceToQuery:    0.1,
									DistanceToResult:   0.1,
								},
								{
									Concept:            "bar",
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
									DistanceToQuery:    0.1,
									DistanceToResult:   0.1,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"semanticPath": map[string]interface{}{
						"path": []interface{}{
							map[string]interface{}{
								"concept":            "foo",
								"distanceToNext":     float32(0.5),
								"distanceToPrevious": nil,
								"distanceToQuery":    float32(0.1),
								"distanceToResult":   float32(0.1),
							},
							map[string]interface{}{
								"concept":            "bar",
								"distanceToPrevious": float32(0.5),
								"distanceToNext":     nil,
								"distanceToQuery":    float32(0.1),
								"distanceToResult":   float32(0.1),
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := newMockResolverWithVectorizer("mock-custom-near-text-module")

			resolver.On("GetClass", test.expectedParams).
				Return(test.resolverReturn, nil).Once()
			result := resolver.AssertResolve(t, test.query)
			assert.Equal(t, test.expectedResult, result.Get("Get", "SomeAction").Result.([]interface{})[0])
		})
	}
}

func TestNearCustomTextRanker(t *testing.T) {
	t.Parallel()

	resolver := newMockResolverWithVectorizer("mock-custom-near-text-module")

	t.Run("for actions", func(t *testing.T) {
		query := `{ Get { SomeAction(nearCustomText: {
								concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for a class that does not have a text2vec module", func(t *testing.T) {
		query := `{ Get { CustomVectorClass(nearCustomText: {
							concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearCustomText\" on field \"CustomVectorClass\"")
	})

	t.Run("for things with optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearCustomText: {
							concepts: ["c1", "c2", "c3"],
								distance: 0.6,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.6),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearCustomText: {
							concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and objects set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearCustomText: {
								concepts: ["c1", "c2", "c3"],
								distance: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
									objects: [
										{ id: "moveTo-uuid1" }
										{ beacon: "weaviate://localhost/moveTo-uuid1" },
										{ beacon: "weaviate://localhost/moveTo-uuid2" }
									]
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
									objects: [
										{ id: "moveAway-uuid1" }
										{ beacon: "weaviate://localhost/moveAway-uuid2" }
									]
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAway-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid2",
							},
						},
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and objects set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearCustomText: {
								concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
									objects: [
										{ id: "moveTo-uuid1" }
										{ beacon: "weaviate://localhost/moveTo-uuid1" },
										{ beacon: "weaviate://localhost/moveTo-uuid2" }
									]
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
									objects: [
										{ id: "moveAway-uuid1" }
										{ beacon: "weaviate://localhost/moveAway-uuid2" }
									]
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid2",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAway-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid2",
							},
						},
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
							limit: 6
							nearCustomText: {
											concepts: ["c1", "c2", "c3"],
								distance: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: 6},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
							limit: 6
							nearCustomText: {
											concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: 6},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
							limit: -1
							nearCustomText: {
											concepts: ["c1", "c2", "c3"],
								distance: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
							limit: -1
							nearCustomText: {
											concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								}
								moveAwayFrom: {
									concepts:["epic"]
									force: 0.25
								}
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestNearVectorRanker(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	t.Run("for actions", func(t *testing.T) {
		query := `{ Get { SomeAction(nearVector: {
								vector: [0.123, 0.984]
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.123, 0.984},
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearVector: {
								vector: [0.123, 0.984]
								distance: 0.4
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:       []float32{0.123, 0.984},
				Distance:     0.4,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearVector: {
								vector: [0.123, 0.984]
								certainty: 0.4
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.4,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
					limit: 4  
						nearVector: {
						vector: [0.123, 0.984]
						distance: 0.1
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: 4},
			NearVector: &searchparams.NearVector{
				Vector:       []float32{0.123, 0.984},
				Distance:     0.1,
				WithDistance: true,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
					limit: 4  
						nearVector: {
						vector: [0.123, 0.984]
						certainty: 0.1
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: 4},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.1,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
					limit: -1  
						nearVector: {
						vector: [0.123, 0.984]
						distance: 0.1
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:       []float32{0.123, 0.984},
				Distance:     0.1,
				WithDistance: true,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
					limit: -1  
						nearVector: {
						vector: [0.123, 0.984]
						certainty: 0.1
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.1,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestExtractPagination(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &filters.Pagination{
			Limit: 10,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { SomeAction(limit: 10) { intField } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractPaginationWithOffset(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &filters.Pagination{
			Offset: 5,
			Limit:  10,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { SomeAction(offset: 5 limit: 10) { intField } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractPaginationWithOnlyOffset(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &filters.Pagination{
			Offset: 5,
			Limit:  -1,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { SomeAction(offset: 5) { intField } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractCursor(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Cursor: &filters.Cursor{
			After: "8ef8d5cc-c101-4fbd-a016-84e766b93ecf",
			Limit: 2,
		},
		Pagination: &filters.Pagination{
			Offset: 0,
			Limit:  2,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ Get { SomeAction(after: "8ef8d5cc-c101-4fbd-a016-84e766b93ecf" limit: 2) { intField } } }`
	resolver.AssertResolve(t, query)
}

func TestExtractGroupParams(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		Group: &dto.GroupParams{
			Strategy: "closest",
			Force:    0.3,
		},
	}

	resolver.On("GetClass", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := "{ Get { SomeAction(group: {type: closest, force: 0.3}) { intField } } }"
	resolver.AssertResolve(t, query)
}

func TestGetRelation(t *testing.T) {
	t.Parallel()

	t.Run("without using custom fragments", func(t *testing.T) {
		resolver := newMockResolver()

		expectedParams := dto.GetParams{
			ClassName: "SomeAction",
			Properties: []search.SelectProperty{
				{
					Name:        "hasAction",
					IsPrimitive: false,
					Refs: []search.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []search.SelectProperty{
								{
									Name:        "intField",
									IsPrimitive: true,
								},
								{
									Name:        "hasAction",
									IsPrimitive: false,
									Refs: []search.SelectClass{
										{
											ClassName: "SomeAction",
											RefProperties: []search.SelectProperty{
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

		query := "{ Get { SomeAction { hasAction { ... on SomeAction { intField, hasAction { ... on SomeAction { intField } } } } } } }"
		resolver.AssertResolve(t, query)
	})

	t.Run("with a custom fragment one level deep", func(t *testing.T) {
		resolver := newMockResolver()

		expectedParams := dto.GetParams{
			ClassName: "SomeAction",
			Properties: []search.SelectProperty{
				{
					Name:        "hasAction",
					IsPrimitive: false,
					Refs: []search.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []search.SelectProperty{
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

		query := "fragment actionFragment on SomeAction { intField } { Get { SomeAction { hasAction { ...actionFragment } } } }"
		resolver.AssertResolve(t, query)
	})

	t.Run("with a custom fragment multiple levels deep", func(t *testing.T) {
		resolver := newMockResolver()

		expectedParams := dto.GetParams{
			ClassName: "SomeAction",
			Properties: []search.SelectProperty{
				{
					Name:        "hasAction",
					IsPrimitive: false,
					Refs: []search.SelectClass{
						{
							ClassName: "SomeAction",
							RefProperties: []search.SelectProperty{
								{
									Name:        "intField",
									IsPrimitive: true,
								},
								{
									Name:        "hasAction",
									IsPrimitive: false,
									Refs: []search.SelectClass{
										{
											ClassName: "SomeAction",
											RefProperties: []search.SelectProperty{
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
			fragment actionFragment on SomeAction { intField hasAction { ...innerFragment } } 
			
			{ Get { SomeAction { hasAction { ...actionFragment } } } }`
		resolver.AssertResolve(t, query)
	})
}

func TestNearObject(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	t.Run("for objects with beacon", func(t *testing.T) {
		query := `{ Get { SomeAction(
								nearObject: {
									beacon: "weaviate://localhost/some-uuid"
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/some-uuid",
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with beacon and optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									beacon: "weaviate://localhost/some-other-uuid"
									distance: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearObject: &searchparams.NearObject{
				Beacon:       "weaviate://localhost/some-other-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with beacon and optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									beacon: "weaviate://localhost/some-other-uuid"
									certainty: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearObject: &searchparams.NearObject{
				Beacon:    "weaviate://localhost/some-other-uuid",
				Certainty: 0.7,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with id set", func(t *testing.T) {
		query := `{ Get { SomeAction(
								nearObject: {
									id: "some-uuid"
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID: "some-uuid",
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with id and optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									id: "some-other-uuid"
									distance: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-other-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with id and optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									id: "some-other-uuid"
									certainty: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-other-uuid",
				Certainty: 0.7,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with optional distance and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: 5
						nearObject: {
							id: "some-other-uuid"
							distance: 0.7
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: 5},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-other-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with optional certainty and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: 5
						nearObject: {
							id: "some-other-uuid"
							certainty: 0.7
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: 5},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-other-uuid",
				Certainty: 0.7,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with optional distance and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearObject: {
							id: "some-other-uuid"
							distance: 0.7
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-other-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with optional certainty and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearObject: {
							id: "some-other-uuid"
							certainty: 0.7
						}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-other-uuid",
				Certainty: 0.7,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestNearTextNoNoModules(t *testing.T) {
	t.Parallel()

	resolver := newMockResolverWithNoModules()

	t.Run("for nearText that is not available", func(t *testing.T) {
		query := `{ Get { SomeAction(nearText: {
								concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		}

		resolver.On("GetClass", expectedParams).
			Return(nil, nil).Once()

		resolver.AssertFailToResolve(t, query)
	})
}

func TestBM25WithSort(t *testing.T) {
	t.Parallel()
	resolver := newMockResolverWithNoModules()
	query := `{Get{SomeAction(bm25:{query:"apple",properties:["name"]},sort:[{path:["name"],order:desc}]){intField}}}`
	resolver.AssertFailToResolve(t, query, "bm25 search is not compatible with sort")
}

func TestHybridWithSort(t *testing.T) {
	t.Parallel()
	resolver := newMockResolverWithNoModules()
	query := `{Get{SomeAction(hybrid:{query:"apple"},sort:[{path:["name"],order:desc}]){intField}}}`
	resolver.AssertFailToResolve(t, query, "hybrid search is not compatible with sort")
}

func TestNearObjectNoModules(t *testing.T) {
	t.Parallel()

	resolver := newMockResolverWithNoModules()

	t.Run("for objects with beacon", func(t *testing.T) {
		query := `{ Get { SomeAction(
								nearObject: {
									beacon: "weaviate://localhost/some-uuid"
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/some-uuid",
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with ID and distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									id: "some-uuid"
									distance: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with ID and certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								nearObject: {
									id: "some-uuid"
									certainty: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-uuid",
				Certainty: 0.7,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with distance and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								limit: 12
								nearObject: {
									id: "some-uuid"
									distance: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: 12},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with certainty and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								limit: 12
								nearObject: {
									id: "some-uuid"
									certainty: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: 12},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-uuid",
				Certainty: 0.7,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with distance and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								limit: -1
								nearObject: {
									id: "some-uuid"
									distance: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:           "some-uuid",
				Distance:     0.7,
				WithDistance: true,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for objects with certainty and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
								limit: -1
								nearObject: {
									id: "some-uuid"
									certainty: 0.7
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearObject: &searchparams.NearObject{
				ID:        "some-uuid",
				Certainty: 0.7,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestNearVectorNoModules(t *testing.T) {
	t.Parallel()

	resolver := newMockResolverWithNoModules()

	t.Run("for actions", func(t *testing.T) {
		query := `{ Get { SomeAction(nearVector: {
								vector: [0.123, 0.984]
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.123, 0.984},
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearVector: {
								vector: [0.123, 0.984]
								distance: 0.4
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:       []float32{0.123, 0.984},
				Distance:     0.4,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearVector: {
								vector: [0.123, 0.984]
								certainty: 0.4
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.4,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: 4
						nearVector: {
							vector: [0.123, 0.984]
							certainty: 0.4
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: 4},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.4,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vector: [0.123, 0.984]
							distance: 0.4
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:       []float32{0.123, 0.984},
				Distance:     0.4,
				WithDistance: true,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and negative limit set", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vector: [0.123, 0.984]
							certainty: 0.4
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Vector:    []float32{0.123, 0.984},
				Certainty: 0.4,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestSort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resolver *mockResolver
	}{
		{
			name:     "with modules",
			resolver: newMockResolver(),
		},
		{
			name:     "with no modules",
			resolver: newMockResolverWithNoModules(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("simple sort", func(t *testing.T) {
				query := `{ Get { SomeAction(sort:[{
										path: ["path"] order: asc
									}]) { intField } } }`

				expectedParams := dto.GetParams{
					ClassName:  "SomeAction",
					Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
					Sort:       []filters.Sort{{Path: []string{"path"}, Order: "asc"}},
				}

				tt.resolver.On("GetClass", expectedParams).
					Return([]interface{}{}, nil).Once()

				tt.resolver.AssertResolve(t, query)
			})

			t.Run("simple sort with two paths", func(t *testing.T) {
				query := `{ Get { SomeAction(sort:[{
										path: ["path1", "path2"] order: desc
									}]) { intField } } }`

				expectedParams := dto.GetParams{
					ClassName:  "SomeAction",
					Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
					Sort:       []filters.Sort{{Path: []string{"path1", "path2"}, Order: "desc"}},
				}

				tt.resolver.On("GetClass", expectedParams).
					Return([]interface{}{}, nil).Once()

				tt.resolver.AssertResolve(t, query)
			})

			t.Run("simple sort with two sort filters", func(t *testing.T) {
				query := `{ Get { SomeAction(sort:[{
										path: ["first1", "first2", "first3", "first4"] order: asc
									} {
										path: ["second1"] order: desc
									}]) { intField } } }`

				expectedParams := dto.GetParams{
					ClassName:  "SomeAction",
					Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
					Sort: []filters.Sort{
						{Path: []string{"first1", "first2", "first3", "first4"}, Order: "asc"},
						{Path: []string{"second1"}, Order: "desc"},
					},
				}

				tt.resolver.On("GetClass", expectedParams).
					Return([]interface{}{}, nil).Once()

				tt.resolver.AssertResolve(t, query)
			})
		})
	}
}

func TestGroupBy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resolver *mockResolver
	}{
		{
			name:     "with modules",
			resolver: newMockResolver(),
		},
		{
			name:     "with no modules",
			resolver: newMockResolverWithNoModules(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("simple groupBy", func(t *testing.T) {
				query := `{ Get {
					SomeAction(
						groupBy:{path: ["path"] groups: 2 objectsPerGroup:3}
					) {
						_additional{group{count groupedBy {value path} maxDistance minDistance hits {_additional{distance}}}
						}
					} } }`

				expectedParams := dto.GetParams{
					ClassName:            "SomeAction",
					GroupBy:              &searchparams.GroupBy{Property: "path", Groups: 2, ObjectsPerGroup: 3},
					AdditionalProperties: additional.Properties{Group: true},
				}

				tt.resolver.On("GetClass", expectedParams).
					Return([]interface{}{}, nil).Once()

				tt.resolver.AssertResolve(t, query)
			})
		})
	}
}

func ptFloat32(in float32) *float32 {
	return &in
}

func timeMust(t strfmt.DateTime, err error) strfmt.DateTime {
	if err != nil {
		panic(err)
	}

	return t
}
