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
						"id":               strfmt.UUID("12345"),
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

	t.Run("for actions with targetvec", func(t *testing.T) {
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
								targetVectors: ["epic"]
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
					"targetVectors": []interface{}{"epic"},
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
				Vectors: []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:      []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:      []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:      []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
				Certainty: 0.1,
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [1, 0], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title2", "title3"},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector list", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[1, 0]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title2", "title3"},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
	t.Run("with target multivector", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[[1, 0]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[][]float32{{1., 0}}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title2", "title3"},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector and multiple entries for a vector", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[1, 0], [0,1]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 1}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with target multivector and multiple entries for multivector", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[[1, 0]], [[0,1]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[][]float32{{1., 0}}, [][]float32{{0, 1}}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector and multiple entries for a vector and targets", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[1, 0], [0,1]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {targetVectors: ["title1", "title1", "title2", "title3"], combinationMethod: sum}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 1}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Sum, Weights: []float32{1, 1, 1, 1}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with target multivector and multiple entries for multivector and targets", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[[1, 0]], [[0,1]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {targetVectors: ["title1", "title1", "title2", "title3"], combinationMethod: sum}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[][]float32{{1., 0}}, [][]float32{{0, 1}}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Sum, Weights: []float32{1, 1, 1, 1}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector and weights", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [1, 0], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
							combinationMethod: manualWeights,
							weights: {title1: 1, title2: 3, title3: 4}
						}
					}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{1, 3, 4}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with target multivector and weights", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [1, 0], title2: [0, 0, 1], title3: [[[0, 0, 0, 1]]]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
							combinationMethod: manualWeights,
							weights: {title1: 1, title2: 3, title3: 4}
						}
					}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 0, 1}, [][]float32{{0, 0, 0, 1}}},
				TargetVectors: []string{"title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{1, 3, 4}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with targetvector and multiple entries for a vector and weights", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[1, 0], [0,1]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {
							targetVectors: ["title1", "title1", "title2", "title3"], 
							combinationMethod: manualWeights,
							weights: {title1: [1, 2], title2: 3, title3: 4}
						}
					}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{1., 0}, []float32{0, 1}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{1, 2, 3, 4}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with target multivector and multiple entries for multivector and weights", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[[1, 0]], [[0,1]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {
							targetVectors: ["title1", "title1", "title2", "title3"], 
							combinationMethod: manualWeights,
							weights: {title1: [1, 2], title2: 3, title3: 4}
						}
					}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[][]float32{{1., 0}}, [][]float32{{0, 1}}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				TargetVectors: []string{"title1", "title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.ManualWeights, Weights: []float32{1, 2, 3, 4}},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with non fitting target vectors", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[1, 0], [0,1]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
						}
					}) { intField } } }`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("with non fitting target multivectors", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1: [[[1, 0]], [[0,1]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
						}
					}) { intField } } }`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("with non fitting target vectors 2", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1:  [0,1], title2: [0, 0, 1], title3:[[1, 0], [0,1]]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
						}
					}) { intField } } }`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("with non fitting target multivectors 2", func(t *testing.T) {
		query := `{ Get { SomeThing(
						nearVector: {
						vectorPerTarget: {title1:  [0,1], title2: [0, 0, 1], title3:[[[1, 0]], [[0,1]]]}
						targets: {
							targetVectors: ["title1", "title2", "title3"], 
						}
					}) { intField } } }`
		resolver.AssertFailToResolve(t, query)
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

func TestHybridWithTargets(t *testing.T) {
	t.Parallel()
	resolver := newMockResolverWithNoModules()
	var emptySubsearches []searchparams.WeightedSearchResult

	t.Run("hybrid search", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple"}
					){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with targets", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],}
					){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with targets and vector", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
                    vector: [0.123, 0.984],
					targetVectors: ["title1", "title2", "title3"],}
					){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
				Vector:          []float32{0.123, 0.984},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with targets and vector", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
                    vector: [0.123, 0.984],
					targetVectors: ["title1", "title2", "title3"],}
					){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
				Vector:          []float32{0.123, 0.984},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],
					searches: {nearVector:{
     							vector: [0.123, 0.984],
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{0.123, 0.984}, []float32{0.123, 0.984}, []float32{0.123, 0.984}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multiple vectors", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, 0], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{1.0, 0}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multiple vectors with multivector", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [[[1, 0]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[][]float32{{1.0, 0}}, []float32{0, 0, 1}, []float32{0, 0, 0, 1}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and wrong input", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, "fish"], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
                    }}
					}){intField}}}`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and wrong input in multivector", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [[[1, "fish"]]], title2: [0, 0, 1], title3: [0, 0, 0, 1]}
                    }}
					}){intField}}}`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multi vector", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title2", "title3", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, 0], title2: [[0, 0, 1], [1,0,0]], title3: [[0, 0, 0, 1], [1, 0, 0, 1]]}
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title2", "title3", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{1.0, 0}, []float32{0, 0, 1}, []float32{1, 0, 0}, []float32{0, 0, 0, 1}, []float32{1, 0, 0, 1}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multiple vectors with multivector2", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title2", "title3", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, 0], title2: [[[0, 0, 1]], [[1,0,0]]], title3: [[0, 0, 0, 1], [1, 0, 0, 1]]}
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title2", "title3", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{1.0, 0}, [][]float32{{0, 0, 1}}, [][]float32{{1, 0, 0}}, []float32{0, 0, 0, 1}, []float32{1, 0, 0, 1}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multiple vectors missing target vectors", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, 0], title2: [[0, 0, 1], [1,0,0]], title3: [[0, 0, 0, 1], [1, 0, 0, 1]]}
                    }}
					}){intField}}}`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multiple vectors and multivector missing target vectors", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					searches: {nearVector:{
     							vectorPerTarget: {title1: [[[1, 0]]], title2: [[[0, 0, 1]], [[1,0,0]]], title3: [[[0, 0, 0, 1]], [[1, 0, 0, 1]]]}
                    }}
					}){intField}}}`
		resolver.AssertFailToResolve(t, query)
	})

	t.Run("hybrid search with near vector subsearch and multi vector2", func(t *testing.T) {
		query := `{Get{SomeAction(hybrid:{
					query:"apple", 
					targetVectors: ["title1", "title2", "title2", "title3", "title3"],
					searches: {nearVector:{
     							vectorPerTarget: {title1: [1, 0], title2: [[0, 0, 1], [1,0,0]], title3: [[0, 0, 0, 1], [1, 0, 0, 1]]}
                    }}
					}){intField}}}`
		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			HybridSearch: &searchparams.HybridSearch{
				Query:           "apple",
				Alpha:           0.75,
				Type:            "hybrid",
				FusionAlgorithm: 1,
				SubSearches:     emptySubsearches,
				TargetVectors:   []string{"title1", "title2", "title2", "title3", "title3"},
				NearVectorParams: &searchparams.NearVector{
					Vectors: []models.Vector{[]float32{1.0, 0}, []float32{0, 0, 1}, []float32{1, 0, 0}, []float32{0, 0, 0, 1}, []float32{1, 0, 0, 1}},
				},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()
		resolver.AssertResolve(t, query)
	})
}

func TestHybridWithVectorDistance(t *testing.T) {
	t.Parallel()
	resolver := newMockResolverWithNoModules()
	query := `{Get{SomeAction(hybrid:{query:"apple", maxVectorDistance: 0.5}){intField}}}`

	var emptySubsearches []searchparams.WeightedSearchResult
	expectedParams := dto.GetParams{
		ClassName:  "SomeAction",
		Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
		HybridSearch: &searchparams.HybridSearch{
			Query:           "apple",
			Distance:        0.5,
			WithDistance:    true,
			FusionAlgorithm: 1,
			Alpha:           0.75,
			Type:            "hybrid",
			SubSearches:     emptySubsearches,
		},
	}
	resolver.On("GetClass", expectedParams).
		Return([]interface{}{}, nil).Once()

	resolver.AssertResolve(t, query)
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
				Vectors: []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:      []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:      []models.Vector{[]float32{0.123, 0.984}},
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
				Vectors:   []models.Vector{[]float32{0.123, 0.984}},
				Certainty: 0.4,
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("vector and targets", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vector: [0.123, 0.984]
							targetVectors: ["test1", "test2"]
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: -1},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{0.123, 0.984}, []float32{0.123, 0.984}},
				TargetVectors: []string{"test1", "test2"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("vectorPerTarget and targets", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vectorPerTarget:{ test1: [0.123, 0.984], test2: [0.456, 0.789]}
							targetVectors: ["test1", "test2"]
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: -1},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[]float32{0.123, 0.984}, []float32{0.456, 0.789}},
				TargetVectors: []string{"test1", "test2"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("vectorPerTarget and targets multivector", func(t *testing.T) {
		query := `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vectorPerTarget:{ test1: [[[0.123, 0.984]]], test2: [0.456, 0.789]}
							targetVectors: ["test1", "test2"]
								}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: -1},
			NearVector: &searchparams.NearVector{
				Vectors:       []models.Vector{[][]float32{{0.123, 0.984}}, []float32{0.456, 0.789}},
				TargetVectors: []string{"test1", "test2"},
			},
			TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func TestNearVectorNoModulesMultiVector(t *testing.T) {
	t.Parallel()

	resolver := newMockResolverWithNoModules()

	tt := []struct {
		name           string
		query          string
		expectedParams *dto.GetParams
	}{
		// single 3x2 multi-vector => valid if no `targetVectors` is given (only 1 multi-vector)
		{
			name: "vectorPerTarget and targets multivector",
			query: `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vectorPerTarget:{ mymultivec2d: [
								[[0.1,0.1],[0.2,0.2],[0.3,0.3]]
							]}
						}) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
					},
					TargetVectors: []string{"mymultivec2d"},
				},
			},
		},
		// 4 levels of nesting is not handled, only support 3 levels right now (eg list of 2d multi-vectors)
		{
			name: "4 levels too much vector nesting",
			query: `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vectorPerTarget:{ mymultivec2d: [
								[[[0.1,0.1],[0.2,0.2],[0.3,0.3]]]
							]}
						}) { intField } } }`,
			// 4 levels => parse error => expect nil
			expectedParams: nil,
		},
		// two multi-vectors with two targetVectors => valid
		{
			name: "vectorPerTarget + targetVectors for multi and normal",
			query: `{ Get { SomeThing(
						limit: -1
						nearVector: {
							vectorPerTarget:{ test1: [[[0.123, 0.984]]], test2: [0.456, 0.789]}
							targetVectors: ["test1", "test2"]
								}) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						[][]float32{{0.123, 0.984}}, // multi-vector for "test1" (3 levels)
						[]float32{0.456, 0.789},     // normal vector for "test2" (2 levels)
					},
					TargetVectors: []string{"test1", "test2"},
				},
				// The original example shows a minimum combination:
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
		// single 3x2 multi-vector => valid if no `targetVectors` is given (only 1 multi-vector)
		{
			name: "single 3x2 multi-vector with no targetVectors",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]]
						]
					}
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
					},
					TargetVectors: []string{"mymultivec2d"},
				},
			},
		},
		// two 3x2 multi-vectors => shape is valid, but missing `targetVectors`,
		// so final parse must fail (multiple multi-vectors with no labels).
		{
			name: "two 3x2 multi-vectors with no targetVectors error)",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4],[0.5,0.5],[0.6,0.6]]
						]
					}
				}
			) { intField } } }`,
			expectedParams: nil, // fails because multiple multi-vectors require targetVectors
		},
		// one 3x2 multi-vector and one 1x2 multi-vector => shape is valid,
		// but no targetVectors => must fail for same reason (multiple MVs).
		{
			name: "2 multi-vectors with no targetVectors error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4]]
						]
					}
				}
			) { intField } } }`,
			expectedParams: nil,
		},
		// one 3x2 multi-vector, one 1x2 multi-vector, one 4x2 multi-vector => shape valid,
		// but still multiple MVs with no targetVectors => error.
		{
			name: "3 multi-vectors, no targetVectors error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4]],
							[[0.5,0.5],[0.6,0.6],[0.7,0.7],[0.8,0.8]]
						]
					}
				}
			) { intField } } }`,
			expectedParams: nil,
		},
		// This fails because it is interpreted as a list of normal vectors, which should require
		// targetVectors to be specified since there are more than one.
		{
			name: "2 levels (multiple normal vectors), no targetVectors",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[0.1,0.1],[0.2,0.2],[0.3,0.3]
						]
					}
				}
			) { intField } } }`,
			expectedParams: nil,
		},
		// 3 levels but sub-vectors have inconsistent lengths, this is not a parse error, but
		// the query layer should handle this to decide if it is an error.
		{
			name: "3 levels but sub-vector length mismatch => error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2,0.2,0.2],[0.3]]
						]
					}
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2, 0.2, 0.2}, {0.3},
						},
					},
					TargetVectors: []string{"mymultivec2d"},
				},
			},
		},
		// one multi-vector with a target label
		{
			name: "single multi-vector + matching label",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]]
						]
					}
					targetVectors: ["mymultivec2d"]
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
					},
					TargetVectors: []string{"mymultivec2d"},
				},
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
		// two multi-vectors for the same target label => we have 2 items, each of shape >=3 levels.
		// The doc example:
		// => valid if we parse each multi-vector separately with the same label.
		{
			name: "2 multi-vectors for same label",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4]]
						]
					}
					targetVectors: ["mymultivec2d", "mymultivec2d"]
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					// two multi-vectors, each a [][]float32
					Vectors: []models.Vector{
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
						[][]float32{
							{0.4, 0.4},
						},
					},
					TargetVectors: []string{"mymultivec2d", "mymultivec2d"},
				},
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
		// two multi-vectors over two labels, no target vectors => error
		{
			name: "2 multi-vectors, no multi-vector labels error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
						],
						mymultivec1d: [
							[[0.5],[0.6]]
						],
					}
				}
			) { intField } } }`,
			expectedParams: nil,
		},
		// multiple multi-vectors over multiple labels, total # must match.
		// => 3 multi-vectors total => 3 target labels => valid
		{
			name: "3 multi-vectors, 3 matching labels",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4]]
						],
						mymultivec1d: [
							[[0.5],[0.6]]
						],
					}
					targetVectors: ["mymultivec2d","mymultivec2d","mymultivec1d"]
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName:  "SomeThing",
				Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					Vectors: []models.Vector{
						// 3) 2x1
						[][]float32{
							{0.5},
							{0.6},
						},
						// 1) 3x2
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
						// 2) 1x2
						[][]float32{
							{0.4, 0.4},
						},
					},
					TargetVectors: []string{"mymultivec1d", "mymultivec2d", "mymultivec2d"},
				},
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
		// If multiple multi-vectors appear but `targetVectors` doesn’t match the count => error
		{
			name: "mismatch between multi-vectors and labels error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.4,0.4]]
						],
						mymultivec1d: [
							[[0.1],[0.2]]
						],
						targetVectors: ["mymultivec2d","mymultivec1d"] 
					}
				}
			) { intField } } }`,
			expectedParams: nil, // we've got 3 multi-vectors but only 2 labels => error
		},
		// Within a single target vector, you cannot mix normal vectors (2-level) and multi-vectors (>=3-level).
		{
			name: "mix normal + multi in one target error",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[0.4,0.5]
						],
						targetVectors: ["mymultivec2d","mymultivec2d"]
					}
				}
			) { intField } } }`,
			expectedParams: nil,
		},
		// However, you *can* do multi-vectors for one target vector and normal vectors for another, as
		//    long as each target is “internally consistent” and the `targetVectors` count lines up.
		{
			name: "multi-vector in one, normal vectors in another",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]],
							[[0.5,0.5],[0.6,0.6],[0.7,0.7],[0.8,0.8]]
						],
						mymultivec3d: [
							[0.1,0.2,0.3],
							[0.4,0.5,0.6],
							[0.7,0.8,0.9]
						],
					}
					targetVectors: ["mymultivec2d","mymultivec2d","mymultivec3d","mymultivec3d","mymultivec3d"]
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName: "SomeThing",
				Properties: []search.SelectProperty{
					{Name: "intField", IsPrimitive: true},
				},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					// two multi-vectors, each a [][]float32:
					Vectors: []models.Vector{
						// 1) the first multi-vector (3x2)
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
						// 2) the second multi-vector (4x2)
						[][]float32{
							{0.5, 0.5}, {0.6, 0.6}, {0.7, 0.7}, {0.8, 0.8},
						},
						// Next 3 are normal vectors of dimension 3
						[]float32{0.1, 0.2, 0.3},
						[]float32{0.4, 0.5, 0.6},
						[]float32{0.7, 0.8, 0.9},
					},
					TargetVectors: []string{
						"mymultivec2d", "mymultivec2d",
						"mymultivec3d", "mymultivec3d", "mymultivec3d",
					},
				},
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
		// multi-vector in one target, single vector in another => valid
		{
			name: "multi-vector in one, single vector in other",
			query: `{ Get { SomeThing(
				limit: -1
				nearVector: {
					vectorPerTarget: {
						mymultivec2d: [
							[[0.1,0.1],[0.2,0.2],[0.3,0.3]]
						],
						mymultivec3d: [
							0.1, 0.2, 0.3
						]
					}
					targetVectors: ["mymultivec2d","mymultivec3d"]
				}
			) { intField } } }`,
			expectedParams: &dto.GetParams{
				ClassName: "SomeThing",
				Properties: []search.SelectProperty{
					{Name: "intField", IsPrimitive: true},
				},
				Pagination: &filters.Pagination{Limit: -1},
				NearVector: &searchparams.NearVector{
					// two multi-vectors, each a [][]float32:
					Vectors: []models.Vector{
						// the first multi-vector (3x2)
						[][]float32{
							{0.1, 0.1}, {0.2, 0.2}, {0.3, 0.3},
						},
						// 3 dimensional normal vector
						[]float32{0.1, 0.2, 0.3},
					},
					TargetVectors: []string{
						"mymultivec2d",
						"mymultivec3d",
					},
				},
				TargetVectorCombination: &dto.TargetCombination{Type: dto.Minimum},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectedParams != nil {
				// If we expect a successful parse, mock the resolver calls
				resolver.On("GetClass", *tc.expectedParams).
					Return([]interface{}{}, nil).Once()
				resolver.AssertResolve(t, tc.query)
			} else {
				// Otherwise, we expect a parse/validation error
				resolver.AssertFailToResolve(t, tc.query)
			}
		})
	}
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
					GroupBy:              &searchparams.GroupBy{Property: "path", Groups: 2, ObjectsPerGroup: 3, Properties: search.SelectProperties{}},
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
