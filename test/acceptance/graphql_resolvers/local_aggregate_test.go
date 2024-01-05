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

package test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// This test prevents a regression on the fix for
// https://github.com/weaviate/weaviate/issues/824
func localMeta_StringPropsNotSetEverywhere(t *testing.T) {
	graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate {
				City {
					name {
						topOccurrences {
							occurs
							value
						}
					}
				}
			}
		}
	`)
}

func localMetaWithWhereAndNearTextFilters(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				City (where: {
					valueBoolean: true,
					operator: Equal,
					path: ["isCapital"]
				}
				nearText: {
					concepts: ["Amsterdam"]
					distance: 0.2
				}
				){
					meta {
						count
					}
					isCapital {
						count
						percentageFalse
						percentageTrue
						totalFalse
						totalTrue
						type
					}
					population {
						mean
						count
						maximum
						minimum
						sum
						type
					}
					inCountry {
						pointingTo
						type
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}
	`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("boolean props", func(t *testing.T) {
			isCapital := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["isCapital"]
			expected := map[string]interface{}{
				"count":           json.Number("1"),
				"percentageTrue":  json.Number("1"),
				"percentageFalse": json.Number("0"),
				"totalTrue":       json.Number("1"),
				"totalFalse":      json.Number("0"),
				"type":            "boolean",
			}
			assert.Equal(t, expected, isCapital)
		})

		t.Run("int/number props", func(t *testing.T) {
			population := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["population"]
			expected := map[string]interface{}{
				"mean":    json.Number("1800000"),
				"count":   json.Number("1"),
				"maximum": json.Number("1800000"),
				"minimum": json.Number("1800000"),
				"sum":     json.Number("1800000"),
				"type":    "int",
			}
			assert.Equal(t, expected, population)
		})

		t.Run("ref prop", func(t *testing.T) {
			inCountry := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["inCountry"]
			expected := map[string]interface{}{
				"pointingTo": []interface{}{"Country"},
				"type":       "cref",
			}
			assert.Equal(t, expected, inCountry)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "City").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Amsterdam",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})

	t.Run("with certainty", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				City (where: {
					valueBoolean: true,
					operator: Equal,
					path: ["isCapital"]
				}
				nearText: {
					concepts: ["Amsterdam"]
					certainty: 0.9
				}
				){
					meta {
						count
					}
					isCapital {
						count
						percentageFalse
						percentageTrue
						totalFalse
						totalTrue
						type
					}
					population {
						mean
						count
						maximum
						minimum
						sum
						type
					}
					inCountry {
						pointingTo
						type
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("boolean props", func(t *testing.T) {
			isCapital := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["isCapital"]
			expected := map[string]interface{}{
				"count":           json.Number("1"),
				"percentageTrue":  json.Number("1"),
				"percentageFalse": json.Number("0"),
				"totalTrue":       json.Number("1"),
				"totalFalse":      json.Number("0"),
				"type":            "boolean",
			}
			assert.Equal(t, expected, isCapital)
		})

		t.Run("int/number props", func(t *testing.T) {
			population := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["population"]
			expected := map[string]interface{}{
				"mean":    json.Number("1800000"),
				"count":   json.Number("1"),
				"maximum": json.Number("1800000"),
				"minimum": json.Number("1800000"),
				"sum":     json.Number("1800000"),
				"type":    "int",
			}
			assert.Equal(t, expected, population)
		})

		t.Run("ref prop", func(t *testing.T) {
			inCountry := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["inCountry"]
			expected := map[string]interface{}{
				"pointingTo": []interface{}{"Country"},
				"type":       "cref",
			}
			assert.Equal(t, expected, inCountry)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "City").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Amsterdam",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})
}

func localMetaWithWhereAndNearObjectFilters(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				City (where: {
					valueBoolean: true,
					operator: Equal,
					path: ["isCapital"]
				}
				nearObject: {
					id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
					distance: 0.2
				}
				){
					meta {
						count
					}
					isCapital {
						count
						percentageFalse
						percentageTrue
						totalFalse
						totalTrue
						type
					}
					population {
						mean
						count
						maximum
						minimum
						sum
						type
					}
					inCountry {
						pointingTo
						type
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("boolean props", func(t *testing.T) {
			isCapital := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["isCapital"]
			expected := map[string]interface{}{
				"count":           json.Number("1"),
				"percentageTrue":  json.Number("1"),
				"percentageFalse": json.Number("0"),
				"totalTrue":       json.Number("1"),
				"totalFalse":      json.Number("0"),
				"type":            "boolean",
			}
			assert.Equal(t, expected, isCapital)
		})

		t.Run("int/number props", func(t *testing.T) {
			population := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["population"]
			expected := map[string]interface{}{
				"mean":    json.Number("3470000"),
				"count":   json.Number("1"),
				"maximum": json.Number("3470000"),
				"minimum": json.Number("3470000"),
				"sum":     json.Number("3470000"),
				"type":    "int",
			}
			assert.Equal(t, expected, population)
		})

		t.Run("ref prop", func(t *testing.T) {
			inCountry := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["inCountry"]
			expected := map[string]interface{}{
				"pointingTo": []interface{}{"Country"},
				"type":       "cref",
			}
			assert.Equal(t, expected, inCountry)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "City").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Berlin",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})

	t.Run("with certainty", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				City (where: {
					valueBoolean: true,
					operator: Equal,
					path: ["isCapital"]
				}
				nearObject: {
					id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
					certainty: 0.9
				}
				){
					meta {
						count
					}
					isCapital {
						count
						percentageFalse
						percentageTrue
						totalFalse
						totalTrue
						type
					}
					population {
						mean
						count
						maximum
						minimum
						sum
						type
					}
					inCountry {
						pointingTo
						type
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("boolean props", func(t *testing.T) {
			isCapital := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["isCapital"]
			expected := map[string]interface{}{
				"count":           json.Number("1"),
				"percentageTrue":  json.Number("1"),
				"percentageFalse": json.Number("0"),
				"totalTrue":       json.Number("1"),
				"totalFalse":      json.Number("0"),
				"type":            "boolean",
			}
			assert.Equal(t, expected, isCapital)
		})

		t.Run("int/number props", func(t *testing.T) {
			population := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["population"]
			expected := map[string]interface{}{
				"mean":    json.Number("3470000"),
				"count":   json.Number("1"),
				"maximum": json.Number("3470000"),
				"minimum": json.Number("3470000"),
				"sum":     json.Number("3470000"),
				"type":    "int",
			}
			assert.Equal(t, expected, population)
		})

		t.Run("ref prop", func(t *testing.T) {
			inCountry := result.Get("Aggregate", "City").AsSlice()[0].(map[string]interface{})["inCountry"]
			expected := map[string]interface{}{
				"pointingTo": []interface{}{"Country"},
				"type":       "cref",
			}
			assert.Equal(t, expected, inCountry)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "City").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Berlin",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})
}

func localMetaWithNearVectorFilter(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				CustomVectorClass(
					nearVector: {
						vector: [1,0,0]
						distance: 0.0002
					}
				){
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "CustomVectorClass").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "CustomVectorClass").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Mercedes",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})

	t.Run("with certainty", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
		{
			Aggregate{
				CustomVectorClass(
					nearVector: {
						vector: [1,0,0]
						certainty: 0.9999
					}
				){
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
			}
		}`)

		t.Run("meta count", func(t *testing.T) {
			meta := result.Get("Aggregate", "CustomVectorClass").AsSlice()[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			expected := json.Number("1")
			assert.Equal(t, expected, count)
		})

		t.Run("string prop", func(t *testing.T) {
			name := result.Get("Aggregate", "CustomVectorClass").
				AsSlice()[0].(map[string]interface{})["name"].(map[string]interface{})
			typeField := name["type"]
			topOccurrences := name["topOccurrences"]

			assert.Equal(t, schema.DataTypeText.String(), typeField)

			expectedTopOccurrences := []interface{}{
				map[string]interface{}{
					"value":  "Mercedes",
					"occurs": json.Number("1"),
				},
			}
			assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
		})
	})
}

func localMetaWithWhereAndNearVectorFilters(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		t.Run("with expected results, low certainty", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Ford"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							distance: 0.6
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}
		`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 1)
			val := topOcc[0].(map[string]interface{})["value"]
			assert.Equal(t, "Ford", val)
		})

		t.Run("with no expected results, low distance", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Ford"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							distance: 0.2
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}
		`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 0)
		})

		t.Run("with expected results, low distance", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Mercedes"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							distance: 0.1
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 1)
			val := topOcc[0].(map[string]interface{})["value"]
			assert.Equal(t, "Mercedes", val)
		})
	})

	t.Run("with certainty", func(t *testing.T) {
		t.Run("with expected results, low certainty", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Ford"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							certainty: 0.7
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}
		`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 1)
			val := topOcc[0].(map[string]interface{})["value"]
			assert.Equal(t, "Ford", val)
		})

		t.Run("with no expected results, high certainty", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Ford"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							certainty: 0.9
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}
		`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 0)
		})

		t.Run("with expected results, high certainty", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
					CustomVectorClass(
						where: {
							valueText: "Mercedes"
							operator: Equal
							path: ["name"]
						}
						nearVector: {
							vector: [1,0,0]
							certainty: 0.9
						}
					) {
					meta {
						count
					}
					name {
						topOccurrences {
							occurs
							value
						}
						type
						count
					}
				}
				}
			}
		`)

			require.NotNil(t, result)

			agg := result.Result.(map[string]interface{})["Aggregate"].(map[string]interface{})
			cls := agg["CustomVectorClass"].([]interface{})
			require.Len(t, cls, 1)
			name := cls[0].(map[string]interface{})["name"].(map[string]interface{})
			topOcc := name["topOccurrences"].([]interface{})
			require.Len(t, topOcc, 1)
			val := topOcc[0].(map[string]interface{})["value"]
			assert.Equal(t, "Mercedes", val)
		})
	})
}

func localMetaWithWhereGroupByNearMediaFilters(t *testing.T) {
	t.Run("with nearObject", func(t *testing.T) {
		query := `
			{
				Aggregate {
					Company
					(
						groupBy: "name"
						nearObject: {id: "cfa3b21e-ca4f-4db7-a432-7fc6a23c534d", certainty: 0.99}
					) 
					{
						groupedBy {
							value
					  	}
						meta {
							count
						}
					}
				}
			}`

		expected := map[string]interface{}{
			"Aggregate": map[string]interface{}{
				"Company": []interface{}{
					map[string]interface{}{
						"groupedBy": map[string]interface{}{
							"value": "Microsoft Inc.",
						},
						"meta": map[string]interface{}{
							"count": json.Number("1"),
						},
					},
				},
			},
		}

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Result
		assert.EqualValues(t, expected, result)
	})

	t.Run("with nearText", func(t *testing.T) {
		t.Run("with distance", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (
					groupBy: "population"
					where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
						distance: 0.2
					}
					){
						meta {
							count
						}
						groupedBy {
							value
						}
					}
				}
			}
		`)

			expected := map[string]interface{}{
				"Aggregate": map[string]interface{}{
					"City": []interface{}{
						map[string]interface{}{
							"groupedBy": map[string]interface{}{
								"value": "1.8e+06",
							},
							"meta": map[string]interface{}{
								"count": json.Number("1"),
							},
						},
					},
				},
			}

			assert.EqualValues(t, expected, result.Result)
		})

		t.Run("with certainty", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate{
					City (
					groupBy: "population"
					where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
						certainty: 0.9
					}
					){
						meta {
							count
						}
						groupedBy {
							value
						}
					}
				}
			}
		`)

			expected := map[string]interface{}{
				"Aggregate": map[string]interface{}{
					"City": []interface{}{
						map[string]interface{}{
							"groupedBy": map[string]interface{}{
								"value": "1.8e+06",
							},
							"meta": map[string]interface{}{
								"count": json.Number("1"),
							},
						},
					},
				},
			}

			assert.EqualValues(t, expected, result.Result)
		})
	})

	t.Run("with nearVector", func(t *testing.T) {
		getQuery := `
			{
				Get {
					Company(where: {
						path: ["name"]
						operator: Equal
						valueText: "Google Inc."
					})
					{
						_additional {
							vector
						}
					}
				}
			}`

		vectorResult := graphqlhelper.AssertGraphQL(t, helper.RootAuth, getQuery).
			Get("Get", "Company").
			AsSlice()[0].(map[string]interface{})["_additional"].(map[string]interface{})["vector"].([]interface{})

		vector := make([]float32, len(vectorResult))
		for i, ifc := range vectorResult {
			val, err := strconv.ParseFloat(ifc.(json.Number).String(), 32)
			require.Nil(t, err)
			vector[i] = float32(val)
		}

		aggQuery := fmt.Sprintf(`
			{
				Aggregate {
					Company
					(
						groupBy: "name"
						nearVector: {vector: %+v, certainty: 0.99}
					)
					{
						groupedBy {
							value
						}
						meta {
							count
						}
					}
				}
			}
		`, vector)

		aggResult := graphqlhelper.AssertGraphQL(t, helper.RootAuth, aggQuery).Result

		expected := map[string]interface{}{
			"Aggregate": map[string]interface{}{
				"Company": []interface{}{
					map[string]interface{}{
						"groupedBy": map[string]interface{}{
							"value": "Google Inc.",
						},
						"meta": map[string]interface{}{
							"count": json.Number("1"),
						},
					},
				},
			},
		}

		assert.EqualValues(t, expected, aggResult)
	})
}

func localMetaWithObjectLimit(t *testing.T) {
	t.Run("with nearObject and distance", func(t *testing.T) {
		objectLimit := 1
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
			{
				Aggregate{
					City (
						objectLimit: %d
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
							distance: 0.3
						}
					){
						meta {
							count
						}
					}
				}
			}
		`, objectLimit))

		t.Run("validate objectLimit functions as expected", func(t *testing.T) {
			res := result.Get("Aggregate", "City").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number(fmt.Sprint(objectLimit)), count)
		})
	})

	t.Run("with nearObject and certainty", func(t *testing.T) {
		objectLimit := 1
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
			{
				Aggregate{
					City (
						objectLimit: %d
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
							certainty: 0.7
						}
					){
						meta {
							count
						}
					}
				}
			}
		`, objectLimit))

		t.Run("validate objectLimit functions as expected", func(t *testing.T) {
			res := result.Get("Aggregate", "City").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number(fmt.Sprint(objectLimit)), count)
		})
	})

	t.Run("with nearObject and no certainty", func(t *testing.T) {
		objectLimit := 2
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
			{
				Aggregate{
					City (
						objectLimit: %d
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
						}
					){
						meta {
							count
						}
					}
				}
			}
		`, objectLimit))

		t.Run("validate objectLimit functions as expected", func(t *testing.T) {
			res := result.Get("Aggregate", "City").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number(fmt.Sprint(objectLimit)), count)
		})
	})

	t.Run("with nearObject and very high distance, no objectLimit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
   				RansomNote(
     					nearText: {
							concepts: ["abc"]
							distance: 1.9998
     					}
   				) {
					  meta {
						count
					  }
  					}
 				}
			}
		`)

		t.Run("validate nearMedia runs unlimited without objectLimit", func(t *testing.T) {
			res := result.Get("Aggregate", "RansomNote").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number("500"), count)
		})
	})

	t.Run("with nearObject and very low certainty, no objectLimit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
   				RansomNote(
     					nearText: {
							concepts: ["abc"]
							certainty: 0.0001
     					}
   				) {
					  meta {
						count
					  }
  					}
 				}
			}
		`)

		t.Run("validate nearMedia runs unlimited without objectLimit", func(t *testing.T) {
			res := result.Get("Aggregate", "RansomNote").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number("500"), count)
		})
	})

	t.Run("with nearObject and low distance (few results), high objectLimit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
   				RansomNote(
     					nearText: {
							concepts: ["abc"]
							distance: 0.6 # should return about 6 elements
     					}
						  objectLimit:100,
   				) {
					  meta {
						count
					  }
  					}
 				}
			}
		`)

		t.Run("validate fewer than objectLimit elements are returned", func(t *testing.T) {
			res := result.Get("Aggregate", "RansomNote").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			countParsed, err := count.(json.Number).Int64()
			require.Nil(t, err)
			assert.Less(t, countParsed, int64(100))
		})
	})

	t.Run("with nearObject and high certainty (few results), high objectLimit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Aggregate {
   				RansomNote(
     					nearText: {
							concepts: ["abc"]
							certainty: 0.7 # should return about 6 elements
     					}
						  objectLimit:100,
   				) {
					  meta {
						count
					  }
  					}
 				}
			}
		`)

		t.Run("validate fewer than objectLimit elements are returned", func(t *testing.T) {
			res := result.Get("Aggregate", "RansomNote").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			countParsed, err := count.(json.Number).Int64()
			require.Nil(t, err)
			assert.Less(t, countParsed, int64(100))
		})
	})

	t.Run("with nearText and no distance/certainty, where filter and groupBy", func(t *testing.T) {
		objectLimit := 4
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
			{
				Aggregate {
					Company (
						groupBy: ["name"]
						where: {
							valueText: "Apple*",
							operator: Like,
							path: ["name"]
						}
						objectLimit: %d
						nearText: {
							concepts: ["Apple"]
							certainty: 0.5
						}
					){
						meta {
							count
						}
						groupedBy {
        					value
						}
					}
				}
			}
		`, objectLimit))

		expected := []interface{}{
			map[string]interface{}{
				"groupedBy": map[string]interface{}{
					"value": "Apple Incorporated",
				},
				"meta": map[string]interface{}{
					"count": json.Number("1"),
				},
			},
			map[string]interface{}{
				"groupedBy": map[string]interface{}{
					"value": "Apple Inc.",
				},
				"meta": map[string]interface{}{
					"count": json.Number("1"),
				},
			},
			map[string]interface{}{
				"groupedBy": map[string]interface{}{
					"value": "Apple",
				},
				"meta": map[string]interface{}{
					"count": json.Number("1"),
				},
			},
		}

		companies := result.Get("Aggregate", "Company").Result.([]interface{})
		for _, company := range companies {
			assert.Contains(t, expected, company)
		}
	})

	t.Run("with nearObject and certainty, where filter", func(t *testing.T) {
		objectLimit := 1
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
			{
				Aggregate{
					City (
						where: {
							valueBoolean: true,
							operator: Equal,
							path: ["isCapital"]
						}
						objectLimit: %d
						nearObject: {
							id: "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
						}
					){
						meta {
							count
						}
					}
				}
			}
		`, objectLimit))

		t.Run("validate objectLimit functions as expected", func(t *testing.T) {
			res := result.Get("Aggregate", "City").AsSlice()
			require.Len(t, res, 1)
			meta := res[0].(map[string]interface{})["meta"]
			count := meta.(map[string]interface{})["count"]
			assert.Equal(t, json.Number(fmt.Sprint(objectLimit)), count)
		})
	})
}

func aggregatesOnDateFields(t *testing.T) {
	t.Run("without grouping", func(t *testing.T) {
		query := `
		{
			Aggregate {
				HasDateField {
					timestamp {
						count
						minimum
						maximum
						median
					}
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "HasDateField").AsSlice()
		assert.Len(t, result, 1)

		expected := []interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("10"),
					"maximum": "2022-06-16T22:19:11.837473Z",
					"median":  "2022-06-16T22:19:06.1449075Z",
					"minimum": "2022-06-16T22:18:59.640162Z",
				},
			},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("with grouping on a unique field", func(t *testing.T) {
		query := `
		{
			Aggregate {
				HasDateField 
				(
					groupBy: "unique"
				)
				{
					timestamp {
						count
						minimum
						maximum
						median
						mode
					}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "HasDateField").AsSlice()
		assert.Len(t, result, 10)

		expected := []interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:05.894857Z",
					"median":  "2022-06-16T22:19:05.894857Z",
					"minimum": "2022-06-16T22:19:05.894857Z",
					"mode":    "2022-06-16T22:19:05.894857Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:08.112395Z",
					"median":  "2022-06-16T22:19:08.112395Z",
					"minimum": "2022-06-16T22:19:08.112395Z",
					"mode":    "2022-06-16T22:19:08.112395Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:03.495596Z",
					"median":  "2022-06-16T22:19:03.495596Z",
					"minimum": "2022-06-16T22:19:03.495596Z",
					"mode":    "2022-06-16T22:19:03.495596Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:07.589828Z",
					"median":  "2022-06-16T22:19:07.589828Z",
					"minimum": "2022-06-16T22:19:07.589828Z",
					"mode":    "2022-06-16T22:19:07.589828Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:06.394958Z",
					"median":  "2022-06-16T22:19:06.394958Z",
					"minimum": "2022-06-16T22:19:06.394958Z",
					"mode":    "2022-06-16T22:19:06.394958Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:11.837473Z",
					"median":  "2022-06-16T22:19:11.837473Z",
					"minimum": "2022-06-16T22:19:11.837473Z",
					"mode":    "2022-06-16T22:19:11.837473Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:18:59.640162Z",
					"median":  "2022-06-16T22:18:59.640162Z",
					"minimum": "2022-06-16T22:18:59.640162Z",
					"mode":    "2022-06-16T22:18:59.640162Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:01.495967Z",
					"median":  "2022-06-16T22:19:01.495967Z",
					"minimum": "2022-06-16T22:19:01.495967Z",
					"mode":    "2022-06-16T22:19:01.495967Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:10.339493Z",
					"median":  "2022-06-16T22:19:10.339493Z",
					"minimum": "2022-06-16T22:19:10.339493Z",
					"mode":    "2022-06-16T22:19:10.339493Z",
				},
			},
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("1"),
					"maximum": "2022-06-16T22:19:04.3828349Z",
					"median":  "2022-06-16T22:19:04.3828349Z",
					"minimum": "2022-06-16T22:19:04.3828349Z",
					"mode":    "2022-06-16T22:19:04.3828349Z",
				},
			},
		}

		for _, res := range result {
			assert.Contains(t, expected, res)
		}
	})

	t.Run("group on identical field", func(t *testing.T) {
		query := `
		{
			Aggregate {
				HasDateField 
				(
					groupBy: "identical"
				)
				{
					timestamp {
						count
						minimum
						maximum
						median
					}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Aggregate", "HasDateField").AsSlice()

		expected := []interface{}{
			map[string]interface{}{
				"timestamp": map[string]interface{}{
					"count":   json.Number("10"),
					"maximum": "2022-06-16T22:19:11.837473Z",
					"median":  "2022-06-16T22:19:06.1449075Z",
					"minimum": "2022-06-16T22:18:59.640162Z",
				},
			},
		}

		assert.Equal(t, expected, result)
	})
}
