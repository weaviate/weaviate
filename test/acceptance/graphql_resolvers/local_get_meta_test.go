//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestLocalGetMetaWithoutFilters(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				GetMeta{
					Things {
						City {
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
							InCountry {
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
			}
		}
	`)

	t.Run("meta count", func(t *testing.T) {
		count := result.Get("Local", "GetMeta", "Things", "City", "meta", "count").Result
		expected := json.Number("4")
		assert.Equal(t, expected, count)
	})

	t.Run("boolean props", func(t *testing.T) {
		isCapital := result.Get("Local", "GetMeta", "Things", "City", "isCapital").Result
		expected := map[string]interface{}{
			"count":           json.Number("4"),
			"percentageTrue":  json.Number("0.5"),
			"percentageFalse": json.Number("0.5"),
			"totalTrue":       json.Number("2"),
			"totalFalse":      json.Number("2"),
			"type":            "boolean",
		}
		assert.Equal(t, expected, isCapital)
	})

	t.Run("int/number props", func(t *testing.T) {
		isCapital := result.Get("Local", "GetMeta", "Things", "City", "population").Result
		expected := map[string]interface{}{
			"mean":    json.Number("1917500"),
			"count":   json.Number("4"),
			"maximum": json.Number("3470000"),
			"minimum": json.Number("600000"),
			"sum":     json.Number("7670000"),
			"type":    "int",
		}
		assert.Equal(t, expected, isCapital)
	})

	t.Run("ref prop", func(t *testing.T) {
		inCountry := result.Get("Local", "GetMeta", "Things", "City", "InCountry").Result
		expected := map[string]interface{}{
			"pointingTo": []interface{}{"Country"},
			"type":       "cref",
		}
		assert.Equal(t, expected, inCountry)
	})

	t.Run("string prop", func(t *testing.T) {
		typeField := result.Get("Local", "GetMeta", "Things", "City", "name", "type").Result
		count := result.Get("Local", "GetMeta", "Things", "City", "name", "count").Result
		topOccurrences := result.Get("Local", "GetMeta", "Things", "City", "name", "topOccurrences").Result

		assert.Equal(t, json.Number("4"), count)
		assert.Equal(t, "string", typeField)

		expectedTopOccurrences := []interface{}{
			map[string]interface{}{
				"value":  "Amsterdam",
				"occurs": json.Number("1"),
			},
			map[string]interface{}{
				"value":  "Dusseldorf",
				"occurs": json.Number("1"),
			},
			map[string]interface{}{
				"value":  "Rotterdam",
				"occurs": json.Number("1"),
			},
		}
		assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
	})
}

func TestLocalGetMetaWithFilters(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				GetMeta{
					Things {
						City (where: {
							valueBoolean: true,
							operator: Equal,
							path: ["isCapital"]
						}){
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
							InCountry {
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
			}
		}
	`)

	t.Run("meta count", func(t *testing.T) {
		count := result.Get("Local", "GetMeta", "Things", "City", "meta", "count").Result
		expected := json.Number("2")
		assert.Equal(t, expected, count)
	})

	t.Run("boolean props", func(t *testing.T) {
		isCapital := result.Get("Local", "GetMeta", "Things", "City", "isCapital").Result
		expected := map[string]interface{}{
			"count":           json.Number("2"),
			"percentageTrue":  json.Number("1"),
			"percentageFalse": json.Number("0"),
			"totalTrue":       json.Number("2"),
			"totalFalse":      json.Number("0"),
			"type":            "boolean",
		}
		assert.Equal(t, expected, isCapital)
	})

	t.Run("int/number props", func(t *testing.T) {
		isCapital := result.Get("Local", "GetMeta", "Things", "City", "population").Result
		expected := map[string]interface{}{
			"mean":    json.Number("2635000"),
			"count":   json.Number("2"),
			"maximum": json.Number("3470000"),
			"minimum": json.Number("1800000"),
			"sum":     json.Number("5270000"),
			"type":    "int",
		}
		assert.Equal(t, expected, isCapital)
	})

	t.Run("ref prop", func(t *testing.T) {
		inCountry := result.Get("Local", "GetMeta", "Things", "City", "InCountry").Result
		expected := map[string]interface{}{
			"pointingTo": []interface{}{"Country"},
			"type":       "cref",
		}
		assert.Equal(t, expected, inCountry)
	})

	t.Run("string prop", func(t *testing.T) {
		typeField := result.Get("Local", "GetMeta", "Things", "City", "name", "type").Result
		count := result.Get("Local", "GetMeta", "Things", "City", "name", "count").Result
		topOccurrences := result.Get("Local", "GetMeta", "Things", "City", "name", "topOccurrences").Result

		assert.Equal(t, json.Number("2"), count)
		assert.Equal(t, "string", typeField)

		expectedTopOccurrences := []interface{}{
			map[string]interface{}{
				"value":  "Amsterdam",
				"occurs": json.Number("1"),
			},
			map[string]interface{}{
				"value":  "Berlin",
				"occurs": json.Number("1"),
			},
		}
		assert.ElementsMatch(t, expectedTopOccurrences, topOccurrences)
	})
}

// This test prevents a regression on the fix for
// https://github.com/semi-technologies/weaviate/issues/824
func TestLocalGetMeta_StringPropsNotSetEverywhere(t *testing.T) {
	AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				GetMeta{
					Actions {
						Event {
							name {
								topOccurrences {
									occurs
									value
								}
							}
						}
					}
				}
			}
		}
	`)

}

// This test prevents a regression on the fix for
// https://github.com/semi-technologies/weaviate/issues/824
func TestLocalGetMeta_TextPropsNotSetEverywhere(t *testing.T) {
	AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				GetMeta{
					Actions {
						Event {
							description {
								topOccurrences {
									occurs
									value
								}
							}
						}
					}
				}
			}
		}
	`)

}
