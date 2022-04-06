//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gettingObjectsWithFilters(t *testing.T) {
	t.Run("without filters <- this is the control", func(t *testing.T) {
		query := `
		{
			Get {
				Airport {
					code
				}
			}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"code": "10000"},
			map[string]interface{}{"code": "20000"},
			map[string]interface{}{"code": "30000"},
			map[string]interface{}{"code": "40000"},
		}

		assert.ElementsMatch(t, expected, airports)
	})

	t.Run("with filters applied", func(t *testing.T) {
		query := `
		{
			Get {
				Airport(where:{
					operator:And
					operands: [
						{
							operator: GreaterThan,
							valueInt: 600000,
							path:["inCity", "City", "population"]
						}
						{
							operator: Equal,
							valueString:"Germany"
							path:["inCity", "City", "inCountry", "Country", "name"]
						}
					]
				}){
					code
				}
			}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"code": "40000"},
		}

		assert.ElementsMatch(t, expected, airports)
	})

	t.Run("with or filters applied", func(t *testing.T) {
		// this test was added to prevent a regression on the bugfix for gh-758

		query := `
			{
				Aggregate {
					City(where:{
						operator:Or
						operands:[{
							valueString:"Amsterdam",
							operator:Equal,
							path:["name"]
						}, {
							valueString:"Berlin",
							operator:Equal,
							path:["name"]
						}]
					}) {
						__typename
						name {
							__typename
							count
						}
					}
				}
			}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		cityMeta := result.Get("Aggregate", "City").AsSlice()[0]

		expected := map[string]interface{}{
			"__typename": "AggregateCity",
			"name": map[string]interface{}{
				"__typename": "AggregateCitynameObj",
				"count":      json.Number("2"),
			},
		}

		assert.Equal(t, expected, cityMeta)
	})

	t.Run("with filters and ref showing a phone number", func(t *testing.T) {
		// this is the journey test for gh-1088

		query := `
			{
				Get {
					Airport(where:{
						valueString:"Amsterdam",
						operator:Equal,
						path:["inCity", "City", "name"]
					}) {
						phone {
							internationalFormatted
							countryCode
							nationalFormatted
						}
					}
				}
			}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airport := result.Get("Get", "Airport").AsSlice()[0]

		expected := map[string]interface{}{
			"phone": map[string]interface{}{
				"internationalFormatted": "+31 1234567",
				"countryCode":            json.Number("31"),
				"nationalFormatted":      "1234567",
			},
		}

		assert.Equal(t, expected, airport)
	})

	t.Run("filtering for ref counts", func(t *testing.T) {
		// this is the journey test for gh-1101

		query := func(op string, count int) string {
			return fmt.Sprintf(`
			{
				Get {
					Person(where:{
						valueInt: %d
						operator:%s,
						path:["livesIn"]
					}) {
						name
					}
				}
			}
		`, count, op)
		}

		t.Run("no refs", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Equal", 0))
			// Alice should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Alice", name)
		})

		t.Run("exactly one", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Equal", 1))
			// bob should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Bob", name)
		})

		t.Run("2 or more", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("GreaterThanEqual", 2))
			// both john(2) and petra(3) should match
			require.Len(t, result.Get("Get", "Person").AsSlice(), 2)
			name1 := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			name2 := result.Get("Get", "Person").AsSlice()[1].(map[string]interface{})["name"]
			assert.ElementsMatch(t, []string{"John", "Petra"}, []string{name1.(string), name2.(string)})
		})
	})

	t.Run("filtering by property with field tokenization", func(t *testing.T) {
		// tests gh-1821 feature

		query := func(value string) string {
			return fmt.Sprintf(`
			{
				Get {
					Person(where:{
						valueString: "%s"
						operator:Equal,
						path:["profession"]
					}) {
						name
					}
				}
			}
		`, value)
		}

		t.Run("noone", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Quality"))
			// Quality is not full field for anyone, therefore noone should be returned
			require.Len(t, result.Get("Get", "Person").AsSlice(), 0)
		})

		t.Run("just one is Mechanical Engineer", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Mechanical Engineer"))
			// Bob is Mechanical Engineer, though John is Senior
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Bob", name)
		})

		t.Run("just one is Senior Mechanical Engineer", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Senior Mechanical Engineer"))
			// so to get John, his full profession name has to be used
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "John", name)
		})

		t.Run("just one is Quality Assurance Manager", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Quality Assurance Manager"))
			// petra is Quality Assurance Manager
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Petra", name)
		})
	})

	t.Run("filtering by array property with field tokenization", func(t *testing.T) {
		// tests gh-1821 feature

		query := func(value string) string {
			return fmt.Sprintf(`
			{
				Get {
					Person(where:{
						valueString: "%s"
						operator:Equal,
						path:["about"]
					}) {
						name
					}
				}
			}
		`, value)
		}

		t.Run("noone", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("swimming"))
			// swimming is not full field for anyone, therefore noone should be returned
			require.Len(t, result.Get("Get", "Person").AsSlice(), 0)
		})

		t.Run("just one hates swimming", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("hates swimming"))
			// but only john hates swimming
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"].(string)
			assert.Equal(t, "John", name)
		})

		t.Run("exactly 2 loves travelling", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("loves travelling"))
			// bob and john loves travelling, alice loves traveling very much
			require.Len(t, result.Get("Get", "Person").AsSlice(), 2)
			name1 := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"].(string)
			name2 := result.Get("Get", "Person").AsSlice()[1].(map[string]interface{})["name"].(string)
			assert.ElementsMatch(t, []string{"Bob", "John"}, []string{name1, name2})
		})

		t.Run("only one likes cooking for family", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("likes cooking for family"))
			// petra likes cooking for family, john simply likes cooking
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Petra", name)
		})
	})

	t.Run("filtering by stopwords", func(t *testing.T) {
		query := func(value string) string {
			return fmt.Sprintf(`
			{
				Get {
					Pizza(where:{
						valueText: "%s"
						operator:Equal,
						path:["description"]
					}) {
						name
						_additional{
							id
						}
					}
				}
			}
		`, value)
		}

		t.Run("2 results by partial description", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("italian"))
			pizzas := result.Get("Get", "Pizza").AsSlice()
			require.Len(t, pizzas, 2)
			id1 := pizzas[0].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			id2 := pizzas[1].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			assert.Equal(t, quattroFormaggi.String(), id1)
			assert.Equal(t, fruttiDiMare.String(), id2)
		})

		t.Run("1 result by full description containing stopwords", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Universally accepted to be the best pizza ever created."))
			pizzas := result.Get("Get", "Pizza").AsSlice()
			require.Len(t, pizzas, 1)
			id1 := pizzas[0].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			assert.Equal(t, hawaii.String(), id1)
		})

		t.Run("error by description containing just stopwords", func(t *testing.T) {
			errors := ErrorGraphQL(t, helper.RootAuth, query("to be or not to be"))
			require.Len(t, errors, 1)
			assert.Contains(t, errors[0].Message, "invalid search term, only stopwords provided. Stopwords can be configured in class.invertedIndexConfig.stopwords")
		})
	})

	t.Run("with filtering with id", func(t *testing.T) {
		// this is the journey test for gh-1088

		query := `
			{
				Get {
					Airport(where:{
						valueString:"4770bb19-20fd-406e-ac64-9dac54c27a0f",
						operator:Equal,
						path:["id"]
					}) {
						phone {
							internationalFormatted
							countryCode
							nationalFormatted
						}
					}
				}
			}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airport := result.Get("Get", "Airport").AsSlice()[0]

		expected := map[string]interface{}{
			"phone": map[string]interface{}{
				"internationalFormatted": "+31 1234567",
				"countryCode":            json.Number("31"),
				"nationalFormatted":      "1234567",
			},
		}

		assert.Equal(t, expected, airport)
	})
}
