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
	"time"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
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
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"code": "10000"},
			map[string]interface{}{"code": "20000"},
			map[string]interface{}{"code": "30000"},
			map[string]interface{}{"code": "40000"},
		}

		assert.ElementsMatch(t, expected, airports)
	})

	t.Run("nearText with prop length", func(t *testing.T) {
		query := `
		{
			  Get {
				City (
					nearText: {
						concepts: ["hi"],
						distance: 0.9
					},
					where: {
						path: "len(name)"
						operator: GreaterThanEqual
						valueInt: 0
					}
				) {
				  name
				}
			  }
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()
		assert.Len(t, cities, 5)
	})

	t.Run("nearText with null filter", func(t *testing.T) {
		query := `
		{
			  Get {
				City (
					nearText: {
						concepts: ["hi"],
						distance: 0.9
					},
					where: {
						path: "name"
						operator: IsNull
						valueBoolean: true
					}
				) {
				  name
				}
			  }
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		cities := result.Get("Get", "City").AsSlice()
		assert.Len(t, cities, 1)
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
							valueText:"Germany"
							path:["inCity", "City", "inCountry", "Country", "name"]
						}
					]
				}){
					code
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
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
							valueText:"Amsterdam",
							operator:Equal,
							path:["name"]
						}, {
							valueText:"Berlin",
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
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
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
						valueText:"Amsterdam",
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
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
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

	t.Run("with uuid filters applied", func(t *testing.T) {
		query := `
		{
			Get {
				Airport(where:{
					operator:And
					operands: [
						{
							operator: GreaterThan,
							valueText: "00000000-0000-0000-0000-000000010000",
							path:["airportId"]
						},
						{
							operator: LessThan,
							valueText: "00000000-0000-0000-0000-000000030000",
							path:["airportId"]
						},
						{
							operator: NotEqual,
							valueText: "00000000-0000-0000-0000-000000040000",
							path:["airportId"]
						}
					]
				}){
					code
					airportId
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Airport").AsSlice()

		expected := []interface{}{
			map[string]interface{}{
				"code":      "20000",
				"airportId": "00000000-0000-0000-0000-000000020000",
			},
		}

		assert.ElementsMatch(t, expected, airports)
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
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Equal", 0))
			// Alice should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Alice", name)
		})

		t.Run("exactly one", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Equal", 1))
			// bob should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Bob", name)
		})

		t.Run("2 or more", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("GreaterThanEqual", 2))
			// both john(2) and petra(3) should match
			require.Len(t, result.Get("Get", "Person").AsSlice(), 2)
			name1 := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			name2 := result.Get("Get", "Person").AsSlice()[1].(map[string]interface{})["name"]
			assert.ElementsMatch(t, []string{"John", "Petra"}, []string{name1.(string), name2.(string)})
		})
	})

	t.Run("filtering by property len", func(t *testing.T) {
		query := `{
				Get {
					ArrayClass(where:{
						valueInt: 4,
						operator:Equal,
						path:["len(texts)"]
					}) {
						texts
					}
				}
			}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		require.Len(t, result.Get("Get", "ArrayClass").AsSlice(), 1)
	})

	t.Run("filtering by null property", func(t *testing.T) {
		query := `{
				Get {
					ArrayClass(where:{
						valueBoolean: true,
						operator:IsNull,
						path:["texts"]
					}) {
						texts
					}
				}
			}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		require.Len(t, result.Get("Get", "ArrayClass").AsSlice(), 3) // empty, nil and len==0 objects
	})

	t.Run("filtering by property with field tokenization", func(t *testing.T) {
		// tests gh-1821 feature

		query := func(value string) string {
			return fmt.Sprintf(`
			{
				Get {
					Person(where:{
						valueText: "%s"
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
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Quality"))
			// Quality is not full field for anyone, therefore noone should be returned
			require.Len(t, result.Get("Get", "Person").AsSlice(), 0)
		})

		t.Run("just one is Mechanical Engineer", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Mechanical Engineer"))
			// Bob is Mechanical Engineer, though John is Senior
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Bob", name)
		})

		t.Run("just one is Senior Mechanical Engineer", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Senior Mechanical Engineer"))
			// so to get John, his full profession name has to be used
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "John", name)
		})

		t.Run("just one is Quality Assurance Manager", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Quality Assurance Manager"))
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
						valueText: "%s"
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
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("swimming"))
			// swimming is not full field for anyone, therefore noone should be returned
			require.Len(t, result.Get("Get", "Person").AsSlice(), 0)
		})

		t.Run("just one hates swimming", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("hates swimming"))
			// but only john hates swimming
			require.Len(t, result.Get("Get", "Person").AsSlice(), 1)
			name := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"].(string)
			assert.Equal(t, "John", name)
		})

		t.Run("exactly 2 loves travelling", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("loves travelling"))
			// bob and john loves travelling, alice loves traveling very much
			require.Len(t, result.Get("Get", "Person").AsSlice(), 2)
			name1 := result.Get("Get", "Person").AsSlice()[0].(map[string]interface{})["name"].(string)
			name2 := result.Get("Get", "Person").AsSlice()[1].(map[string]interface{})["name"].(string)
			assert.ElementsMatch(t, []string{"Bob", "John"}, []string{name1, name2})
		})

		t.Run("only one likes cooking for family", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("likes cooking for family"))
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
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("italian"))
			pizzas := result.Get("Get", "Pizza").AsSlice()
			require.Len(t, pizzas, 2)
			id1 := pizzas[0].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			id2 := pizzas[1].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			assert.Equal(t, quattroFormaggi.String(), id1)
			assert.Equal(t, fruttiDiMare.String(), id2)
		})

		t.Run("1 result by full description containing stopwords", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query("Universally accepted to be the best pizza ever created."))
			pizzas := result.Get("Get", "Pizza").AsSlice()
			require.Len(t, pizzas, 1)
			id1 := pizzas[0].(map[string]interface{})["_additional"].(map[string]interface{})["id"]
			assert.Equal(t, hawaii.String(), id1)
		})

		t.Run("error by description containing just stopwords", func(t *testing.T) {
			errors := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query("to be or not to be"))
			require.Len(t, errors, 1)
			assert.Contains(t, errors[0].Message, "invalid search term, only stopwords provided. Stopwords can be configured in class.invertedIndexConfig.stopwords")
		})
	})

	t.Run("with filtering by id", func(t *testing.T) {
		// this is the journey test for gh-1088

		query := `
			{
				Get {
					Airport(where:{
						valueText:"4770bb19-20fd-406e-ac64-9dac54c27a0f",
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
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
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

	t.Run("with filtering by timestamps", func(t *testing.T) {
		query := `
			{
				Get {
					Airport {
						_additional {
							id
							creationTimeUnix
							lastUpdateTimeUnix
						}
					}
				}
			}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		airport := result.Get("Get", "Airport").AsSlice()[0]
		additional := airport.(map[string]interface{})["_additional"]
		targetID := additional.(map[string]interface{})["id"].(string)
		targetCreationTime := additional.(map[string]interface{})["creationTimeUnix"].(string)
		targetUpdateTime := additional.(map[string]interface{})["lastUpdateTimeUnix"].(string)

		creationTimestamp, err := strconv.ParseInt(targetCreationTime, 10, 64)
		assert.Nil(t, err)
		creationDate := time.UnixMilli(creationTimestamp).Format(time.RFC3339)
		updateTimestamp, err := strconv.ParseInt(targetUpdateTime, 10, 64)
		assert.Nil(t, err)
		updateDate := time.UnixMilli(updateTimestamp).Format(time.RFC3339)

		t.Run("creationTimeUnix as timestamp", func(t *testing.T) {
			query := fmt.Sprintf(`
				{
					Get {
						Airport(
							where: {
								path: ["_creationTimeUnix"]
								operator: Equal
								valueText: "%s"
							}
						)
						{
							_additional {
								id
							}
						}
					}
				}
			`, targetCreationTime)

			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			airport := result.Get("Get", "Airport").AsSlice()[0]
			additional := airport.(map[string]interface{})["_additional"]
			resultID := additional.(map[string]interface{})["id"].(string)
			assert.Equal(t, targetID, resultID)
		})

		t.Run("creationTimeUnix as date", func(t *testing.T) {
			query := fmt.Sprintf(`
				{
					Get {
						Airport(
							where: {
								path: ["_creationTimeUnix"]
								operator: GreaterThanEqual
								valueDate: "%s"
							}
						)
						{
							_additional {
								id
							}
						}
					}
				}
			`, creationDate)

			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			airport := result.Get("Get", "Airport").AsSlice()[0]
			additional := airport.(map[string]interface{})["_additional"]
			resultID := additional.(map[string]interface{})["id"].(string)
			assert.Equal(t, targetID, resultID)
		})

		t.Run("lastUpdateTimeUnix as timestamp", func(t *testing.T) {
			query := fmt.Sprintf(`
				{
					Get {
						Airport(
							where: {
								path: ["_lastUpdateTimeUnix"]
								operator: Equal
								valueText: "%s"
							}
						)
						{
							_additional {
								id
							}
						}
					}
				}
			`, targetUpdateTime)

			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			airport := result.Get("Get", "Airport").AsSlice()[0]
			additional := airport.(map[string]interface{})["_additional"]
			resultID := additional.(map[string]interface{})["id"].(string)
			assert.Equal(t, targetID, resultID)
		})

		t.Run("lastUpdateTimeUnix as date", func(t *testing.T) {
			query := fmt.Sprintf(`
				{
					Get {
						Airport(
							where: {
								path: ["_lastUpdateTimeUnix"]
								operator: GreaterThanEqual
								valueDate: "%s"
							}
						)
						{
							_additional {
								id
							}
						}
					}
				}
			`, updateDate)

			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			airport := result.Get("Get", "Airport").AsSlice()[0]
			additional := airport.(map[string]interface{})["_additional"]
			resultID := additional.(map[string]interface{})["id"].(string)
			assert.Equal(t, targetID, resultID)
		})
	})

	t.Run("with id filter on object with no props", func(t *testing.T) {
		id := strfmt.UUID("f0ea8fb8-5a1f-449d-aed5-d68dc65cd644")
		defer deleteObjectClass(t, "NoProps")

		t.Run("setup test class and obj", func(t *testing.T) {
			createObjectClass(t, &models.Class{
				Class: "NoProps", Properties: []*models.Property{
					{Name: "unused", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
				},
			})

			createObject(t, &models.Object{Class: "NoProps", ID: id})
		})

		t.Run("do query", func(t *testing.T) {
			query := fmt.Sprintf(`
				{
					Get {
						NoProps(where:{operator:Equal path:["_id"] valueText:"%s"})
						{
							_additional {id}
						}
					}
				}
			`, id)
			response := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			result := response.Get("Get", "NoProps").AsSlice()
			require.Len(t, result, 1)
			additional := result[0].(map[string]interface{})["_additional"]
			resultID := additional.(map[string]interface{})["id"].(string)
			assert.Equal(t, id.String(), resultID)
		})
	})

	t.Run("with nul filter", func(t *testing.T) {
		tests := []struct {
			name    string
			value   bool
			Results []interface{}
		}{
			{
				name:    "Null values",
				value:   true,
				Results: []interface{}{"Missing Island", nil}, // one entry with null history has no name
			},
			{
				name:    "Non-null values",
				value:   false,
				Results: []interface{}{"Amsterdam", "Rotterdam", "Berlin", "Dusseldorf"},
			},
		}
		query := `
			{
				Get {
					City(where:{
						valueBoolean: %v,
						operator:IsNull,
						path:["history"]
					}) {
						name
					}
				}
			}
		`
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.value))
				cities := result.Get("Get", "City").AsSlice()
				require.Len(t, cities, len(tt.Results))
				for _, city := range cities {
					cityMap := city.(map[string]interface{})
					require.Contains(t, tt.Results, cityMap["name"])
				}
			})
		}
	})
}
