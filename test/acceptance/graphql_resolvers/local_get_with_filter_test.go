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
					Things {
						Airport {
							code
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Things", "Airport").AsSlice()

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
					Things {
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
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airports := result.Get("Get", "Things", "Airport").AsSlice()

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
						Things {
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
			}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		cityMeta := result.Get("Aggregate", "Things", "City").AsSlice()[0]

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
						Things {
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
			}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		airport := result.Get("Get", "Things", "Airport").AsSlice()[0]

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
						Things {
							Person(where:{
								valueInt: %d
								operator:%s,
								path:["livesIn"]
							}) {
							  name
							}
						}
					}
			}
		`, count, op)
		}

		t.Run("no refs", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Equal", 0))
			// Alice should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Things", "Person").AsSlice(), 1)
			name := result.Get("Get", "Things", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Alice", name)
		})

		t.Run("exactly one", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("Equal", 1))
			// bob should be the only person that has zero places she lives in
			require.Len(t, result.Get("Get", "Things", "Person").AsSlice(), 1)
			name := result.Get("Get", "Things", "Person").AsSlice()[0].(map[string]interface{})["name"]
			assert.Equal(t, "Bob", name)
		})

		t.Run("2 or more", func(t *testing.T) {
			result := AssertGraphQL(t, helper.RootAuth, query("GreaterThanEqual", 2))
			// both john(2) and petra(3) should match
			require.Len(t, result.Get("Get", "Things", "Person").AsSlice(), 2)
			name1 := result.Get("Get", "Things", "Person").AsSlice()[0].(map[string]interface{})["name"]
			name2 := result.Get("Get", "Things", "Person").AsSlice()[1].(map[string]interface{})["name"]
			assert.ElementsMatch(t, []string{"John", "Petra"}, []string{name1.(string), name2.(string)})
		})
	})
}
