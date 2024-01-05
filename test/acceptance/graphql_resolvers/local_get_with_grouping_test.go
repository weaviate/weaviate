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
	"strings"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func gettingObjectsWithGrouping(t *testing.T) {
	t.Run("without grouping <- this is the control", func(t *testing.T) {
		query := `
		{
			Get {
				Company {
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Microsoft Inc."},
			map[string]interface{}{"name": "Microsoft Incorporated"},
			map[string]interface{}{"name": "Microsoft"},
			map[string]interface{}{"name": "Apple Inc."},
			map[string]interface{}{"name": "Apple Incorporated"},
			map[string]interface{}{"name": "Apple"},
			map[string]interface{}{"name": "Google Inc."},
			map[string]interface{}{"name": "Google Incorporated"},
			map[string]interface{}{"name": "Google"},
		}

		assert.ElementsMatch(t, expected, companies)
	})

	t.Run("grouping mode set to merge and force to 1.0", func(t *testing.T) {
		query := `
		{
			Get {
				Company(group: {type: merge, force:1.0}) {
					name
					inCity {
						... on City {name}
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		require.Len(t, companies, 1)

		companyNames := companies[0].(map[string]interface{})["name"].(string)
		assert.NotEmpty(t, companies)

		mustContain := []string{"Apple", "Google", "Microsoft"}
		for _, companyName := range mustContain {
			if !strings.Contains(companyNames, companyName) {
				t.Errorf("%s not contained in %v", companyName, companyNames)
			}
		}

		companyCities := companies[0].(map[string]interface{})["inCity"].([]interface{})
		expectedCities := []map[string]interface{}{
			{"name": "Dusseldorf"},
			{"name": "Amsterdam"},
			{"name": "Berlin"},
		}

		assert.ElementsMatch(t, expectedCities, companyCities)
	})

	t.Run("grouping mode set to merge and force to 0.0", func(t *testing.T) {
		query := `
		{
			Get {
				Company(group: {type: merge, force:0.0}) {
					name
					inCity {
						... on City {
							name
						}
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		require.Len(t, companies, 9)

		getName := func(value map[string]interface{}) string {
			return value["name"].(string)
		}

		getCities := func(value map[string]interface{}) []string {
			inCity := value["inCity"].([]interface{})
			cities := make([]string, len(inCity))
			for i := range inCity {
				cityVal := inCity[i].(map[string]interface{})
				cities[i] = getName(cityVal)
			}
			return cities
		}

		for _, current := range companies {
			currentMap := current.(map[string]interface{})
			if getName(currentMap) == "Microsoft Incorporated" {
				assert.Len(t, getCities(currentMap), 2)
			}
			if getName(currentMap) == "Microsoft Inc." {
				assert.Len(t, getCities(currentMap), 1)
			}
			if getName(currentMap) == "Microsoft" {
				assert.Len(t, getCities(currentMap), 1)
			}
		}
	})

	t.Run("grouping mode set to closest and force to 0.1", func(t *testing.T) {
		query := `
			{
				Get {
					Company(group: {type: closest, force:0.1}) {
						name
						inCity {
							... on City {
								name
							}
						}
					}
				}
			}
			`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		assert.True(t, len(companies) > 0)
	})

	t.Run("grouping mode set to closest with near text", func(t *testing.T) {
		query := `
			{
				Get {
					Company(nearText: {concepts: "Apple"}, group: {type: closest, force:1.0}) {
						name
					}
				}
			}
			`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		assert.True(t, len(companies) == 1)
	})

	t.Run("grouping with where filter", func(t *testing.T) {
		query := `
			{
				Get {
					Company(group:{type:merge force:1.0} where:{path:["id"] operator:Like valueText:"*"}) {
						name
						inCity {
							... on City {
								name
							}
						}
					}
				}
			}
		`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		grouped := result.Get("Get", "Company").AsSlice()
		require.Len(t, grouped, 1)
		groupedName := grouped[0].(map[string]interface{})["name"].(string)
		assert.Equal(t, "Microsoft Inc. (Microsoft Incorporated, Microsoft, Apple Inc., "+
			"Apple Incorporated, Apple, Google Inc., Google Incorporated, Google)",
			groupedName)

		companyCities := grouped[0].(map[string]interface{})["inCity"].([]interface{})
		expectedCities := []map[string]interface{}{
			{"name": "Dusseldorf"},
			{"name": "Amsterdam"},
			{"name": "Berlin"},
		}

		assert.ElementsMatch(t, expectedCities, companyCities)

		// this query should yield the same results as the above, as the above where filter will
		// match all records. checking the previous payload with the one below is a sanity check
		// for the sake of validating the fix for [github issue 1958]
		// (https://github.com/weaviate/weaviate/issues/1958)
		// UPDATE: due to introducing roaring bitmaps as set holding docIDs of filtered documents
		// internal order of results returned has changed from property value based to docID based,
		// but set content remain unchanged
		// for that reason grouped name in the following test is different with and without filters,
		// though it still contains the same elements
		queryWithoutWhere := `
			{
				Get {
					Company(group:{type:merge force:1.0}) {
						name
						inCity {
							... on City {
								name
							}
						}
					}
				}
			}
		`
		result = graphqlhelper.AssertGraphQL(t, helper.RootAuth, queryWithoutWhere)
		groupedWithoutWhere := result.Get("Get", "Company").AsSlice()
		groupedWithoutWhereName := groupedWithoutWhere[0].(map[string]interface{})["name"].(string)
		assert.Equal(t, "Apple Inc. (Google Incorporated, Google Inc., Microsoft Incorporated, "+
			"Apple, Apple Incorporated, Google, Microsoft Inc., Microsoft)",
			groupedWithoutWhereName)

		companyCities = groupedWithoutWhere[0].(map[string]interface{})["inCity"].([]interface{})
		assert.ElementsMatch(t, expectedCities, companyCities)
	})

	t.Run("grouping with sort", func(t *testing.T) {
		query := `
			{
				Get {
					Company(group:{type:merge force:1.0} sort:{path:["name"]}) {
						name
						inCity {
							... on City {
								name
							}
						}
					}
				}
			}
		`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		grouped := result.Get("Get", "Company").AsSlice()
		require.Len(t, grouped, 1)
		groupedName := grouped[0].(map[string]interface{})["name"].(string)
		assert.Equal(t, "Apple (Apple Inc., Apple Incorporated, Google, Google Inc., "+
			"Google Incorporated, Microsoft, Microsoft Inc., Microsoft Incorporated)",
			groupedName)

		groupedCities := grouped[0].(map[string]interface{})["inCity"].([]interface{})
		expectedCities := []map[string]interface{}{
			{"name": "Dusseldorf"},
			{"name": "Amsterdam"},
			{"name": "Berlin"},
		}

		assert.ElementsMatch(t, expectedCities, groupedCities)
	})

	// temporarily removed due to
	// https://github.com/weaviate/weaviate/issues/1302
	// t.Run("grouping mode set to closest", func(t *testing.T) {
	// 	query := `
	// 	{
	// 		Get {
	// 			Company(group: {type: closest, force:0.10}) {
	// 				name
	// 			}
	// 		}
	// 	}
	// 	`
	// 	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	// 	companies := result.Get("Get", "Company").AsSlice()

	// 	assert.Len(t, companies, 3)
	// 	mustContain := []string{"Apple", "Microsoft", "Google"}
	// outer:
	// 	for _, toContain := range mustContain {
	// 		for _, current := range companies {
	// 			if strings.Contains(current.(map[string]interface{})["name"].(string), toContain) {
	// 				continue outer
	// 			}
	// 		}

	// 		t.Errorf("%s not contained in %v", toContain, companies)
	// 	}
	// })

	// ignore as 0.16.0 contextionaries aren't compatible with this test
	// t.Run("grouping mode set to merge", func(t *testing.T) {
	// 	query := `
	// 	{
	// 		Get {
	// 				Company(group: {type: merge, force:0.1}) {
	// 					name
	// 					inCity {
	// 					  ... on City {
	// 						  name
	// 						}
	// 					}
	// 				}
	// 		}
	// 	}
	// 	`
	// 	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	// 	companies := result.Get("Get", "Company").AsSlice()

	// 	assert.Len(t, companies, 3)
	// 	mustContain := [][]string{
	// 		[]string{"Apple", "Apple Inc.", "Apple Incorporated"},
	// 		[]string{"Microsoft", "Microsoft Inc.", "Microsoft Incorporated"},
	// 		[]string{"Google", "Google Inc.", "Google Incorporated"},
	// 	}

	// 	allContained := func(current map[string]interface{}, toContains []string) bool {
	// 		for _, toContain := range toContains {
	// 			if !strings.Contains(current["name"].(string), toContain) {
	// 				return false
	// 			}
	// 		}
	// 		return true
	// 	}

	// outer:
	// 	for _, toContain := range mustContain {
	// 		for _, current := range companies {
	// 			if allContained(current.(map[string]interface{}), toContain) {
	// 				continue outer
	// 			}
	// 		}

	// 		t.Errorf("%s not contained in %v", toContain, companies)
	// 	}
	// })
}
