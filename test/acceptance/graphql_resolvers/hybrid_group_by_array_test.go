//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func hybridSearchGroupByArrayTests(t *testing.T) {
	className := "HybridGroupByArrayTest"

	// Setup schema with array properties
	t.Run("create schema", func(t *testing.T) {
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "title",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "tags",
					DataType: []string{"text[]"}, // Array property
				},
				{
					Name:     "categories",
					DataType: []string{"text[]"}, // Array property
				},
				{
					Name:     "content",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}
		helper.CreateClass(t, class)
	})

	// Add test data
	t.Run("add test data", func(t *testing.T) {
		objects := []*models.Object{
			{
				Class: className,
				ID:    "00000000-0000-0000-0000-000000000001",
				Properties: map[string]interface{}{
					"title":      "Red Sports Car",
					"tags":       []string{"red", "vehicle", "sports"},
					"categories": []string{"automotive", "luxury"},
					"content":    "A beautiful red sports car with excellent performance.",
				},
			},
			{
				Class: className,
				ID:    "00000000-0000-0000-0000-000000000002",
				Properties: map[string]interface{}{
					"title":      "Blue Ocean Boat",
					"tags":       []string{"blue", "vehicle", "water"},
					"categories": []string{"nautical", "recreation"},
					"content":    "A sleek blue boat designed for ocean adventures.",
				},
			},
			{
				Class: className,
				ID:    "00000000-0000-0000-0000-000000000003",
				Properties: map[string]interface{}{
					"title":      "Red Fire Truck",
					"tags":       []string{"red", "vehicle", "emergency"},
					"categories": []string{"automotive", "emergency"},
					"content":    "A red emergency vehicle used by firefighters.",
				},
			},
			{
				Class: className,
				ID:    "00000000-0000-0000-0000-000000000004",
				Properties: map[string]interface{}{
					"title":      "Green Garden Tool",
					"tags":       []string{"green", "tool", "garden"},
					"categories": []string{"tools", "garden"},
					"content":    "A green tool perfect for gardening activities.",
				},
			},
		}

		for _, obj := range objects {
			helper.CreateObject(t, obj)
		}
		helper.AssertEventuallyEqual(t, len(objects), func() interface{} {
			return graphqlhelper.AssertGraphQL(t, helper.RootAuth, `{ Aggregate { `+className+` { meta { count } } } }`).
				Get("Aggregate", className).AsSlice()[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
		})
	})

	// Test hybrid search with groupBy on array property "tags"
	t.Run("hybrid search group by tags array", func(t *testing.T) {
		query := `{
			Get {
				` + className + `(
					hybrid: {
						query: "vehicle"
						alpha: 0.5
					}
					groupBy: {
						path: ["tags"]
						groups: 10
						objectsPerGroup: 5
					}
				) {
					title
					tags
					_additional {
						group {
							id
							groupedBy { value }
							count
							hits {
								title
								tags
								_additional {
									id
									distance
								}
							}
						}
					}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		groups := result.Get("Get", className).AsSlice()

		// Should have multiple groups for different tag values
		require.GreaterOrEqual(t, len(groups), 3, "should have at least 3 groups")

		groupsByValue := make(map[string]interface{})
		for _, group := range groups {
			groupData := group.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
			groupValue := groupData["groupedBy"].(map[string]interface{})["value"].(string)
			groupsByValue[groupValue] = groupData
		}

		// Verify specific groups exist
		expectedGroups := []string{"red", "blue", "vehicle", "sports", "water", "emergency", "green", "tool", "garden"}
		foundGroups := 0
		for _, expectedGroup := range expectedGroups {
			if _, exists := groupsByValue[expectedGroup]; exists {
				foundGroups++
			}
		}
		assert.GreaterOrEqual(t, foundGroups, 5, "should find at least 5 expected groups")

		// Check that "red" group has 2 objects (Red Sports Car and Red Fire Truck)
		if redGroup, exists := groupsByValue["red"]; exists {
			redGroupData := redGroup.(map[string]interface{})
			count := int(redGroupData["count"].(float64))
			assert.Equal(t, 2, count, "red group should have 2 objects")

			hits := redGroupData["hits"].([]interface{})
			assert.Len(t, hits, 2, "red group should have 2 hits")
		}

		// Check that "vehicle" group has 3 objects (all except green garden tool)
		if vehicleGroup, exists := groupsByValue["vehicle"]; exists {
			vehicleGroupData := vehicleGroup.(map[string]interface{})
			count := int(vehicleGroupData["count"].(float64))
			assert.Equal(t, 3, count, "vehicle group should have 3 objects")
		}
	})

	// Test hybrid search with groupBy on different array property "categories"
	t.Run("hybrid search group by categories array", func(t *testing.T) {
		query := `{
			Get {
				` + className + `(
					hybrid: {
						query: "automotive"
						alpha: 0.7
					}
					groupBy: {
						path: ["categories"]
						groups: 10
						objectsPerGroup: 5
					}
				) {
					title
					categories
					_additional {
						group {
							groupedBy { value }
							count
							hits {
								title
								categories
								_additional {
									id
								}
							}
						}
					}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		groups := result.Get("Get", className).AsSlice()

		require.GreaterOrEqual(t, len(groups), 2, "should have at least 2 groups")

		groupsByValue := make(map[string]interface{})
		for _, group := range groups {
			groupData := group.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
			groupValue := groupData["groupedBy"].(map[string]interface{})["value"].(string)
			groupsByValue[groupValue] = groupData
		}

		// Check that "automotive" group has 2 objects (Red Sports Car and Red Fire Truck)
		if automotiveGroup, exists := groupsByValue["automotive"]; exists {
			automotiveGroupData := automotiveGroup.(map[string]interface{})
			count := int(automotiveGroupData["count"].(float64))
			assert.Equal(t, 2, count, "automotive group should have 2 objects")
		}
	})

	// Test hybrid search with groupBy on array property with objects per group limit
	t.Run("hybrid search group by with objects per group limit", func(t *testing.T) {
		query := `{
			Get {
				` + className + `(
					hybrid: {
						query: "red"
						alpha: 0.5
					}
					groupBy: {
						path: ["tags"]
						groups: 10
						objectsPerGroup: 1
					}
				) {
					_additional {
						group {
							groupedBy { value }
							count
							hits {
								title
								_additional { id }
							}
						}
					}
				}
			}
		}`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		groups := result.Get("Get", className).AsSlice()

		// Find the "red" group
		var redGroupData map[string]interface{}
		for _, group := range groups {
			groupData := group.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
			groupValue := groupData["groupedBy"].(map[string]interface{})["value"].(string)
			if groupValue == "red" {
				redGroupData = groupData
				break
			}
		}

		require.NotNil(t, redGroupData, "red group should exist")
		count := int(redGroupData["count"].(float64))
		assert.Equal(t, 1, count, "red group should have only 1 object due to objectsPerGroup limit")

		hits := redGroupData["hits"].([]interface{})
		assert.Len(t, hits, 1, "red group should have only 1 hit due to objectsPerGroup limit")
	})

	// Clean up
	t.Run("cleanup", func(t *testing.T) {
		helper.DeleteClass(t, className)
	})
}
