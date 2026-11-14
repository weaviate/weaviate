//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestExtractGroupByValues(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expected    []string
		expectError bool
	}{
		{
			name:     "string value",
			input:    "test-value",
			expected: []string{"test-value"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{""},
		},
		{
			name:     "nil value",
			input:    nil,
			expected: []string{},
		},
		{
			name:     "string array",
			input:    []string{"tag1", "tag2", "tag3"},
			expected: []string{"tag1", "tag2", "tag3"},
		},
		{
			name:     "empty string array",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "interface array with strings",
			input:    []interface{}{"category1", "category2"},
			expected: []string{"category1", "category2"},
		},
		{
			name:     "empty interface array",
			input:    []interface{}{},
			expected: []string{},
		},
		{
			name:     "integer value",
			input:    42,
			expected: []string{"42"},
		},
		{
			name:     "int64 value",
			input:    int64(-7),
			expected: []string{"-7"},
		},
		{
			name:     "boolean true",
			input:    true,
			expected: []string{"true"},
		},
		{
			name:     "boolean false",
			input:    false,
			expected: []string{"false"},
		},
		{
			name:     "whole float64 keeps no decimal point",
			input:    float64(2),
			expected: []string{"2"},
		},
		{
			name:     "fractional float64",
			input:    0.5,
			expected: []string{"0.5"},
		},
		{
			name:     "large whole float64 is not rounded or put in exponent form",
			input:    float64(12345678),
			expected: []string{"12345678"},
		},
		{
			name:     "json.Number",
			input:    json.Number("1.5"),
			expected: []string{"1.5"},
		},
		{
			name:     "unsupported type (skipped)",
			input:    map[string]interface{}{"a": 1},
			expected: []string{},
		},
		{
			name:        "interface array with non-string",
			input:       []interface{}{"valid", 123, "also-valid"},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractGroupByValues(tt.input)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExplorerGroupSearchResults(t *testing.T) {
	explorer := &Explorer{}
	ctx := context.Background()

	t.Run("single string property grouping", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"category": "A"}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"category": "B"}, 0.2),
			createSearchResult("obj3", map[string]interface{}{"category": "A"}, 0.3),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "category",
			Groups:          10,
			ObjectsPerGroup: 5,
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have 2 groups: A and B
		assert.Len(t, grouped, 2)

		// Check first group (A)
		group1 := grouped[0].AdditionalProperties["group"].(*additional.Group)
		assert.Equal(t, "A", group1.GroupedBy.Value)
		assert.Equal(t, 2, group1.Count) // obj1 and obj3
		assert.Len(t, group1.Hits, 2)

		// Check second group (B)
		group2 := grouped[1].AdditionalProperties["group"].(*additional.Group)
		assert.Equal(t, "B", group2.GroupedBy.Value)
		assert.Equal(t, 1, group2.Count) // obj2
		assert.Len(t, group2.Hits, 1)
	})

	t.Run("array property grouping", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"tags": []string{"red", "large"}}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"tags": []string{"blue", "small"}}, 0.2),
			createSearchResult("obj3", map[string]interface{}{"tags": []string{"red", "medium"}}, 0.3),
			createSearchResult("obj4", map[string]interface{}{"tags": []string{"green"}}, 0.4),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "tags",
			Groups:          100, // Very high limit to avoid interference
			ObjectsPerGroup: 100, // Very high limit to avoid interference
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Debug: print what properties each test result has
		for i, result := range results {
			prop := result.Object().Properties.(map[string]interface{})
			rawValue := prop["tags"]
			values, _ := extractGroupByValues(rawValue)
			t.Logf("Object %d: %v -> %v", i, rawValue, values)
		}

		// Debug: print what groups we actually got
		t.Logf("Got %d groups:", len(grouped))
		for i, result := range grouped {
			group := result.AdditionalProperties["group"].(*additional.Group)
			t.Logf("  Group %d: %s (count: %d)", i, group.GroupedBy.Value, group.Count)
		}

		// Let me manually call the same function to understand what's happening
		tempGroupsOrdered := []string{}
		tempGroups := map[string][]search.Result{}
		for _, result := range results {
			prop := result.Object().Properties.(map[string]interface{})
			rawValue := prop["tags"]
			values, _ := extractGroupByValues(rawValue)
			t.Logf("Processing result with values: %v", values)
			for _, val := range values {
				current, groupExists := tempGroups[val]
				tempGroups[val] = append(current, result)
				if !groupExists {
					tempGroupsOrdered = append(tempGroupsOrdered, val)
					t.Logf("  Added new group: %s (total groups: %d)", val, len(tempGroupsOrdered))
				} else {
					t.Logf("  Added to existing group: %s", val)
				}
			}
		}
		t.Logf("Final groupsOrdered: %v", tempGroupsOrdered)
		t.Logf("Final groups: %v", func() map[string]int {
			result := make(map[string]int)
			for k, v := range tempGroups {
				result[k] = len(v)
			}
			return result
		}())

		// Should have 6 unique groups: red, large, blue, small, medium, green
		// Each unique tag value creates its own group
		assert.Len(t, grouped, 6)

		// Collect all group values to verify we have the expected ones
		groupValues := make(map[string]int)
		for _, result := range grouped {
			group := result.AdditionalProperties["group"].(*additional.Group)
			groupValues[group.GroupedBy.Value] = group.Count
		}

		// Verify we have all expected groups
		expectedGroups := []string{"red", "large", "blue", "small", "medium", "green"}
		for _, expectedGroup := range expectedGroups {
			assert.Contains(t, groupValues, expectedGroup, fmt.Sprintf("should contain group '%s'", expectedGroup))
		}

		// The "red" group should have 2 objects (obj1 and obj3)
		assert.Equal(t, 2, groupValues["red"], "red group should have 2 objects")
		// All other groups should have 1 object each
		assert.Equal(t, 1, groupValues["large"], "large group should have 1 object")
		assert.Equal(t, 1, groupValues["blue"], "blue group should have 1 object")
		assert.Equal(t, 1, groupValues["small"], "small group should have 1 object")
		assert.Equal(t, 1, groupValues["medium"], "medium group should have 1 object")
		assert.Equal(t, 1, groupValues["green"], "green group should have 1 object")
	})

	t.Run("interface array property grouping", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"categories": []interface{}{"tech", "mobile"}}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"categories": []interface{}{"tech", "web"}}, 0.2),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "categories",
			Groups:          10,
			ObjectsPerGroup: 5,
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have 3 groups: tech, mobile, web
		assert.Len(t, grouped, 3)

		// Collect all group values to verify we have the expected ones
		groupValues := make(map[string]int)
		for _, result := range grouped {
			group := result.AdditionalProperties["group"].(*additional.Group)
			groupValues[group.GroupedBy.Value] = group.Count
		}

		// Verify we have all expected groups
		expectedGroups := []string{"tech", "mobile", "web"}
		for _, expectedGroup := range expectedGroups {
			assert.Contains(t, groupValues, expectedGroup, fmt.Sprintf("should contain group '%s'", expectedGroup))
		}

		// The "tech" group should have 2 objects (obj1 and obj2)
		assert.Equal(t, 2, groupValues["tech"], "tech group should have 2 objects")
		// Other groups should have 1 object each
		assert.Equal(t, 1, groupValues["mobile"], "mobile group should have 1 object")
		assert.Equal(t, 1, groupValues["web"], "web group should have 1 object")
	})

	t.Run("mixed string and array properties", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"type": "single"}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"type": []string{"multi1", "multi2"}}, 0.2),
			createSearchResult("obj3", map[string]interface{}{"type": "single"}, 0.3),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "type",
			Groups:          10,
			ObjectsPerGroup: 5,
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have 3 groups: single, multi1, multi2
		assert.Len(t, grouped, 3)

		// Find the "single" group
		var singleGroup *additional.Group
		for _, result := range grouped {
			group := result.AdditionalProperties["group"].(*additional.Group)
			if group.GroupedBy.Value == "single" {
				singleGroup = group
				break
			}
		}
		require.NotNil(t, singleGroup, "single group should exist")
		assert.Equal(t, 2, singleGroup.Count) // obj1 and obj3
		assert.Len(t, singleGroup.Hits, 2)
	})

	t.Run("objects per group limit", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"tags": []string{"popular"}}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"tags": []string{"popular"}}, 0.2),
			createSearchResult("obj3", map[string]interface{}{"tags": []string{"popular"}}, 0.3),
			createSearchResult("obj4", map[string]interface{}{"tags": []string{"rare"}}, 0.4),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "tags",
			Groups:          10,
			ObjectsPerGroup: 2, // Limit to 2 objects per group
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have 2 groups: popular and rare
		assert.Len(t, grouped, 2)

		// Find the "popular" group
		var popularGroup *additional.Group
		for _, result := range grouped {
			group := result.AdditionalProperties["group"].(*additional.Group)
			if group.GroupedBy.Value == "popular" {
				popularGroup = group
				break
			}
		}
		require.NotNil(t, popularGroup, "popular group should exist")
		assert.Equal(t, 2, popularGroup.Count) // Limited to 2 despite 3 objects having "popular"
		assert.Len(t, popularGroup.Hits, 2)
	})

	t.Run("groups limit", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"category": "A"}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"category": "B"}, 0.2),
			createSearchResult("obj3", map[string]interface{}{"category": "C"}, 0.3),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "category",
			Groups:          2, // Limit to 2 groups
			ObjectsPerGroup: 5,
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have only 2 groups due to limit
		assert.Len(t, grouped, 2)
	})

	t.Run("group by bool, int and number properties", func(t *testing.T) {
		groupBy := &searchparams.GroupBy{
			Property:        "category",
			Groups:          10,
			ObjectsPerGroup: 5,
		}

		tests := []struct {
			name     string
			values   []interface{}
			expected []string
		}{
			{
				name:     "bool",
				values:   []interface{}{true, false, true},
				expected: []string{"true", "false"},
			},
			{
				name:     "int",
				values:   []interface{}{float64(1), float64(2), float64(1)},
				expected: []string{"1", "2"},
			},
			{
				name:     "number",
				values:   []interface{}{0.5, 1.5, 0.5},
				expected: []string{"0.5", "1.5"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				results := make(search.Results, len(tt.values))
				for i, v := range tt.values {
					results[i] = createSearchResult(fmt.Sprintf("obj%d", i+1),
						map[string]interface{}{"category": v}, float32(i)/10)
				}

				grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
				require.NoError(t, err)

				require.Len(t, grouped, len(tt.expected))
				for i, want := range tt.expected {
					group := grouped[i].AdditionalProperties["group"].(*additional.Group)
					assert.Equal(t, want, group.GroupedBy.Value)
				}
				// the first value occurs twice and must land in one group
				first := grouped[0].AdditionalProperties["group"].(*additional.Group)
				assert.Equal(t, 2, first.Count)
			})
		}
	})

	t.Run("skip unsupported property types", func(t *testing.T) {
		results := search.Results{
			createSearchResult("obj1", map[string]interface{}{"category": "A"}, 0.1),
			createSearchResult("obj2", map[string]interface{}{"category": map[string]interface{}{"x": 1}}, 0.2), // object, should be skipped
			createSearchResult("obj3", map[string]interface{}{"category": struct{}{}}, 0.3),                     // unknown type, should be skipped
			createSearchResult("obj4", map[string]interface{}{"category": "B"}, 0.4),
		}

		groupBy := &searchparams.GroupBy{
			Property:        "category",
			Groups:          10,
			ObjectsPerGroup: 5,
		}

		grouped, err := explorer.groupSearchResults(ctx, results, groupBy)
		require.NoError(t, err)

		// Should have only 2 groups: A and B (unsupported types skipped)
		assert.Len(t, grouped, 2)
	})
}

// Helper function to create search results for testing
func createSearchResult(id string, properties map[string]interface{}, distance float32) search.Result {
	// Create a valid UUID by padding the id
	uuidStr := id
	for len(uuidStr) < 32 {
		uuidStr += "0"
	}
	// Format as UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if len(uuidStr) >= 32 {
		uuidStr = uuidStr[:8] + "-" + uuidStr[8:12] + "-" + uuidStr[12:16] + "-" + uuidStr[16:20] + "-" + uuidStr[20:32]
	}
	uuid := strfmt.UUID(uuidStr)

	return search.Result{
		ID:                   uuid,
		Dist:                 distance,
		Schema:               properties,
		AdditionalProperties: models.AdditionalProperties{},
	}
}
