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

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func boolPtr(v bool) *bool { return &v }

func TestHasNestedFilterableIndex(t *testing.T) {
	t.Run("child filterable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city"},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("child explicitly filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", IndexFilterable: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("all children not filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", IndexFilterable: boolPtr(false)},
			},
		}
		assert.False(t, HasNestedFilterableIndex(prop))
	})

	t.Run("deeply nested child is filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "tires",
					IndexFilterable: boolPtr(false),
					DataType:        schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", IndexFilterable: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("no nested properties", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
		}
		assert.False(t, HasNestedFilterableIndex(prop))
	})
}

func TestHasNestedSearchableIndex(t *testing.T) {
	t.Run("text child searchable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString()},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})

	t.Run("text child explicitly searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexSearchable: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})

	t.Run("text child not searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexSearchable: boolPtr(false)},
			},
		}
		assert.False(t, HasNestedSearchableIndex(prop))
	})

	t.Run("int child is never searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
			},
		}
		assert.False(t, HasNestedSearchableIndex(prop))
	})

	t.Run("deeply nested text child is searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "bio", DataType: schema.DataTypeText.PropString()},
					},
				},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})
}

func TestHasNestedRangeableIndex(t *testing.T) {
	t.Run("int child not rangeable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
			},
		}
		assert.False(t, HasNestedRangeableIndex(prop))
	})

	t.Run("int child explicitly rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedRangeableIndex(prop))
	})

	t.Run("text child is never rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.False(t, HasNestedRangeableIndex(prop))
	})

	t.Run("deeply nested number child is rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "stats",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "score", DataType: schema.DataTypeNumber.PropString(), IndexRangeFilters: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasNestedRangeableIndex(prop))
	})
}

func TestHasAnyNestedInvertedIndex(t *testing.T) {
	t.Run("child filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("child searchable only", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("child rangeable only", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("all children not indexed", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)},
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(false)},
			},
		}
		assert.False(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("parent not indexed but deeply nested child is filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "tires",
					DataType:        schema.DataTypeObjectArray.PropString(),
					IndexFilterable: boolPtr(false),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("parent not indexed but deeply nested child is rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "stats",
					DataType:        schema.DataTypeObject.PropString(),
					IndexFilterable: boolPtr(false),
					NestedProperties: []*models.NestedProperty{
						{Name: "price", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})
}

func TestCollectNestedIndexConfig(t *testing.T) {
	props := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString()},                                                                    // text: filterable+searchable
		{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},                                  // int: filterable+rangeable
		{Name: "active", DataType: schema.DataTypeBoolean.PropString()},                                                               // bool: filterable only
		{Name: "score", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false)},                                // number: not filterable, not rangeable
		{Name: "label", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)}, // not indexed at all
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "name", DataType: schema.DataTypeText.PropString()},
				{Name: "age", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		},
	}

	configs := collectNestedIndexConfig("", props)

	t.Run("text is filterable and searchable", func(t *testing.T) {
		cfg := configs["city"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
	})

	t.Run("int with range is filterable and rangeable", func(t *testing.T) {
		cfg := configs["count"]
		assert.True(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.True(t, cfg.rangeable)
	})

	t.Run("bool is filterable only", func(t *testing.T) {
		cfg := configs["active"]
		assert.True(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
	})

	t.Run("number with filterable disabled", func(t *testing.T) {
		cfg := configs["score"]
		assert.False(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
		assert.False(t, cfg.hasAny())
	})

	t.Run("fully disabled property", func(t *testing.T) {
		cfg := configs["label"]
		assert.False(t, cfg.hasAny())
	})

	t.Run("nested object paths not in config", func(t *testing.T) {
		_, exists := configs["owner"]
		assert.False(t, exists)
	})

	t.Run("nested leaf paths use dot notation", func(t *testing.T) {
		cfg := configs["owner.name"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.searchable)

		cfg = configs["owner.age"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.rangeable)
	})

	t.Run("total leaf paths", func(t *testing.T) {
		// city, count, active, score, label, owner.name, owner.age
		assert.Len(t, configs, 7)
	})
}

func TestAnalyzeNestedProp(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")

	t.Run("doc123 produces correct values and flags", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nestedObject",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
						{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
						{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
					},
				},
				{
					Name:     "addresses",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					},
				},
				{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		}

		value := map[string]any{
			"name": "subdoc_123",
			"owner": map[string]any{
				"firstname": "Marsha",
				"lastname":  "Mallow",
				"nicknames": []any{"Marshmallow", "M&M"},
			},
			"addresses": []any{
				map[string]any{"city": "Berlin"},
			},
			"tags": []any{"german", "premium"},
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, "nestedObject", result.Name)
		assert.True(t, result.HasFilterableIndex)
		assert.True(t, result.HasSearchableIndex)
		assert.False(t, result.HasRangeableIndex)

		// Verify values are analyzed (bytes, not raw strings)
		assert.NotEmpty(t, result.Values)
		for _, v := range result.Values {
			assert.NotEmpty(t, v.Data, "value at %s should have analyzed data", v.Path)
			assert.NotEmpty(t, v.Positions, "value at %s should have positions", v.Path)
			assert.True(t, v.HasFilterableIndex, "all text values should be filterable")
		}

		// Verify metadata is propagated
		assert.NotEmpty(t, result.Idx)
		assert.NotEmpty(t, result.Exists)
	})

	t.Run("non-filterable paths are excluded from values", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "indexed", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(true)},
				{Name: "skipped", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)},
			},
		}

		value := map[string]any{
			"indexed": "hello",
			"skipped": "world",
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Only "indexed" should produce values
		for _, v := range result.Values {
			assert.Equal(t, "indexed", v.Path, "skipped path should not appear in values")
		}

		// But metadata still includes both (structural)
		existsPaths := map[string]bool{}
		for _, e := range result.Exists {
			existsPaths[e.Path] = true
		}
		assert.True(t, existsPaths["indexed"])
		assert.True(t, existsPaths["skipped"])
	})

	t.Run("aggregate flags reflect index types", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(false)},
				{Name: "price", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
			},
		}

		value := map[string]any{
			"title": "hello",
			"price": float64(9.99),
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.False(t, result.HasFilterableIndex, "no filterable paths")
		assert.True(t, result.HasSearchableIndex, "title is searchable by default")
		assert.True(t, result.HasRangeableIndex, "price is rangeable")
	})

	t.Run("per-value flags match path config", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}

		value := map[string]any{
			"city":  "Berlin",
			"count": float64(42),
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)

		for _, v := range result.Values {
			switch v.Path {
			case "city":
				assert.True(t, v.HasFilterableIndex)
				assert.True(t, v.HasSearchableIndex)
				assert.False(t, v.HasRangeableIndex)
			case "count":
				assert.True(t, v.HasFilterableIndex)
				assert.False(t, v.HasSearchableIndex)
				assert.True(t, v.HasRangeableIndex)
			default:
				t.Errorf("unexpected path %q", v.Path)
			}
		}
	})

	t.Run("nil value returns nil", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString()},
			},
		}

		result, err := analyzer.analyzeNestedProp(prop, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// Full design document tests using the shared schema from the design summary.
	// Each test runs with both full shared schema and minimal per-document schema
	// to verify that appending new sub-properties to the schema doesn't change
	// analysis results.

	fullSchema := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		},
		{
			Name:     "addresses",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString()},
			},
		},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{
					Name:     "tires",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString()},
						{Name: "radiuses", DataType: schema.DataTypeIntArray.PropString()},
					},
				},
				{
					Name:     "accessories",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					},
				},
				{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		},
	}

	// doc123/124 schema: no accessories
	doc123Schema := []*models.NestedProperty{
		fullSchema[0], fullSchema[1], fullSchema[2], fullSchema[3],
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				fullSchema[4].NestedProperties[0], // make
				fullSchema[4].NestedProperties[1], // tires
				fullSchema[4].NestedProperties[3], // colors
			},
		},
	}

	// doc125 schema: no nicknames in owner, has accessories
	doc125Schema := []*models.NestedProperty{
		fullSchema[0],
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				fullSchema[1].NestedProperties[0], // firstname
				fullSchema[1].NestedProperties[1], // lastname
			},
		},
		fullSchema[2], fullSchema[3], fullSchema[4],
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc123Schema},
	} {
		t.Run("doc123/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc123(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc123Schema},
	} {
		t.Run("doc124/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc124(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc125Schema},
	} {
		t.Run("doc125/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc125(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", fullSchema},
	} {
		t.Run("doc999/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc999(t, analyzer, prop)
		})
	}
}

func assertAnalyzeDoc123(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_123",
		"owner": map[string]any{
			"firstname": "Marsha", "lastname": "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
		"addresses": []any{
			map[string]any{"city": "Berlin", "postcode": "10115", "numbers": []any{float64(123), float64(1123)}},
		},
		"tags": []any{"german", "premium"},
		"cars": []any{
			map[string]any{
				"make":   "BMW",
				"tires":  []any{map[string]any{"width": float64(225), "radiuses": []any{float64(18), float64(19)}}},
				"colors": []any{"black", "orange"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 2, valuePaths["owner.nicknames"])
	assert.Equal(t, 2, valuePaths["addresses.numbers"])
	assert.Equal(t, 2, valuePaths["tags"])
	assert.Equal(t, 2, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 2, valuePaths["cars.colors"])
	assert.Equal(t, 1, valuePaths["cars.tires.width"])
	assert.Equal(t, 1, valuePaths["cars.make"])
	assert.Contains(t, valuePaths, "name")
	assert.Contains(t, valuePaths, "owner.firstname")
	assert.Contains(t, valuePaths, "owner.lastname")
	assert.Contains(t, valuePaths, "addresses.city")
	assert.Contains(t, valuePaths, "addresses.postcode")

	assert.Len(t, result.Idx, 14)
	assert.Len(t, result.Exists, 17)
}

func assertAnalyzeDoc124(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_124",
		"owner": map[string]any{
			"firstname": "Justin", "lastname": "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
			map[string]any{"city": "London", "postcode": "SW1"},
		},
		"tags": []any{"german", "japanese", "sedan"},
		"cars": []any{
			map[string]any{
				"make": "Audi",
				"tires": []any{
					map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
					map[string]any{"width": float64(225)},
				},
			},
			map[string]any{
				"make":   "Kia",
				"tires":  []any{map[string]any{"width": float64(195), "radiuses": []any{}}},
				"colors": []any{"white"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 1, valuePaths["owner.nicknames"])
	assert.Equal(t, 1, valuePaths["addresses.numbers"])
	assert.Equal(t, 2, valuePaths["addresses.city"])
	assert.Equal(t, 2, valuePaths["addresses.postcode"])
	assert.Equal(t, 3, valuePaths["tags"])
	assert.Equal(t, 2, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 3, valuePaths["cars.tires.width"])
	assert.Equal(t, 2, valuePaths["cars.make"])
	assert.Equal(t, 1, valuePaths["cars.colors"])

	assert.Len(t, result.Idx, 16)
	assert.Len(t, result.Exists, 23)
}

func assertAnalyzeDoc125(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_125",
		"owner": map[string]any{
			"firstname": "Anna", "lastname": "Wanna",
		},
		"addresses": []any{
			map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
		},
		"tags": []any{"electric"},
		"cars": []any{
			map[string]any{
				"make":        "Tesla",
				"tires":       []any{map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}}},
				"accessories": []any{map[string]any{"type": "charger"}, map[string]any{"type": "mats"}},
				"colors":      []any{"yellow"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 0, valuePaths["owner.nicknames"])
	assert.Equal(t, 1, valuePaths["addresses.numbers"])
	assert.Equal(t, 1, valuePaths["tags"])
	assert.Equal(t, 3, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 1, valuePaths["cars.tires.width"])
	assert.Equal(t, 2, valuePaths["cars.accessories.type"])
	assert.Equal(t, 1, valuePaths["cars.colors"])
	assert.Equal(t, 1, valuePaths["cars.make"])

	assert.Len(t, result.Idx, 12)
	assert.Len(t, result.Exists, 19)
}

func assertAnalyzeDoc999(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := []any{
		map[string]any{
			"name": "subdoc_124",
			"owner": map[string]any{
				"firstname": "Justin", "lastname": "Time",
				"nicknames": []any{"watch"},
			},
			"addresses": []any{
				map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
				map[string]any{"city": "London", "postcode": "SW1"},
			},
			"tags": []any{"german", "japanese", "sedan"},
			"cars": []any{
				map[string]any{
					"make": "Audi",
					"tires": []any{
						map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
						map[string]any{"width": float64(225)},
					},
				},
				map[string]any{
					"make":   "Kia",
					"tires":  []any{map[string]any{"width": float64(195), "radiuses": []any{}}},
					"colors": []any{"white"},
				},
			},
		},
		map[string]any{
			"name": "subdoc_125",
			"owner": map[string]any{
				"firstname": "Anna", "lastname": "Wanna",
			},
			"addresses": []any{
				map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
			},
			"tags": []any{"electric"},
			"cars": []any{
				map[string]any{
					"make":        "Tesla",
					"tires":       []any{map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}}},
					"accessories": []any{map[string]any{"type": "charger"}, map[string]any{"type": "mats"}},
					"colors":      []any{"yellow"},
				},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	// "subdoc_124"/"subdoc_125" tokenize to 2 tokens each
	assert.Equal(t, 4, valuePaths["name"])
	assert.Equal(t, 2, valuePaths["owner.firstname"])
	assert.Equal(t, 2, valuePaths["owner.lastname"])
	assert.Equal(t, 1, valuePaths["owner.nicknames"])
	assert.Equal(t, 3, valuePaths["addresses.city"])
	assert.Equal(t, 3, valuePaths["addresses.postcode"])
	assert.Equal(t, 2, valuePaths["addresses.numbers"])
	assert.Equal(t, 4, valuePaths["tags"])
	assert.Equal(t, 3, valuePaths["cars.make"])
	assert.Equal(t, 4, valuePaths["cars.tires.width"])
	assert.Equal(t, 5, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 2, valuePaths["cars.colors"])
	assert.Equal(t, 2, valuePaths["cars.accessories.type"])

	assert.Len(t, result.Idx, 28)
	assert.Len(t, result.Exists, 41)
}

func collectValuePaths(result *NestedProperty) map[string]int {
	paths := map[string]int{}
	for _, v := range result.Values {
		paths[v.Path]++
	}
	return paths
}
