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

package search

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
)

func selectProps(names ...string) search.SelectProperties {
	props := make(search.SelectProperties, len(names))
	for i, name := range names {
		props[i] = search.SelectProperty{Name: name, IsPrimitive: true}
	}
	return props
}

func TestBuildResponseFlatObjects(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"year":  int64(2021),
			"_additional": map[string]interface{}{
				"distance": float32(0.12),
			},
		},
	}
	params := dto.GetParams{
		Properties:           selectProps("title", "year"),
		AdditionalProperties: additional.Properties{ID: true, Distance: true},
	}

	reply, err := buildResponse(res, params, 8*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, int64(8), reply.TookMs)
	require.Len(t, reply.Results, 1)
	obj := reply.Results[0].(map[string]interface{})
	assert.Equal(t, "Dune", obj["title"])
	assert.Equal(t, int64(2021), obj["year"])

	metadata, ok := obj[metadataKey].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", metadata["id"])
	assert.Equal(t, float32(0.12), metadata["distance"])
}

func TestBuildResponseOnlySelectedProperties(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"id":     strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title":  "Dune",
			"year":   int64(2021),
			"secret": "not selected",
		},
	}
	params := dto.GetParams{
		Properties:           selectProps("title"),
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0].(map[string]interface{})
	assert.Contains(t, obj, "title")
	assert.NotContains(t, obj, "year")
	assert.NotContains(t, obj, "secret")
	// the raw id lives in metadata only, never at the object root
	assert.NotContains(t, obj, "id")
}

func TestBuildResponseSkipsMissingProperties(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"title": "Dune",
		},
	}
	params := dto.GetParams{
		Properties: selectProps("title", "year"),
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0].(map[string]interface{})
	assert.Equal(t, "Dune", obj["title"])
	assert.NotContains(t, obj, "year")
	assert.NotContains(t, obj, metadataKey)
}

func TestBuildResponseMetadataOnlyWhenRequested(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]interface{}{
				"distance":           float32(0.12),
				"certainty":          float64(0.94),
				"score":              float32(0.5),
				"explainScore":       "because",
				"creationTimeUnix":   int64(1700000000000),
				"lastUpdateTimeUnix": int64(1700000000001),
			},
		},
	}
	params := dto.GetParams{
		Properties: selectProps("title"),
		AdditionalProperties: additional.Properties{
			Certainty:        true,
			ExplainScore:     true,
			CreationTimeUnix: true,
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	metadata := reply.Results[0].(map[string]interface{})[metadataKey].(map[string]interface{})
	assert.Equal(t, float64(0.94), metadata["certainty"])
	assert.Equal(t, "because", metadata["explain_score"])
	assert.Equal(t, int64(1700000000000), metadata["creation_time"])
	assert.NotContains(t, metadata, "id")
	assert.NotContains(t, metadata, "distance")
	assert.NotContains(t, metadata, "score")
	assert.NotContains(t, metadata, "last_update_time")
}

func TestBuildResponseCrossReferences(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"title": "Dune",
			"hasAuthor": []interface{}{
				search.LocalRef{
					Class: "Author",
					Fields: map[string]interface{}{
						"name": "Frank Herbert",
						"age":  int64(65),
					},
				},
			},
		},
	}
	params := dto.GetParams{
		Properties: search.SelectProperties{
			{Name: "title", IsPrimitive: true},
			{Name: "hasAuthor", Refs: []search.SelectClass{{
				ClassName:     "Author",
				RefProperties: selectProps("name"),
			}}},
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0].(map[string]interface{})
	refs, ok := obj["hasAuthor"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, refs, 1)
	assert.Equal(t, "Frank Herbert", refs[0]["name"])
	// only the selected ref property comes back
	assert.NotContains(t, refs[0], "age")
}

func TestBuildResponsePrunesNestedObjects(t *testing.T) {
	// storage returns ALL nested fields of an object property — including
	// ones the selection excluded (e.g. a nested blob); pruning is the
	// reply builder's job
	res := []interface{}{
		map[string]interface{}{
			"title": "Dune",
			"cover": map[string]interface{}{
				"caption": "front cover",
				"image":   "aGVsbG8=", // nested blob, not selected
				"size": map[string]interface{}{
					"width":  int64(800),
					"height": int64(600), // not selected either
				},
			},
		},
	}
	params := dto.GetParams{
		Properties: search.SelectProperties{
			{Name: "title", IsPrimitive: true},
			{Name: "cover", IsObject: true, Props: []search.SelectProperty{
				{Name: "caption", IsPrimitive: true},
				{Name: "size", IsObject: true, Props: []search.SelectProperty{
					{Name: "width", IsPrimitive: true},
				}},
			}},
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	cover, ok := reply.Results[0].(map[string]interface{})["cover"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "front cover", cover["caption"])
	assert.NotContains(t, cover, "image")
	size, ok := cover["size"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, int64(800), size["width"])
	assert.NotContains(t, size, "height")
}

func TestBuildResponsePrunesObjectArrays(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{"name": "opening", "raw": "ZGF0YQ=="},
				map[string]interface{}{"name": "finale", "raw": "ZGF0YQ=="},
			},
		},
	}
	params := dto.GetParams{
		Properties: search.SelectProperties{
			{Name: "scenes", IsObject: true, Props: []search.SelectProperty{
				{Name: "name", IsPrimitive: true},
			}},
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	scenes, ok := reply.Results[0].(map[string]interface{})["scenes"].([]interface{})
	require.True(t, ok)
	require.Len(t, scenes, 2)
	for _, scene := range scenes {
		fields := scene.(map[string]interface{})
		assert.Contains(t, fields, "name")
		assert.NotContains(t, fields, "raw")
	}
}

func TestBuildResponsePrunesObjectsInsideRefs(t *testing.T) {
	res := []interface{}{
		map[string]interface{}{
			"hasAuthor": []interface{}{
				search.LocalRef{
					Class: "Author",
					Fields: map[string]interface{}{
						"name": "Frank Herbert",
						"address": map[string]interface{}{
							"city":   "Tacoma",
							"secret": "not selected",
						},
					},
				},
			},
		},
	}
	params := dto.GetParams{
		Properties: search.SelectProperties{
			{Name: "hasAuthor", Refs: []search.SelectClass{{
				ClassName: "Author",
				RefProperties: []search.SelectProperty{
					{Name: "name", IsPrimitive: true},
					{Name: "address", IsObject: true, Props: []search.SelectProperty{
						{Name: "city", IsPrimitive: true},
					}},
				},
			}}},
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	refs, ok := reply.Results[0].(map[string]interface{})["hasAuthor"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, refs, 1)
	assert.Equal(t, "Frank Herbert", refs[0]["name"])
	address, ok := refs[0]["address"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Tacoma", address["city"])
	assert.NotContains(t, address, "secret")
}

func TestBuildResponseErrorsOnMissingRequestedID(t *testing.T) {
	params := dto.GetParams{
		Properties:           selectProps("title"),
		AdditionalProperties: additional.Properties{ID: true},
	}

	t.Run("id missing", func(t *testing.T) {
		_, err := buildResponse([]interface{}{
			map[string]interface{}{"title": "Dune"},
		}, params, time.Millisecond)
		assert.Error(t, err)
	})

	t.Run("id mistyped", func(t *testing.T) {
		_, err := buildResponse([]interface{}{
			map[string]interface{}{"title": "Dune", "id": "not-a-strfmt-uuid"},
		}, params, time.Millisecond)
		assert.Error(t, err)
	})
}

func TestBuildResponseEmptyResults(t *testing.T) {
	reply, err := buildResponse(nil, dto.GetParams{}, time.Millisecond)
	require.NoError(t, err)
	assert.NotNil(t, reply.Results)
	assert.Empty(t, reply.Results)
}

func TestBuildResponseRejectsUnexpectedShape(t *testing.T) {
	_, err := buildResponse([]interface{}{"not a map"}, dto.GetParams{}, time.Millisecond)
	assert.Error(t, err)
}
