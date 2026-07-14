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

func TestBuildResponseEnvelope(t *testing.T) {
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"year":  int64(2021),
			"_additional": map[string]any{
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
	obj := reply.Results[0]
	require.NotNil(t, obj.ID)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", obj.ID.String())
	assert.Equal(t, "Dune", obj.Properties["title"])
	assert.Equal(t, int64(2021), obj.Properties["year"])

	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Distance)
	assert.Equal(t, float32(0.12), *obj.Metadata.Distance)
	assert.Nil(t, obj.References)
}

func TestBuildResponseOnlySelectedProperties(t *testing.T) {
	res := []any{
		map[string]any{
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
	obj := reply.Results[0]
	assert.Contains(t, obj.Properties, "title")
	assert.NotContains(t, obj.Properties, "year")
	assert.NotContains(t, obj.Properties, "secret")
	// the id lives on the envelope only, never inside properties
	assert.NotContains(t, obj.Properties, "id")
	require.NotNil(t, obj.ID)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", obj.ID.String())
}

func TestBuildResponseSkipsMissingProperties(t *testing.T) {
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
		},
	}
	params := dto.GetParams{
		Properties:           selectProps("title", "year"),
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0]
	assert.Equal(t, "Dune", obj.Properties["title"])
	assert.NotContains(t, obj.Properties, "year")
	assert.Nil(t, obj.Metadata)
}

// TestBuildResponseUserPropertyNamedMetadata: with the envelope, a user
// property named "metadata" is ordinary data under properties — it can never
// collide with the envelope's metadata field.
func TestBuildResponseUserPropertyNamedMetadata(t *testing.T) {
	res := []any{
		map[string]any{
			"id":       strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"metadata": "user data",
			"_additional": map[string]any{
				"distance": float32(0.12),
			},
		},
	}
	params := dto.GetParams{
		Properties:           selectProps("metadata"),
		AdditionalProperties: additional.Properties{ID: true, Distance: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0]
	assert.Equal(t, "user data", obj.Properties["metadata"])
	require.NotNil(t, obj.Metadata)
	require.NotNil(t, obj.Metadata.Distance)
	assert.Equal(t, float32(0.12), *obj.Metadata.Distance)
}

func TestBuildResponseMetadataOnlyWhenRequested(t *testing.T) {
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]any{
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
			ID:               true,
			Certainty:        true,
			ExplainScore:     true,
			CreationTimeUnix: true,
		},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	metadata := reply.Results[0].Metadata
	require.NotNil(t, metadata)
	require.NotNil(t, metadata.Certainty)
	assert.Equal(t, float64(0.94), *metadata.Certainty)
	require.NotNil(t, metadata.ExplainScore)
	assert.Equal(t, "because", *metadata.ExplainScore)
	require.NotNil(t, metadata.CreationTime)
	assert.Equal(t, int64(1700000000000), *metadata.CreationTime)
	assert.Nil(t, metadata.Distance)
	assert.Nil(t, metadata.Score)
	assert.Nil(t, metadata.LastUpdateTime)
}

// TestBuildResponseMetadataOmittedWhenIDOnly: the id is carried on the
// envelope, so an id-only request produces no metadata block at all.
func TestBuildResponseMetadataOmittedWhenIDOnly(t *testing.T) {
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"_additional": map[string]any{
				"distance": float32(0.12), // present but not requested
			},
		},
	}
	params := dto.GetParams{
		Properties:           selectProps("title"),
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0]
	require.NotNil(t, obj.ID)
	assert.Equal(t, "73f2eb5f-5abf-447a-81ca-74b1dd168247", obj.ID.String())
	assert.Nil(t, obj.Metadata)
}

func TestBuildResponseCrossReferences(t *testing.T) {
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"hasAuthor": []any{
				search.LocalRef{
					Class: "Author",
					Fields: map[string]any{
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
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	obj := reply.Results[0]
	// reference selections live under references, not properties
	assert.NotContains(t, obj.Properties, "hasAuthor")
	require.Contains(t, obj.References, "hasAuthor")
	refs := obj.References["hasAuthor"]
	require.Len(t, refs, 1)
	fields, ok := refs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Frank Herbert", fields["name"])
	// only the selected ref property comes back
	assert.NotContains(t, fields, "age")
}

func TestBuildResponsePrunesNestedObjects(t *testing.T) {
	// storage returns ALL nested fields of an object property — including
	// ones the selection excluded (e.g. a nested blob); pruning is the
	// reply builder's job
	res := []any{
		map[string]any{
			"id":    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"title": "Dune",
			"cover": map[string]any{
				"caption": "front cover",
				"image":   "aGVsbG8=", // nested blob, not selected
				"size": map[string]any{
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
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	cover, ok := reply.Results[0].Properties["cover"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "front cover", cover["caption"])
	assert.NotContains(t, cover, "image")
	size, ok := cover["size"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(800), size["width"])
	assert.NotContains(t, size, "height")
}

func TestBuildResponsePrunesObjectArrays(t *testing.T) {
	res := []any{
		map[string]any{
			"id": strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"scenes": []any{
				map[string]any{"name": "opening", "raw": "ZGF0YQ=="},
				map[string]any{"name": "finale", "raw": "ZGF0YQ=="},
			},
		},
	}
	params := dto.GetParams{
		Properties: search.SelectProperties{
			{Name: "scenes", IsObject: true, Props: []search.SelectProperty{
				{Name: "name", IsPrimitive: true},
			}},
		},
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	scenes, ok := reply.Results[0].Properties["scenes"].([]any)
	require.True(t, ok)
	require.Len(t, scenes, 2)
	for _, scene := range scenes {
		fields := scene.(map[string]any)
		assert.Contains(t, fields, "name")
		assert.NotContains(t, fields, "raw")
	}
}

func TestBuildResponsePrunesObjectsInsideRefs(t *testing.T) {
	res := []any{
		map[string]any{
			"id": strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			"hasAuthor": []any{
				search.LocalRef{
					Class: "Author",
					Fields: map[string]any{
						"name": "Frank Herbert",
						"address": map[string]any{
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
		AdditionalProperties: additional.Properties{ID: true},
	}

	reply, err := buildResponse(res, params, time.Millisecond)
	require.NoError(t, err)
	refs := reply.Results[0].References["hasAuthor"]
	require.Len(t, refs, 1)
	fields, ok := refs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Frank Herbert", fields["name"])
	address, ok := fields["address"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Tacoma", address["city"])
	assert.NotContains(t, address, "secret")
}

// TestBuildResponseErrorsOnMissingID: the id is requested on every search,
// so a result without a readable id is an internal error, regardless of
// what metadata the caller asked for.
func TestBuildResponseErrorsOnMissingID(t *testing.T) {
	params := dto.GetParams{
		Properties:           selectProps("title"),
		AdditionalProperties: additional.Properties{ID: true},
	}

	t.Run("id missing", func(t *testing.T) {
		_, err := buildResponse([]any{
			map[string]any{"title": "Dune"},
		}, params, time.Millisecond)
		assert.Error(t, err)
	})

	t.Run("id mistyped", func(t *testing.T) {
		_, err := buildResponse([]any{
			map[string]any{"title": "Dune", "id": "not-a-strfmt-uuid"},
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
	_, err := buildResponse([]any{"not a map"}, dto.GetParams{}, time.Millisecond)
	assert.Error(t, err)
}
