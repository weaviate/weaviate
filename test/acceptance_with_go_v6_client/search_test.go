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

package acceptance_with_go_v6_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/modules"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
	"github.com/weaviate/weaviate-go-client/v6/query"
	"github.com/weaviate/weaviate-go-client/v6/types"
)

// The v6 client has no standalone BM25 query: keyword search is only reachable
// through Hybrid with an alpha of 0, which the server resolves to pure BM25.
// The keyword cases below therefore all go through Hybrid.
var keywordOnly = query.Alpha(0)

// paragraphCollection has a text[] property to run keyword search against and
// an int property naming each object in the result set.
func paragraphCollection(name string, vectorizer modules.Module) collections.Collection {
	return collections.Collection{
		Name: name,
		Properties: []collections.Property{
			{
				Name:            "contents",
				DataType:        collections.DataTypeTextArray,
				Tokenization:    collections.TokenizationWord,
				IndexSearchable: true,
			},
			{
				Name:            "num",
				DataType:        collections.DataTypeInt,
				IndexFilterable: true,
			},
		},
		// UsingBlockMaxWAND is deliberately left unset so the server picks its
		// own default, rather than the test second-guessing it.
		InvertedIndex: &collections.InvertedIndexConfig{
			BM25: &collections.BM25Config{K1: 1.2, B: 0.75},
		},
		Vectors: map[string]collections.VectorConfig{
			"default": {Vectorizer: vectorizer},
		},
	}
}

func addCollectionAndObjects(t *testing.T, ctx context.Context, c *client.Client, name string, vectorizer modules.Module) *collections.Handle {
	t.Helper()

	h, err := c.Collections.Create(ctx, paragraphCollection(name, vectorizer))
	require.NoError(t, err)

	Insert(t, ctx, h,
		&data.Object{Properties: map[string]any{
			"contents": []any{"nice", "what a rain day"},
			"num":      0,
		}},
		&data.Object{Properties: map[string]any{
			"contents": []any{"rain", "snow and sun at once? nice"},
			"num":      1,
		}},
		&data.Object{Properties: map[string]any{
			"contents": []any{
				"super long text to get the score down",
				"snow and sun at the same time? How nice",
				"long text without any meaning",
				"just ignore this",
				"this too, it doesn't matter",
			},
			"num": 2,
		}},
		&data.Object{Properties: map[string]any{
			"contents": []any{
				"super long text to get the score down",
				"rain is necessary",
				"long text without any meaning",
				"just ignore this",
				"this too, it doesn't matter",
			},
			"num": 3,
		}},
	)
	return h
}

// TestSearchOnArrays covers keyword search over a text[] property. The v5 suite
// also ran this against the deprecated string[] type, which the v6 client
// cannot express.
func TestSearchOnArrays(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "Paragraph15845"
	h, err := c.Collections.Create(ctx, paragraphCollection(className, selfprovided.Vectorizer))
	require.NoError(t, err)
	defer c.Collections.Delete(ctx, className)

	Insert(t, ctx, h,
		&data.Object{Properties: map[string]any{
			"contents": []any{"what a nice day", "what a rainy day"},
			"num":      0,
		}},
		&data.Object{Properties: map[string]any{
			"contents": []any{"rain all day", "snow and sun at the same time? How nice"},
			"num":      1,
		}},
	)

	result, err := h.Query.Hybrid(ctx, query.Hybrid{
		Query:            "nice",
		QueryProperties:  []string{"contents"},
		Alpha:            keywordOnly,
		ReturnProperties: []string{"num"},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1}, nums(t, result))
}

func TestSearchOnSomeProperties(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	// only one property contains the search term
	cases := []struct {
		property string
		results  int
	}{
		{property: "one", results: 1},
		{property: "two", results: 0},
	}
	for _, tt := range cases {
		t.Run("search on property "+tt.property, func(t *testing.T) {
			className := "Paragraph15845"
			textProperty := func(name string) collections.Property {
				return collections.Property{
					Name:            name,
					DataType:        collections.DataTypeText,
					Tokenization:    collections.TokenizationWord,
					IndexSearchable: true,
				}
			}
			h, err := c.Collections.Create(ctx, collections.Collection{
				Name:       className,
				Properties: []collections.Property{textProperty("one"), textProperty("two")},
				InvertedIndex: &collections.InvertedIndexConfig{
					BM25: &collections.BM25Config{K1: 1.2, B: 0.75},
				},
				Vectors: map[string]collections.VectorConfig{
					"default": {Vectorizer: selfprovided.Vectorizer},
				},
			})
			require.NoError(t, err)
			defer c.Collections.Delete(ctx, className)

			Insert(t, ctx, h, &data.Object{Properties: map[string]any{"one": "hello", "two": "world"}})

			result, err := h.Query.Hybrid(ctx, query.Hybrid{
				Query:           "hello",
				QueryProperties: []string{tt.property},
				Alpha:           keywordOnly,
				ReturnMetadata:  query.ReturnMetadata{Score: true},
			})
			require.NoError(t, err)
			require.Len(t, result.Objects, tt.results)

			for _, o := range result.Objects {
				require.NotNil(t, o.Metadata.Score)
				require.Greater(t, *o.Metadata.Score, float32(0))
			}
		})
	}
}

func TestAutocut(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "Paragraph453745"
	h := addCollectionAndObjects(t, ctx, c, className, selfprovided.Vectorizer)
	defer c.Collections.Delete(ctx, className)

	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 2},
		{autocut: 2, numResults: 4},
		{autocut: 0, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			result, err := h.Query.Hybrid(ctx, query.Hybrid{
				Query:            "rain nice",
				QueryProperties:  []string{"contents"},
				Alpha:            keywordOnly,
				Fusion:           query.HybridFusionRelativeScore,
				AutoLimit:        tt.autocut,
				ReturnProperties: []string{"num"},
			})
			require.NoError(t, err)

			got := nums(t, result)
			require.Len(t, got, tt.numResults)
			require.Equal(t, int64(0), got[0])
			require.Equal(t, int64(1), got[1])
		})
	}
}

func TestHybridWithPureVectorSearch(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "ParagraphWithManyWords"
	h := addCollectionAndObjects(t, ctx, c, className, contextionary{})
	defer c.Collections.Delete(ctx, className)

	result, err := h.Query.Hybrid(ctx, query.Hybrid{
		Query:           "rain nice",
		QueryProperties: []string{"contents"},
		Alpha:           query.Alpha(1),
	})
	require.NoError(t, err)
	require.Len(t, result.Objects, 4)
}

func TestHybridWithNearTextSubsearch(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "ParagraphWithManyWords"
	h := addCollectionAndObjects(t, ctx, c, className, contextionary{})
	defer c.Collections.Delete(ctx, className)

	result, err := h.Query.Hybrid(ctx, query.Hybrid{
		NearText:        &query.NearText{Concepts: []string{"rain", "nice"}},
		QueryProperties: []string{"contents"},
		Alpha:           query.Alpha(1),
	})
	require.NoError(t, err)
	require.Len(t, result.Objects, 4)
}

func TestHybridWithNearVectorSubsearch(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "HybridVectorOnlySearch"
	h, err := c.Collections.Create(ctx, collections.Collection{
		Name: className,
		Properties: []collections.Property{
			{Name: "text", DataType: collections.DataTypeText},
		},
		Vectors: map[string]collections.VectorConfig{
			"default": {Vectorizer: contextionary{}},
		},
	})
	require.NoError(t, err)
	defer c.Collections.Delete(ctx, className)

	Insert(t, ctx, h, &data.Object{Properties: map[string]any{
		"text": "how much wood can a woodchuck chuck?",
	}})

	// The v5 suite read the vector off the insert response; v6 inserts are
	// batched and do not return one, so read it back from the collection.
	stored, err := h.Query.OverAll(ctx, query.OverAll{ReturnVectors: []string{"default"}})
	require.NoError(t, err)
	require.Len(t, stored.Objects, 1)
	vector := stored.Objects[0].Vectors["default"]
	require.NotEmpty(t, vector.Single)

	result, err := h.Query.Hybrid(ctx, query.Hybrid{
		NearVector: &query.NearVector{
			Target: types.Vector{Name: "default", Single: vector.Single},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Objects, 1)
}

// TestNearVectorAutocut covers the near-vector half of the v5 suite's
// TestNearVectorAndObjectAutocut. The v6 client has no near-object query.
func TestNearVectorAutocut(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "YellowAndBlueTrain"
	h, err := c.Collections.Create(ctx, collections.Collection{
		Name: className,
		Vectors: map[string]collections.VectorConfig{
			"default": {Vectorizer: selfprovided.Vectorizer},
		},
	})
	require.NoError(t, err)
	defer c.Collections.Delete(ctx, className)

	vectorNumbers := []float32{1, 1.1, 1.2, 2.0, 2.1, 2.2, 3.1, 3.2, 3.2}
	objects := make([]*data.Object, len(vectorNumbers))
	for i, vectorNumber := range vectorNumbers {
		objects[i] = &data.Object{Vectors: []types.Vector{{
			Name:   "default",
			Single: []float32{1, 1, 1, 1, 1, vectorNumber},
		}}}
	}
	Insert(t, ctx, h, objects...)

	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 3},
		{autocut: 2, numResults: 6},
		{autocut: 0, numResults: 9 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			result, err := h.Query.NearVector(ctx, query.NearVector{
				Target:    types.Vector{Name: "default", Single: []float32{1, 1, 1, 1, 1, 1}},
				AutoLimit: tt.autocut,
			})
			require.NoError(t, err)
			require.Len(t, result.Objects, tt.numResults)
		})
	}
}

func TestHybridExplainScore(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "ParagraphWithManyWords"
	h := addCollectionAndObjects(t, ctx, c, className, contextionary{})
	defer c.Collections.Delete(ctx, className)

	Insert(t, ctx, h, &data.Object{Properties: map[string]any{
		"contents": []any{"specific", "hybrid", "search", "object"},
		"num":      4,
	}})

	cases := []struct {
		name     string
		hybrid   query.Hybrid
		contains []string
	}{
		{
			name: "ranked fusion with alpha",
			hybrid: query.Hybrid{
				Query:           "rain nice",
				QueryProperties: []string{"contents"},
				Alpha:           query.Alpha(0.5),
				Fusion:          query.HybridFusionRanked,
			},
			contains: []string{
				"contributed 0.008333334 to the score",
				"contributed 0.008196721 to the score",
			},
		},
		{
			name: "ranked fusion",
			hybrid: query.Hybrid{
				Query:           "rain snow sun score",
				QueryProperties: []string{"contents"},
				Fusion:          query.HybridFusionRanked,
			},
			contains: []string{
				"contributed 0.004166667 to the score",
				"contributed 0.0125 to the score",
			},
		},
		{
			name: "relative score fusion",
			hybrid: query.Hybrid{
				Query:           "rain snow sun score",
				QueryProperties: []string{"contents"},
				Fusion:          query.HybridFusionRelativeScore,
			},
			contains: []string{
				"normalized score: 0.75",
				"normalized score: 0.25",
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			tt.hybrid.ReturnMetadata = query.ReturnMetadata{Score: true, ExplainScore: true}

			result, err := h.Query.Hybrid(ctx, tt.hybrid)
			require.NoError(t, err)
			require.Len(t, result.Objects, 5)
			for _, o := range result.Objects {
				require.NotNil(t, o.Metadata.Score)
			}

			require.NotNil(t, result.Objects[0].Metadata.ExplainScore)
			for _, want := range tt.contains {
				require.Contains(t, *result.Objects[0].Metadata.ExplainScore, want)
			}
		})
	}
}

func TestNearTextAutocut(t *testing.T) {
	ctx := context.Background()
	c := NewClient(t, ctx)
	require.NoError(t, c.Collections.DeleteAll(ctx))

	className := "YellowAndBlueSub"
	h, err := c.Collections.Create(ctx, collections.Collection{
		Name: className,
		Properties: []collections.Property{
			{Name: "text", DataType: collections.DataTypeText, Tokenization: collections.TokenizationWord},
		},
		Vectors: map[string]collections.VectorConfig{
			"default": {Vectorizer: contextionary{}},
		},
	})
	require.NoError(t, err)
	defer c.Collections.Delete(ctx, className)

	texts := []string{"word", "another word", "another word and", "completely unrelated"}
	objects := make([]*data.Object, len(texts))
	for i, text := range texts {
		objects[i] = &data.Object{Properties: map[string]any{"text": text}}
	}
	Insert(t, ctx, h, objects...)

	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 3},
		{autocut: 0, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			result, err := h.Query.NearText(ctx, query.NearText{
				Concepts:  []string{"word"},
				AutoLimit: tt.autocut,
			})
			require.NoError(t, err)
			require.Len(t, result.Objects, tt.numResults)
		})
	}
}

func nums(t *testing.T, result *query.Result) []int64 {
	t.Helper()

	out := make([]int64, len(result.Objects))
	for i, o := range result.Objects {
		num, ok := o.Properties["num"].(int64)
		require.Truef(t, ok, "unexpected type for num: %T", o.Properties["num"])
		out[i] = num
	}
	return out
}
