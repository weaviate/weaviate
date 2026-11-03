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

package rank

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should fail with empty content", func(t *testing.T) {
		// given
		rankClient := &fakeRankClient{}
		rankProvider := New(rankClient)
		in := []search.Result{
			{
				ID: "some-uuid",
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := rankProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty schema content")
	})

	t.Run("should fail with empty params", func(t *testing.T) {
		// given
		rankClient := &fakeRankClient{}
		rankProvider := New(rankClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "content",
				},
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := rankProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty params")
	})

	t.Run("should fail on cohere error", func(t *testing.T) {
		rankClient := &fakeRankClient{}
		rankProvider := New(rankClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "this is the content",
				},
			},
		}
		property := "content"
		query := "unavailable"
		fakeParams := &Params{Property: &property, Query: &query}
		limit := 3
		argumentModuleParams := map[string]interface{}{}

		_, err := rankProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)
		require.EqualError(t, err, "client rank: unavailable")
	})

	t.Run("should rank", func(t *testing.T) {
		rankClient := &fakeRankClient{}
		rankProvider := New(rankClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "this is the content",
				},
			},
		}
		property := "content"
		query := "this is the query"
		fakeParams := &Params{Property: &property, Query: &query}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := rankProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)
		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["rerank"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, ok := answer.([]*models.RankResult)
		require.True(t, ok)
		require.Len(t, answerAdditional, 1)
		assert.Equal(t, float64(0.15), *answerAdditional[0].Score)
	})
}

type fakeRankClient struct{}

func (c *fakeRankClient) Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (result *ent.RankResult, err error) {
	if query == "unavailable" {
		return nil, errors.New("unavailable")
	}
	score := 0.15
	result = &ent.RankResult{
		DocumentScores: []ent.DocumentScore{
			{
				Document: documents[0],
				Score:    score,
			},
		},
		Query: query,
	}
	return result, nil
}

type fixedScoresClient struct{ scores map[string]float64 }

func (c *fixedScoresClient) Rank(_ context.Context, query string, documents []string, _ moduletools.ClassConfig) (*ent.RankResult, error) {
	out := make([]ent.DocumentScore, len(documents))
	for i, d := range documents {
		out[i] = ent.DocumentScore{Document: d, Score: c.scores[d]}
	}
	return &ent.RankResult{Query: query, DocumentScores: out}, nil
}

func TestRankKeepsSearchOrderOnEqualScores(t *testing.T) {
	manyIDs := make([]string, 100)
	manyScores := make(map[string]float64, len(manyIDs))
	manyWant := make([]string, 0, len(manyIDs))
	for i := range manyIDs {
		manyIDs[i] = fmt.Sprintf("id-%03d", i)
		manyScores[manyIDs[i]] = float64(i % 3)
	}
	for _, score := range []float64{2, 1, 0} {
		for _, id := range manyIDs {
			if manyScores[id] == score {
				manyWant = append(manyWant, id)
			}
		}
	}

	tests := []struct {
		name   string
		ids    []string
		scores map[string]float64
		want   []string
	}{
		{
			name:   "ties among five results",
			ids:    []string{"a", "b", "c", "d", "e"},
			scores: map[string]float64{"a": 0.5, "b": 0.9, "c": 0.5, "d": 0.5, "e": 0.9},
			want:   []string{"b", "e", "a", "c", "d"},
		},
		{
			name:   "all scores equal",
			ids:    []string{"a", "b", "c", "d", "e"},
			scores: map[string]float64{"a": 0.5, "b": 0.5, "c": 0.5, "d": 0.5, "e": 0.5},
			want:   []string{"a", "b", "c", "d", "e"},
		},
		{
			name:   "ties among many results",
			ids:    manyIDs,
			scores: manyScores,
			want:   manyWant,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make([]search.Result, len(tt.ids))
			for i, id := range tt.ids {
				in[i] = search.Result{ID: strfmt.UUID(id), Schema: map[string]interface{}{"content": id}}
			}
			provider := New(&fixedScoresClient{scores: tt.scores})
			property := "content"
			query := "query"
			limit := len(in)

			out, err := provider.AdditionalPropertyFn(context.Background(), in,
				&Params{Property: &property, Query: &query}, &limit, nil, nil)
			require.NoError(t, err)

			got := make([]string, len(out))
			for i := range out {
				got[i] = string(out[i].ID)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
