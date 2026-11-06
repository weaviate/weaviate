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
	"testing"

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

func TestRankScoreCountMustMatchDocuments(t *testing.T) {
	tests := []struct {
		name    string
		scores  int
		wantErr string
	}{
		{name: "one score per document", scores: 2},
		{name: "no scores", scores: 0, wantErr: "reranker returned 0 scores for 2 documents"},
		{name: "fewer scores than documents", scores: 1, wantErr: "reranker returned 1 scores for 2 documents"},
		{name: "more scores than documents", scores: 3, wantErr: "reranker returned 3 scores for 2 documents"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rankProvider := New(&fixedCountRankClient{scores: tt.scores})
			in := []search.Result{
				{ID: "uuid-1", Schema: map[string]interface{}{"content": "a"}},
				{ID: "uuid-2", Schema: map[string]interface{}{"content": "b"}},
			}
			property := "content"
			params := &Params{Property: &property}

			var err error
			require.NotPanics(t, func() {
				_, err = rankProvider.AdditionalPropertyFn(context.Background(), in, params, nil, nil, nil)
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			for i := range in {
				require.Contains(t, in[i].AdditionalProperties, "rerank")
			}
		})
	}
}

type fixedCountRankClient struct {
	scores int
}

func (c *fixedCountRankClient) Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error) {
	scores := make([]ent.DocumentScore, c.scores)
	for i := range scores {
		scores[i] = ent.DocumentScore{Score: float64(i)}
	}
	return &ent.RankResult{DocumentScores: scores, Query: query}, nil
}
