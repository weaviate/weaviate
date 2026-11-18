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

	"github.com/go-openapi/strfmt"
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

func TestAdditionalAnswerProviderScoreMapping(t *testing.T) {
	newResults := func(contents ...string) []search.Result {
		in := make([]search.Result, len(contents))
		for i, c := range contents {
			in[i] = search.Result{ID: strfmt.UUID(c), Schema: map[string]interface{}{"content": c}}
		}
		return in
	}
	ids := func(out []search.Result) []string {
		res := make([]string, len(out))
		for i := range out {
			res[i] = out[i].ID.String()
		}
		return res
	}

	tests := []struct {
		name    string
		in      []string
		scores  []ent.DocumentScore
		wantIDs []string
		wantErr string
	}{
		{
			name: "all scored, sorted by score descending",
			in:   []string{"a", "b", "c"},
			scores: []ent.DocumentScore{
				{Document: "a", Score: 0.1},
				{Document: "b", Score: 0.9},
				{Document: "c", Score: -2},
			},
			wantIDs: []string{"b", "a", "c"},
		},
		{
			name: "unscored documents are dropped",
			in:   []string{"a", "b", "c", "d"},
			scores: []ent.DocumentScore{
				{Document: "a", Unscored: true},
				{Document: "b", Score: 0.2},
				{Document: "c", Unscored: true},
				{Document: "d", Score: 0.8},
			},
			wantIDs: []string{"d", "b"},
		},
		{
			name: "all unscored",
			in:   []string{"a", "b"},
			scores: []ent.DocumentScore{
				{Document: "a", Unscored: true},
				{Document: "b", Unscored: true},
			},
			wantIDs: []string{},
		},
		{
			name:    "fewer scores than results",
			in:      []string{"a", "b", "c"},
			scores:  []ent.DocumentScore{{Document: "a", Score: 0.1}},
			wantErr: "reranker returned 1 scores for 3 results",
		},
		{
			name: "more scores than results",
			in:   []string{"a"},
			scores: []ent.DocumentScore{
				{Document: "a", Score: 0.1},
				{Document: "b", Score: 0.2},
			},
			wantErr: "reranker returned 2 scores for 1 results",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rankProvider := New(&fixedScoresRankClient{scores: tt.scores})
			property := "content"
			query := "q"
			params := &Params{Property: &property, Query: &query}

			out, err := rankProvider.AdditionalPropertyFn(context.Background(), newResults(tt.in...), params, nil, nil, nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantIDs, ids(out))
			for i := range out {
				rerank, ok := out[i].AdditionalProperties["rerank"].([]*models.RankResult)
				require.True(t, ok)
				require.Len(t, rerank, 1)
				require.NotNil(t, rerank[0].Score)
			}
		})
	}
}

type fixedScoresRankClient struct {
	scores []ent.DocumentScore
}

func (c *fixedScoresRankClient) Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error) {
	return &ent.RankResult{Query: query, DocumentScores: c.scores}, nil
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
