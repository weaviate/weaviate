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
		require.EqualError(t, err, "error ranking with cohere: unavailable")
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
