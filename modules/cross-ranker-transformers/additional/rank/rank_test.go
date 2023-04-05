//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rank

import (
	"context"
	crossrankmodels "github.com/weaviate/weaviate/modules/cross-ranker-transformers/additional/models"
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/ent"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		answer, answerOK := in[0].AdditionalProperties["crossrank"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, _ := answer.(ent.RankResult)
		//assert.True(t, answerAdditionalOK)
		assert.Equal(t, float64(0), answerAdditional.Score)
	})
}

type fakeRankClient struct{}

func (c *fakeRankClient) Rank(ctx context.Context, rankpropertyValue string, query string,
) (*ent.RankResult, error) {
	score := 0.15
	//rankResult := c.getRank(rankpropertyValue, query)
	returnResult := ent.RankResult{
		Score:             score,
		Query:             query,
		RankPropertyValue: rankpropertyValue,
	}
	return &returnResult, nil
}

func (c *fakeRankClient) getRank(rankpropertyValue string, query string) crossrankmodels.RankResult {
	score := 0.15
	return crossrankmodels.RankResult{
		Score: &score,
	}
}
