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

package summary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/sum-transformers/ent"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should fail with empty content", func(t *testing.T) {
		// given
		sumClient := &fakeSUMClient{}
		summaryProvider := New(sumClient)
		in := []search.Result{
			{
				ID: "some-uuid",
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty schema content")
	})

	t.Run("should fail with empty params", func(t *testing.T) {
		// given
		sumClient := &fakeSUMClient{}
		summaryProvider := New(sumClient)
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
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty params")
	})

	t.Run("should summarize", func(t *testing.T) {
		sumClient := &fakeSUMClient{}
		summaryProvider := New(sumClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "this is the content",
				},
			},
		}
		fakeParams := &Params{Properties: []string{"content"}}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)
		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["summary"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.([]ent.SummaryResult)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "this is the summary", answerAdditional[0].Result)
		assert.Equal(t, "content", answerAdditional[0].Property)
	})
}

type fakeSUMClient struct{}

func (c *fakeSUMClient) GetSummary(ctx context.Context, property, text string,
) ([]ent.SummaryResult, error) {
	return c.getSummary(property), nil
}

func (c *fakeSUMClient) getSummary(property string) []ent.SummaryResult {
	return []ent.SummaryResult{{
		Property: property,
		Result:   "this is the summary",
	}}
}
