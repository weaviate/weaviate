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

package generate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativemodels "github.com/weaviate/weaviate/modules/generative-openai/additional/models"
	"github.com/weaviate/weaviate/modules/generative-openai/ent"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should fail with empty content", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(openaiClient, fakeHelper)
		in := []search.Result{
			{
				ID: "some-uuid",
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty content")
	})

	t.Run("should fail with empty question", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(openaiClient, fakeHelper)
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
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty content")
	})

	t.Run("should answer", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(openaiClient, fakeHelper)
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
		argumentModuleParams := map[string]interface{}{
			"ask": map[string]interface{}{
				"question": "question",
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*generativemodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
	})

	t.Run("should answer with property", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(openaiClient, fakeHelper)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content":  "content with answer",
					"content2": "this one is just a title",
				},
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{
			"ask": map[string]interface{}{
				"question":   "question",
				"properties": []string{"content", "content2"},
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*generativemodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 13, answerAdditional.StartPosition)
		assert.Equal(t, 19, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})
}

type fakeOpenAIClient struct{}

func (c *fakeOpenAIClient) Generate(ctx context.Context, text, question, language string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	return c.getResult(question, "generate"), nil
}

func (c *fakeOpenAIClient) getResult(question, answer string) *ent.GenerateResult {
	return &ent.GenerateResult{
		Text:     question,
		Question: question,
		Answer:   &answer,
	}
}

type fakeParamsHelper struct{}

func (h *fakeParamsHelper) GetQuestion(params interface{}) string {
	if fakeParamsMap, ok := params.(map[string]interface{}); ok {
		if question, ok := fakeParamsMap["question"].(string); ok {
			return question
		}
	}
	return ""
}

func (h *fakeParamsHelper) GetProperties(params interface{}) []string {
	if fakeParamsMap, ok := params.(map[string]interface{}); ok {
		if properties, ok := fakeParamsMap["properties"].([]string); ok {
			return properties
		}
	}
	return nil
}
