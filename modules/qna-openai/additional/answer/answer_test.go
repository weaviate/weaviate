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

package answer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	qnamodels "github.com/weaviate/weaviate/modules/qna-openai/additional/models"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should fail with empty content", func(t *testing.T) {
		// given
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
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
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
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
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
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
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
	})

	t.Run("should answer with property", func(t *testing.T) {
		// given
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
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
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 13, answerAdditional.StartPosition)
		assert.Equal(t, 19, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})
}

type fakeQnAClient struct{}

func (c *fakeQnAClient) Answer(ctx context.Context, text, question string, cfg moduletools.ClassConfig) (*ent.AnswerResult, error) {
	return c.getAnswer(question, "answer"), nil
}

func (c *fakeQnAClient) getAnswer(question, answer string) *ent.AnswerResult {
	return &ent.AnswerResult{
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
