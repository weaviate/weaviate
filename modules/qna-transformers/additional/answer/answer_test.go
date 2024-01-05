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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	qnamodels "github.com/weaviate/weaviate/modules/qna-transformers/additional/models"
	"github.com/weaviate/weaviate/modules/qna-transformers/ent"
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
		assert.Equal(t, 0.8, *answerAdditional.Certainty)
		assert.InDelta(t, 0.4, *answerAdditional.Distance, 1e-9)
		assert.Equal(t, 13, answerAdditional.StartPosition)
		assert.Equal(t, 19, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})

	t.Run("should answer with similarity set above ask distance", func(t *testing.T) {
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
				"distance":   float64(0.4),
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(out))
		answer, answerOK := out[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.8, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.8)), *answerAdditional.Distance)
		assert.Equal(t, 13, answerAdditional.StartPosition)
		assert.Equal(t, 19, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})

	t.Run("should answer with similarity set above ask certainty", func(t *testing.T) {
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
				"certainty":  float64(0.8),
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(out))
		answer, answerOK := out[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "answer", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.8, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.8)), *answerAdditional.Distance)
		assert.Equal(t, 13, answerAdditional.StartPosition)
		assert.Equal(t, 19, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})

	t.Run("should not answer with distance set below ask distance", func(t *testing.T) {
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
				"distance":   float64(0.19),
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
		assert.True(t, answerAdditional.Result == nil)
		assert.True(t, answerAdditional.Property == nil)
		assert.True(t, answerAdditional.Certainty == nil)
		assert.True(t, answerAdditional.Distance == nil)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 0, answerAdditional.EndPosition)
		assert.Equal(t, false, answerAdditional.HasAnswer)
	})

	t.Run("should not answer with certainty set below ask certainty", func(t *testing.T) {
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
				"certainty":  float64(0.81),
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
		assert.True(t, answerAdditional.Result == nil)
		assert.True(t, answerAdditional.Property == nil)
		assert.True(t, answerAdditional.Certainty == nil)
		assert.True(t, answerAdditional.Distance == nil)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 0, answerAdditional.EndPosition)
		assert.Equal(t, false, answerAdditional.HasAnswer)
	})

	t.Run("should answer with certainty set above ask certainty and the results should be reranked", func(t *testing.T) {
		// given
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
		in := []search.Result{
			{
				ID: "uuid1",
				Schema: map[string]interface{}{
					"content": "rerank 0.5",
				},
			},
			{
				ID: "uuid2",
				Schema: map[string]interface{}{
					"content": "rerank 0.2",
				},
			},
			{
				ID: "uuid3",
				Schema: map[string]interface{}{
					"content": "rerank 0.9",
				},
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{
			"ask": map[string]interface{}{
				"question":   "question",
				"properties": []string{"content"},
				"rerank":     true,
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 3, len(in))
		answer, answerOK := in[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.9", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.9, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.9)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)

		answer, answerOK = in[1].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK = answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.5", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.5, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.5)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)

		answer, answerOK = in[2].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK = answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.2", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.2, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.2)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})

	t.Run("should answer with certainty set above ask certainty and the results should not be reranked", func(t *testing.T) {
		// given
		qnaClient := &fakeQnAClient{}
		fakeHelper := &fakeParamsHelper{}
		answerProvider := New(qnaClient, fakeHelper)
		in := []search.Result{
			{
				ID: "uuid1",
				Schema: map[string]interface{}{
					"content": "rerank 0.5",
				},
			},
			{
				ID: "uuid2",
				Schema: map[string]interface{}{
					"content": "rerank 0.2",
				},
			},
			{
				ID: "uuid3",
				Schema: map[string]interface{}{
					"content": "rerank 0.9",
				},
			},
		}
		fakeParams := &Params{}
		limit := 1
		argumentModuleParams := map[string]interface{}{
			"ask": map[string]interface{}{
				"question":   "question",
				"properties": []string{"content"},
				"rerank":     false,
			},
		}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 3, len(in))
		answer, answerOK := in[0].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.5", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.5, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.5)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)

		answer, answerOK = in[1].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK = answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.2", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.2, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.2)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)

		answer, answerOK = in[2].AdditionalProperties["answer"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK = answer.(*qnamodels.Answer)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "rerank 0.9", *answerAdditional.Result)
		assert.Equal(t, "content", *answerAdditional.Property)
		assert.Equal(t, 0.9, *answerAdditional.Certainty)
		assert.Equal(t, *additional.CertaintyToDistPtr(ptFloat(0.9)), *answerAdditional.Distance)
		assert.Equal(t, 0, answerAdditional.StartPosition)
		assert.Equal(t, 10, answerAdditional.EndPosition)
		assert.Equal(t, true, answerAdditional.HasAnswer)
	})
}

type fakeQnAClient struct{}

func (c *fakeQnAClient) Answer(ctx context.Context,
	text, question string,
) (*ent.AnswerResult, error) {
	if text == "rerank 0.9" {
		return c.getAnswer(question, "rerank 0.9", 0.9), nil
	}
	if text == "rerank 0.5" {
		return c.getAnswer(question, "rerank 0.5", 0.5), nil
	}
	if text == "rerank 0.2" {
		return c.getAnswer(question, "rerank 0.2", 0.2), nil
	}
	return c.getAnswer(question, "answer", 0.8), nil
}

func (c *fakeQnAClient) getAnswer(question, answer string, certainty float64) *ent.AnswerResult {
	return &ent.AnswerResult{
		Text:      question,
		Question:  question,
		Answer:    &answer,
		Certainty: &certainty,
		Distance:  additional.CertaintyToDistPtr(&certainty),
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

func (h *fakeParamsHelper) GetCertainty(params interface{}) float64 {
	if fakeParamsMap, ok := params.(map[string]interface{}); ok {
		if certainty, ok := fakeParamsMap["certainty"].(float64); ok {
			return certainty
		}
	}
	return 0
}

func (h *fakeParamsHelper) GetDistance(params interface{}) float64 {
	if fakeParamsMap, ok := params.(map[string]interface{}); ok {
		if distance, ok := fakeParamsMap["distance"].(float64); ok {
			return distance
		}
	}
	return 0
}

func (h *fakeParamsHelper) GetRerank(params interface{}) bool {
	if fakeParamsMap, ok := params.(map[string]interface{}); ok {
		if rerank, ok := fakeParamsMap["rerank"].(bool); ok {
			return rerank
		}
	}
	return false
}

func ptFloat(f float64) *float64 {
	return &f
}
