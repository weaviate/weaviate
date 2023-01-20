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
	t.Run("should answer", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		answerProvider := New(openaiClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content": "content",
				},
			},
		}
		fakeParams := &Params{
			Task:           "this is a task",
			ResultLanguage: "English",
			OnSet:          "individualResults",
			Properties:     nil,
		}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["generate"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*generativemodels.GenerateResult)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "this is a task", *answerAdditional.Result)
	})

	t.Run("should answer with property", func(t *testing.T) {
		// given
		openaiClient := &fakeOpenAIClient{}
		answerProvider := New(openaiClient)
		in := []search.Result{
			{
				ID: "some-uuid",
				Schema: map[string]interface{}{
					"content":  "content with answer",
					"content2": "this one is just a title",
				},
			},
		}
		fakeParams := &Params{
			Task:           "this is a task",
			ResultLanguage: "English",
			OnSet:          "individualResults",
			Properties:     []string{"content", "content2"},
		}
		limit := 1
		argumentModuleParams := map[string]interface{}{}

		// when
		out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams, nil)

		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		answer, answerOK := in[0].AdditionalProperties["generate"]
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.(*generativemodels.GenerateResult)
		assert.True(t, answerAdditionalOK)
		assert.Equal(t, "this is a task", *answerAdditional.Result)
	})
}

type fakeOpenAIClient struct{}

func (c *fakeOpenAIClient) Generate(ctx context.Context, textProperties []map[string]string, task string, language string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	return c.getResult(textProperties, task, language, "generate"), nil
}

func (c *fakeOpenAIClient) getResult(text []map[string]string, task, language, s string) *ent.GenerateResult {
	return &ent.GenerateResult{
		Result: &task,
	}
}
