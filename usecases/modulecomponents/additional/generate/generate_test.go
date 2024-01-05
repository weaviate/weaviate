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

package generate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
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
		s := "this is a task"
		fakeParams := &Params{
			Task: &s,
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
		assert.Equal(t, "this is a task", *answerAdditional.GroupedResult)
	})
}

type fakeOpenAIClient struct{}

func (c *fakeOpenAIClient) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return c.getResults(textProperties, task), nil
}

func (c *fakeOpenAIClient) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return c.getResult(textProperties, prompt), nil
}

func (c *fakeOpenAIClient) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	return &generativemodels.GenerateResponse{
		Result: &prompt,
	}, nil
}

func (c *fakeOpenAIClient) getResults(text []map[string]string, task string) *generativemodels.GenerateResponse {
	return &generativemodels.GenerateResponse{
		Result: &task,
	}
}

func (c *fakeOpenAIClient) getResult(text map[string]string, task string) *generativemodels.GenerateResponse {
	return &generativemodels.GenerateResponse{
		Result: &task,
	}
}
