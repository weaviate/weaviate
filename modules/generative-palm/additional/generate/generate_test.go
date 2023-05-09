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
	generativemodels "github.com/weaviate/weaviate/modules/generative-palm/additional/models"
	"github.com/weaviate/weaviate/modules/generative-palm/ent"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should answer", func(t *testing.T) {
		// given
		palmClient := &fakePaLMClient{}
		answerProvider := New(palmClient)
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

type fakePaLMClient struct{}

func (c *fakePaLMClient) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	return c.getResults(textProperties, task), nil
}

func (c *fakePaLMClient) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error) {
	return c.getResult(textProperties, prompt), nil
}

func (c *fakePaLMClient) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error) {
	return &ent.GenerateResult{
		Result: &prompt,
	}, nil
}

func (c *fakePaLMClient) getResults(text []map[string]string, task string) *ent.GenerateResult {
	return &ent.GenerateResult{
		Result: &task,
	}
}

func (c *fakePaLMClient) getResult(text map[string]string, task string) *ent.GenerateResult {
	return &ent.GenerateResult{
		Result: &task,
	}
}
