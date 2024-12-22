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

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/search"
)

func TestAdditionalAnswerProvider(t *testing.T) {
	t.Run("should answer", func(t *testing.T) {
		// given
		logger, _ := test.NewNullLogger()
		client := &fakeClient{}
		defaultProviderName := "openai"
		additionalGenerativeParameters := map[string]modulecapabilities.GenerativeProperty{
			defaultProviderName: {Client: client},
		}
		answerProvider := NewGeneric(additionalGenerativeParameters, defaultProviderName, logger)
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
		answerAdditional, answerAdditionalOK := answer.(map[string]interface{})
		assert.True(t, answerAdditionalOK)
		groupedResult, ok := answerAdditional["groupedResult"].(*string)
		assert.True(t, ok)
		assert.Equal(t, "this is a task", *groupedResult)
	})
}

type fakeClient struct{}

func (c *fakeClient) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResults(textProperties, task), nil
}

func (c *fakeClient) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResult(textProperties, prompt), nil
}

func (c *fakeClient) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, settings interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	return &modulecapabilities.GenerateResponse{
		Result: &prompt,
	}, nil
}

func (c *fakeClient) getResults(text []map[string]string, task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}

func (c *fakeClient) getResult(text map[string]string, task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}
