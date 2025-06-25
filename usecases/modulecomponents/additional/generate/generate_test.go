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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
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

func (c *fakeClient) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResults(task), nil
}

func (c *fakeClient) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, settings interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return c.getResult(prompt), nil
}

func (c *fakeClient) getResults(task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}

func (c *fakeClient) getResult(task string) *modulecapabilities.GenerateResponse {
	return &modulecapabilities.GenerateResponse{
		Result: &task,
	}
}

func Test_getProperties(t *testing.T) {
	var provider GenerateProvider

	for _, tt := range []struct {
		missing  any
		dataType schema.DataType
	}{
		{nil, schema.DataTypeBlob},
		{[]string{}, schema.DataTypeTextArray},
		{nil, schema.DataTypeTextArray},
	} {
		t.Run(fmt.Sprintf("%s=%v", tt.dataType, tt.missing), func(t *testing.T) {
			result := search.Result{
				Schema: models.PropertySchema(map[string]any{
					"missing": tt.missing,
				}),
			}

			// Get provider to iterate over a result object with a nil property.
			require.NotPanics(t, func() {
				provider.getProperties(result, []string{"missing"},
					map[string]schema.DataType{"missing": tt.dataType})
			})
		})
	}
}
