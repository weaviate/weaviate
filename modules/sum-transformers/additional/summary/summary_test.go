//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package summary

import (
	"context"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/modules/sum-transformers/ent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams)

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
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams)

		// then
		require.NotNil(t, err)
		require.NotEmpty(t, out)
		assert.Error(t, err, "empty params")
	})

	t.Run("should summarize", func(t *testing.T) {
		// given
		// logger := logrus.New()
		// uri := "http://localhost:8006"
		// fmt.Print("uri is: ", uri)
		// sumClient := client.New(uri, logger)
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
		out, err := summaryProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams)

		fmt.Print("out is: ", out, "\n")
		// then
		require.Nil(t, err)
		require.NotEmpty(t, out)
		assert.Equal(t, 1, len(in))
		fmt.Print("in is\n", in, "\n")
		fmt.Print("out is\n", out, "\n")
		answer, answerOK := in[0].AdditionalProperties["summary"]
		fmt.Print("answer is \n", answer, "\n")
		fmt.Printf("%+v\n", answer)
		fmt.Printf("Type of answer is %T \n", answer)
		assert.True(t, answerOK)
		assert.NotNil(t, answer)
		answerAdditional, answerAdditionalOK := answer.([]ent.SummaryResult)
		fmt.Print("answerAdditional is \n", answerAdditional, "\n")
		assert.True(t, answerAdditionalOK)
		// assert.Equal(t, "this is the summary", answerAdditional.Result)
		// assert.Equal(t, "content", answerAdditional.Property)
	})

	// t.Run("should answer with property", func(t *testing.T) {
	// 	// given
	// 	qnaClient := &fakeQnAClient{}
	// 	fakeHelper := &fakeParamsHelper{}
	// 	answerProvider := New(qnaClient, fakeHelper)
	// 	in := []search.Result{
	// 		{
	// 			ID: "some-uuid",
	// 			Schema: map[string]interface{}{
	// 				"content":  "content with answer",
	// 				"content2": "this one is just a title",
	// 			},
	// 		},
	// 	}
	// 	fakeParams := &Params{}
	// 	limit := 1
	// 	argumentModuleParams := map[string]interface{}{}

	// 	// when
	// 	out, err := answerProvider.AdditionalPropertyFn(context.Background(), in, fakeParams, &limit, argumentModuleParams)

	// 	// then
	// 	require.Nil(t, err)
	// 	require.NotEmpty(t, out)
	// 	assert.Equal(t, 1, len(in))
	// 	answer, answerOK := in[0].AdditionalProperties["answer"]
	// 	assert.True(t, answerOK)
	// 	assert.NotNil(t, answer)
	// 	answerAdditional, answerAdditionalOK := answer.(*qnamodels.Answer)
	// 	assert.True(t, answerAdditionalOK)
	// 	assert.Equal(t, "answer", *answerAdditional.Result)
	// 	assert.Equal(t, "content", *answerAdditional.Property)
	// 	assert.Equal(t, 0.8, *answerAdditional.Certainty)
	// 	assert.Equal(t, 13, answerAdditional.StartPosition)
	// 	assert.Equal(t, 19, answerAdditional.EndPosition)
	// 	assert.Equal(t, true, answerAdditional.HasAnswer)
	// })

}

type fakeSUMClient struct{}

func (c *fakeSUMClient) GetSummary(ctx context.Context, property, text string) ([]ent.SummaryResult, error) {
	return c.getSummary(property), nil
}

func (c *fakeSUMClient) getSummary(property string) []ent.SummaryResult {
	return []ent.SummaryResult{{
		Property: property,
		Result:   "this is the summary"}}
}
