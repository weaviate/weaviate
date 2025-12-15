//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestAWS_ModelChecks(t *testing.T) {
	c := &awsClient{}
	// Amazon
	assert.True(t, c.isAmazonNovaModel("amazon.nova-micro-v1:0"))
	assert.True(t, c.isAmazonNovaModel("us.amazon.nova-micro-v1:0"))
	assert.False(t, c.isAmazonTitanModel("us.amazon.nova-micro-v1:0"))
	assert.False(t, c.isAmazonTitanModel("amazon.nova-micro-v1:0"))
	assert.True(t, c.isAmazonTitanModel("us.amazon.titan-text-lite-v1"))
	assert.True(t, c.isAmazonTitanModel("amazon.titan-text-lite-v1"))
	// Anthropic
	assert.True(t, c.isAnthropicModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.True(t, c.isAnthropicModel("us.anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.True(t, c.isAnthropicClaude3Model("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.True(t, c.isAnthropicClaude3Model("us.anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.False(t, c.isAmazonTitanModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.False(t, c.isAmazonTitanModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	// Cohere
	assert.True(t, c.isCohereCommandRModel("cohere.command-r-v1:0"))
	assert.True(t, c.isCohereCommandRModel("us.cohere.command-r-v1:0"))
	assert.True(t, c.isCohereModel("cohere.command-r-v1:0"))
	assert.True(t, c.isCohereModel("us.cohere.command-r-v1:0"))
	// Meta
	assert.True(t, c.isMetaModel("meta.llama3-70b-instruct-v1:0"))
	assert.True(t, c.isMetaModel("us.meta.llama3-70b-instruct-v1:0"))
	// Mistral
	assert.True(t, c.isMistralAIModel("mistral.mistral-large-2402-v1:0"))
	assert.True(t, c.isMistralAIModel("us.mistral.mistral-large-2402-v1:0"))
}

func TestAWS_GetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		t.Skip("Skipping this test for now")
		handler := &testAnswerHandler{
			t: t,
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := &awsClient{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "123",
			awsSecretKey: "123",
			buildBedrockUrlFn: func(service, region, model string) string {
				return server.URL
			},
			buildSagemakerUrlFn: func(service, region, endpoint string) string {
				return server.URL
			},
		}

		props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}
		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		t.Skip("Skipping this test for now")
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
		})
		defer server.Close()

		c := &awsClient{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "123",
			awsSecretKey: "123",
			buildBedrockUrlFn: func(service, region, model string) string {
				return server.URL
			},
			buildSagemakerUrlFn: func(service, region, endpoint string) string {
				return server.URL
			},
		}

		props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

		_, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, nil)

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to AWS failed with status: 200 error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var outBytes []byte
	authHeader := r.Header["Authorization"][0]
	if strings.Contains(authHeader, "bedrock") {
		var request bedrockAmazonGenerateRequest
		require.Nil(f.t, json.Unmarshal(bodyBytes, &request))

		outBytes, err = json.Marshal(request)
		require.Nil(f.t, err)
	}

	w.Write(outBytes)
}

func ptString(in string) *string {
	return &in
}
