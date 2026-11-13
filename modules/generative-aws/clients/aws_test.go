//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	awsparams "github.com/weaviate/weaviate/modules/generative-aws/parameters"
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
	// Anthropic — Messages API models (Claude 3.x, 4.x, future families)
	assert.True(t, c.isAnthropicModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.True(t, c.isAnthropicModel("us.anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("us.anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.False(t, c.isAmazonTitanModel("anthropic.claude-3-7-sonnet-20250219-v1:0"))
	assert.True(t, c.isAnthropicModel("anthropic.claude-sonnet-4-5-20250929-v1:0"))
	assert.True(t, c.isAnthropicModel("us.anthropic.claude-sonnet-4-5-20250929-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("anthropic.claude-sonnet-4-5-20250929-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("anthropic.claude-sonnet-4-20250514-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("anthropic.claude-haiku-4-5-20251001-v1:0"))
	assert.False(t, c.isAnthropicLegacyModel("anthropic.claude-opus-4-1-20250805-v1:0"))
	// Anthropic — legacy text-completion API (claude-v1, claude-v2, claude-instant)
	assert.True(t, c.isAnthropicModel("anthropic.claude-v2"))
	assert.True(t, c.isAnthropicModel("anthropic.claude-instant-v1"))
	assert.True(t, c.isAnthropicLegacyModel("anthropic.claude-v2"))
	assert.True(t, c.isAnthropicLegacyModel("anthropic.claude-v2:1"))
	assert.True(t, c.isAnthropicLegacyModel("anthropic.claude-instant-v1"))
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

type responseShape struct {
	name    string
	body    string
	want    string
	wantErr string
}

func TestBedrockResponseShapes(t *testing.T) {
	tests := []struct {
		model  string
		shapes []responseShape
	}{
		{
			model: "anthropic.claude-3-5-sonnet-20240620-v1:0",
			shapes: []responseShape{
				{name: "no content", body: `{"type":"message"}`, wantErr: "empty content in response"},
				{
					name:    "empty content, with stop_reason",
					body:    `{"type":"message","content":[],"stop_reason":"refusal"}`,
					wantErr: "empty content in response (stop_reason: refusal)",
				},
				{
					name:    "content without text, with stop_reason",
					body:    `{"type":"message","content":[{"type":"tool_use"}],"stop_reason":"tool_use"}`,
					wantErr: "empty content in response (stop_reason: tool_use)",
				},
				{
					name: "thinking before text",
					body: `{"type":"message","content":[{"type":"thinking","thinking":"Let me think"},{"type":"text","text":"John"}]}`,
					want: "John",
				},
				{name: "text with empty value", body: `{"type":"message","content":[{"type":"text","text":""}],"stop_reason":"end_turn"}`, want: ""},
				{name: "text", body: `{"type":"message","content":[{"type":"text","text":"John"}]}`, want: "John"},
			},
		},
		{
			model: "anthropic.claude-v2",
			shapes: []responseShape{
				{name: "no completion", body: `{}`, wantErr: "empty content in response"},
				{
					name:    "no completion, with stop_reason",
					body:    `{"stop_reason":"max_tokens"}`,
					wantErr: "empty content in response (stop_reason: max_tokens)",
				},
				{name: "empty completion", body: `{"completion":"","stop_reason":"stop_sequence"}`, want: ""},
				{name: "completion", body: `{"completion":"John"}`, want: "John"},
			},
		},
		{
			model: "ai21.j2-ultra-v1",
			shapes: []responseShape{
				{name: "no completions", body: `{}`, wantErr: "empty completions in response"},
				{name: "completion without data", body: `{"completions":[{}]}`, wantErr: "empty content in response"},
				{
					name:    "completion without data, with finishReason",
					body:    `{"completions":[{"finishReason":{"reason":"length"}}]}`,
					wantErr: "empty content in response (finishReason: length)",
				},
				{name: "completion with empty text", body: `{"completions":[{"data":{"text":""}}]}`, want: ""},
				{name: "completion with text", body: `{"completions":[{"data":{"text":"John"}}]}`, want: "John"},
			},
		},
		{
			model: "cohere.command-r-v1:0",
			shapes: []responseShape{
				{name: "no text", body: `{}`, wantErr: "empty content in response"},
				{
					name:    "no text, with finish_reason",
					body:    `{"finish_reason":"ERROR_TOXIC"}`,
					wantErr: "empty content in response (finish_reason: ERROR_TOXIC)",
				},
				{name: "empty text", body: `{"text":"","finish_reason":"COMPLETE"}`, want: ""},
				{name: "text", body: `{"text":"John"}`, want: "John"},
			},
		},
		{
			model: "cohere.command-text-v14",
			shapes: []responseShape{
				{name: "no generations", body: `{}`, wantErr: "received empty response from AWS Bedrock"},
				{
					name:    "generation without text, with finish_reason",
					body:    `{"generations":[{"id":"1","finish_reason":"ERROR_TOXIC"}]}`,
					wantErr: "empty content in response (finish_reason: ERROR_TOXIC)",
				},
				{name: "generation with empty text", body: `{"generations":[{"id":"1","text":""}]}`, want: ""},
				{name: "generation with text", body: `{"generations":[{"id":"1","text":"John"}]}`, want: "John"},
			},
		},
		{
			model: "amazon.titan-text-express-v1",
			shapes: []responseShape{
				{name: "no results", body: `{}`, wantErr: "received empty response from AWS Bedrock"},
				{
					name:    "result without outputText, with completionReason",
					body:    `{"results":[{"completionReason":"CONTENT_FILTERED"}]}`,
					wantErr: "empty content in response (completionReason: CONTENT_FILTERED)",
				},
				{name: "result with empty outputText", body: `{"results":[{"outputText":"","completionReason":"FINISH"}]}`, want: ""},
				{name: "result with outputText", body: `{"results":[{"outputText":"John","completionReason":"FINISH"}]}`, want: "John"},
				{name: "result with outputText, without completionReason", body: `{"results":[{"outputText":"John"}]}`, want: "John"},
			},
		},
		{
			model: "mistral.mistral-7b-instruct-v0:2",
			shapes: []responseShape{
				{name: "no outputs", body: `{}`, wantErr: "empty outputs in response"},
				{
					name:    "output without text, with stop_reason",
					body:    `{"outputs":[{"stop_reason":"length"}]}`,
					wantErr: "empty content in response (stop_reason: length)",
				},
				{name: "output with empty text", body: `{"outputs":[{"text":"","stop_reason":"stop"}]}`, want: ""},
				{name: "output with text", body: `{"outputs":[{"text":"John"}]}`, want: "John"},
			},
		},
		{
			model: "meta.llama3-8b-instruct-v1:0",
			shapes: []responseShape{
				{name: "no generation", body: `{}`, wantErr: "empty content in response"},
				{
					name:    "no generation, with stop_reason",
					body:    `{"stop_reason":"length"}`,
					wantErr: "empty content in response (stop_reason: length)",
				},
				{name: "empty generation", body: `{"generation":"","stop_reason":"stop"}`, want: ""},
				{name: "generation", body: `{"generation":"John"}`, want: "John"},
			},
		},
		{
			model: "amazon.nova-micro-v1:0",
			shapes: []responseShape{
				{name: "no output message", body: `{}`, wantErr: "empty message in response"},
				{
					name:    "no output message, with stopReason",
					body:    `{"output":{},"stopReason":"content_filtered"}`,
					wantErr: "empty message in response (stopReason: content_filtered)",
				},
				{
					name:    "message with empty content, with stopReason",
					body:    `{"output":{"message":{"role":"assistant","content":[]}},"stopReason":"content_filtered"}`,
					wantErr: "empty content in response (stopReason: content_filtered)",
				},
				{
					name:    "content without text, with stopReason",
					body:    `{"output":{"message":{"role":"assistant","content":[{"toolUse":{}}]}},"stopReason":"tool_use"}`,
					wantErr: "empty content in response (stopReason: tool_use)",
				},
				{
					name: "reasoning before text",
					body: `{"output":{"message":{"role":"assistant","content":[{"reasoningContent":{"reasoningText":{"text":"Let me think"}}},{"text":"John"}]}}}`,
					want: "John",
				},
				{name: "text with empty value", body: `{"output":{"message":{"role":"assistant","content":[{"text":""}]}},"stopReason":"end_turn"}`, want: ""},
				{name: "text", body: `{"output":{"message":{"role":"assistant","content":[{"text":"John"}]}}}`, want: "John"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			for _, sh := range tt.shapes {
				t.Run(sh.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						assert.Equal(t, "/model/"+tt.model+"/invoke", r.URL.Path)
						w.Header().Set("Content-Type", "application/json")
						w.Write([]byte(sh.body))
					}))
					defer server.Close()
					t.Setenv("AWS_ENDPOINT_URL", server.URL)
					t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
					t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")

					c := New("access", "secret", "", 0, nullLogger())
					params := awsparams.Params{Service: "bedrock", Region: "us-east-1", Model: tt.model}
					props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

					var res *modulecapabilities.GenerateResponse
					var err error
					require.NotPanics(t, func() {
						res, err = c.GenerateAllResults(context.Background(), props, "What is my name?", params, false, nil)
					})
					assertResponseShape(t, sh, res, err)
				})
			}
		})
	}
}

func TestSagemakerResponseShapes(t *testing.T) {
	tests := []responseShape{
		{name: "no generations", body: `{}`, wantErr: "received empty response from AWS Sagemaker"},
		{name: "generation without text", body: `{"generations":[{"id":"1"}]}`, wantErr: "empty content in response"},
		{name: "generation with empty text", body: `{"generations":[{"id":"1","text":""}]}`, want: ""},
		{name: "generation with text", body: `{"generations":[{"id":"1","text":"John"}]}`, want: "John"},
		{name: "generation with text, without id", body: `{"generations":[{"text":"John"}]}`, want: "John"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := New("access", "secret", "", 0, nullLogger())
			c.buildSagemakerUrlFn = func(service, region, endpoint string) string {
				return server.URL
			}
			params := awsparams.Params{Service: "sagemaker", Region: "us-east-1", Endpoint: "my-endpoint"}
			props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

			var res *modulecapabilities.GenerateResponse
			var err error
			require.NotPanics(t, func() {
				res, err = c.GenerateAllResults(context.Background(), props, "What is my name?", params, false, nil)
			})
			assertResponseShape(t, tt, res, err)
		})
	}

	t.Run("debug information", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"generations":[{"id":"1","text":"John"}]}`))
		}))
		defer server.Close()

		c := New("access", "secret", "", 0, nullLogger())
		c.buildSagemakerUrlFn = func(service, region, endpoint string) string {
			return server.URL
		}
		params := awsparams.Params{Service: "sagemaker", Region: "us-east-1", Endpoint: "my-endpoint"}
		props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "My name is john"}}

		res, err := c.GenerateSingleResult(context.Background(), props, "What is my name?", params, true, nil)

		require.NoError(t, err)
		require.NotNil(t, res.Debug)
		assert.Equal(t, "What is my name?", res.Debug.Prompt)
	})
}

func TestGenerateUnsupportedService(t *testing.T) {
	c := New("access", "secret", "", 0, nullLogger())
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

	res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", awsparams.Params{Service: "bedrok"}, false, nil)

	require.EqualError(t, err, "unsupported service: bedrok")
	assert.Nil(t, res)
}

func assertResponseShape(t *testing.T, sh responseShape, res *modulecapabilities.GenerateResponse, err error) {
	t.Helper()
	if sh.wantErr != "" {
		require.EqualError(t, err, sh.wantErr)
		assert.Nil(t, res)
		return
	}
	require.NoError(t, err)
	require.NotNil(t, res.Result)
	assert.Equal(t, sh.want, *res.Result)
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
