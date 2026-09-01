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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	digitaloceanparams "github.com/weaviate/weaviate/modules/generative-digitalocean/parameters"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestURL(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		header    string
		expectedU string
	}{
		{
			name:      "serverless inference",
			baseURL:   "https://inference.do-ai.run",
			expectedU: "https://inference.do-ai.run/v1/chat/completions",
		},
		{
			name:      "serverless inference with trailing slash",
			baseURL:   "https://inference.do-ai.run/",
			expectedU: "https://inference.do-ai.run/v1/chat/completions",
		},
		{
			name:      "header overrides baseURL",
			baseURL:   "https://inference.do-ai.run",
			header:    "https://override.do-ai.run",
			expectedU: "https://override.do-ai.run/v1/chat/completions",
		},
	}

	c := New("key", 0, nullLogger())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Digitalocean-Baseurl", []string{tt.header})
			}
			u, err := c.url(ctx, tt.baseURL)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedU, u)
		})
	}
}

func TestAPIKey(t *testing.T) {
	tests := []struct {
		name        string
		envKey      string
		headerKey   string
		expectedKey string
		expectedErr string
	}{
		{name: "from environment", envKey: "env-key", expectedKey: "env-key"},
		{name: "header wins over environment", envKey: "env-key", headerKey: "header-key", expectedKey: "header-key"},
		{name: "header only", headerKey: "header-key", expectedKey: "header-key"},
		{name: "no key at all", expectedErr: "no api key found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.headerKey != "" {
				ctx = context.WithValue(ctx, "X-Digitalocean-Api-Key", []string{tt.headerKey})
			}
			key, err := New(tt.envKey, 0, nullLogger()).apiKeyFromContext(ctx)
			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedKey, key)
		})
	}
}

func TestGenerate(t *testing.T) {
	properties := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "value"}}}

	tests := []struct {
		name           string
		statusCode     int
		body           string
		expectedResult string
		expectedUsage  *usage
		expectedErr    string
	}{
		{
			name:       "success",
			statusCode: http.StatusOK,
			body: `{"choices":[{"message":{"role":"assistant","content":" hello "}}],
				"usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}`,
			expectedResult: "hello",
			expectedUsage:  &usage{PromptTokens: ptr(11), CompletionTokens: ptr(22), TotalTokens: ptr(33)},
		},
		{
			name:           "success without usage",
			statusCode:     http.StatusOK,
			body:           `{"choices":[{"message":{"role":"assistant","content":"hello"}}]}`,
			expectedResult: "hello",
		},
		{
			name:        "top level error payload",
			statusCode:  http.StatusUnauthorized,
			body:        `{"id": "Unauthorized", "message": "Unable to authenticate you" }`,
			expectedErr: "connection to DigitalOcean API failed with status: 401 Unauthorized error: Unable to authenticate you",
		},
		{
			name:        "top level error payload with request id",
			statusCode:  http.StatusPaymentRequired,
			body:        `{"id":"Payment Required","message":"You are not allowed to perform this operation","request_id":"abc-123"}`,
			expectedErr: "connection to DigitalOcean API failed with status: 402 Payment Required error: You are not allowed to perform this operation request_id: abc-123",
		},
		{
			name:        "wrapped error payload",
			statusCode:  http.StatusBadRequest,
			body:        `{"error":{"message":"model not found","type":"invalid_request_error"}}`,
			expectedErr: "connection to DigitalOcean API failed with status: 400 error: model not found",
		},
		{
			name:        "wrapped error payload does not report the completion id as a status",
			statusCode:  http.StatusBadRequest,
			body:        `{"id":"chatcmpl-abc","error":{"message":"model not found"}}`,
			expectedErr: "connection to DigitalOcean API failed with status: 400 error: model not found",
		},
		{
			name:        "error status without payload",
			statusCode:  http.StatusUnauthorized,
			body:        `{}`,
			expectedErr: "connection to DigitalOcean API failed with status: 401",
		},
		{
			name:        "unparsable body",
			statusCode:  http.StatusOK,
			body:        `not json`,
			expectedErr: "unmarshal response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := New("key", time.Minute, nullLogger())
			res, err := c.GenerateSingleResult(context.Background(), properties[0], "prompt",
				digitaloceanparams.Params{BaseURL: server.URL}, false, nil)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, res.Result)
			assert.Equal(t, tt.expectedResult, *res.Result)
			var gotUsage *usage
			if params := GetResponseParams(res.Params); params != nil {
				gotUsage = params.Usage
			}
			assert.Equal(t, tt.expectedUsage, gotUsage)
		})
	}
}

func TestGenerateRequest(t *testing.T) {
	tests := []struct {
		name            string
		params          digitaloceanparams.Params
		expectedPath    string
		expectedQuery   string
		expectedPayload map[string]any
	}{
		{
			name:         "defaults are filled in from the class settings",
			params:       digitaloceanparams.Params{},
			expectedPath: "/v1/chat/completions",
			expectedPayload: map[string]any{
				"model":    "llama-4-maverick",
				"messages": []any{map[string]any{"role": "user", "content": `task: [{"prop":"value"}]`}},
			},
		},
		{
			name: "all parameters are forwarded",
			params: digitaloceanparams.Params{
				Model:            "openai-gpt-4o",
				Temperature:      ptr(0.5),
				TopP:             ptr(0.9),
				MaxTokens:        ptr(512),
				FrequencyPenalty: ptr(1.5),
				PresencePenalty:  ptr(-1.5),
				Stop:             []string{"END"},
			},
			expectedPath: "/v1/chat/completions",
			expectedPayload: map[string]any{
				"model":                 "openai-gpt-4o",
				"messages":              []any{map[string]any{"role": "user", "content": `task: [{"prop":"value"}]`}},
				"temperature":           0.5,
				"top_p":                 0.9,
				"max_completion_tokens": float64(512),
				"frequency_penalty":     1.5,
				"presence_penalty":      -1.5,
				"stop":                  []any{"END"},
			},
		},
	}

	properties := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "value"}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotQuery, gotAuth string
			var gotPayload map[string]any

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotQuery, gotAuth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
				body, _ := io.ReadAll(r.Body)
				require.NoError(t, json.Unmarshal(body, &gotPayload))
				w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
			}))
			defer server.Close()

			params := tt.params
			params.BaseURL = server.URL
			c := New("key", time.Minute, nullLogger())
			_, err := c.GenerateAllResults(context.Background(), properties, "task", params, false, nil)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedPath, gotPath)
			assert.Equal(t, tt.expectedQuery, gotQuery)
			assert.Equal(t, "Bearer key", gotAuth)
			assert.Equal(t, tt.expectedPayload, gotPayload)
		})
	}
}

func TestMetaInfo(t *testing.T) {
	meta, err := New("key", 0, nullLogger()).MetaInfo()
	require.NoError(t, err)
	assert.Equal(t, "Generative Search - DigitalOcean", meta["name"])
	assert.Contains(t, meta["documentationHref"], "docs.digitalocean.com")
}

func ptr[T any](v T) *T {
	return &v
}
