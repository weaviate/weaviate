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
	metaparams "github.com/weaviate/weaviate/modules/generative-meta/parameters"
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
			name:      "default base url",
			baseURL:   "https://api.meta.ai",
			expectedU: "https://api.meta.ai/v1/chat/completions",
		},
		{
			name:      "base url with trailing slash",
			baseURL:   "https://api.meta.ai/",
			expectedU: "https://api.meta.ai/v1/chat/completions",
		},
		{
			name:      "header overrides baseURL",
			baseURL:   "https://api.meta.ai",
			header:    "https://proxy.meta.ai",
			expectedU: "https://proxy.meta.ai/v1/chat/completions",
		},
	}

	c := New("key", 0, nullLogger())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, "X-Meta-Baseurl", []string{tt.header})
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
				ctx = context.WithValue(ctx, "X-Meta-Api-Key", []string{tt.headerKey})
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
	properties := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "value"}}

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
			name:        "error payload",
			statusCode:  http.StatusBadRequest,
			body:        `{"error":{"message":"model not found","type":"invalid_request_error","code":"model_not_found"}}`,
			expectedErr: "connection to Meta API failed with status: 400 error: model not found",
		},
		{
			name:        "error payload on a 200 status",
			statusCode:  http.StatusOK,
			body:        `{"error":{"message":"rate limited"}}`,
			expectedErr: "connection to Meta API failed with status: 200 error: rate limited",
		},
		{
			name:        "error status without payload",
			statusCode:  http.StatusUnauthorized,
			body:        `{}`,
			expectedErr: "connection to Meta API failed with status: 401",
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
			res, err := c.GenerateSingleResult(context.Background(), properties, "prompt",
				metaparams.Params{BaseURL: server.URL}, false, nil)

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
	earth, mars := "earth-base64", "mars-base64"
	textProps := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "value"}}}
	imageProps := []*modulecapabilities.GenerateProperties{
		{Text: map[string]string{"prop": "value"}, Blob: map[string]*string{"image": &earth}},
	}
	textContent := `task: [{"prop":"value"}]`

	tests := []struct {
		name            string
		properties      []*modulecapabilities.GenerateProperties
		params          metaparams.Params
		expectedPayload map[string]any
	}{
		{
			name:       "defaults are filled in from the class settings",
			properties: textProps,
			params:     metaparams.Params{},
			expectedPayload: map[string]any{
				"model":    "muse-spark-1.2",
				"messages": []any{map[string]any{"role": "user", "content": textContent}},
			},
		},
		{
			name:       "all parameters are forwarded",
			properties: textProps,
			params: metaparams.Params{
				Model:            "muse-spark-1.1",
				Temperature:      ptr(0.5),
				TopP:             ptr(0.9),
				MaxTokens:        ptr(512),
				FrequencyPenalty: ptr(1.5),
				PresencePenalty:  ptr(-1.5),
				ReasoningEffort:  ptr("xhigh"),
			},
			expectedPayload: map[string]any{
				"model":                 "muse-spark-1.1",
				"messages":              []any{map[string]any{"role": "user", "content": textContent}},
				"temperature":           0.5,
				"top_p":                 0.9,
				"max_completion_tokens": float64(512),
				"frequency_penalty":     1.5,
				"presence_penalty":      -1.5,
				"reasoning_effort":      "xhigh",
			},
		},
		{
			name:       "server stored images are sent as content parts",
			properties: imageProps,
			params:     metaparams.Params{ImageProperties: []string{"image"}},
			expectedPayload: map[string]any{
				"model": "muse-spark-1.2",
				"messages": []any{map[string]any{"role": "user", "content": []any{
					map[string]any{"type": "text", "text": textContent},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64," + earth}},
				}}},
			},
		},
		{
			name:       "user provided images are appended after the stored ones",
			properties: imageProps,
			params:     metaparams.Params{ImageProperties: []string{"image"}, Images: []*string{&mars}},
			expectedPayload: map[string]any{
				"model": "muse-spark-1.2",
				"messages": []any{map[string]any{"role": "user", "content": []any{
					map[string]any{"type": "text", "text": textContent},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64," + earth}},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64," + mars}},
				}}},
			},
		},
		{
			name:       "an image property the object does not have is skipped",
			properties: imageProps,
			params:     metaparams.Params{ImageProperties: []string{"thumbnail"}},
			expectedPayload: map[string]any{
				"model":    "muse-spark-1.2",
				"messages": []any{map[string]any{"role": "user", "content": textContent}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotAuth string
			var gotPayload map[string]any

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
				body, _ := io.ReadAll(r.Body)
				require.NoError(t, json.Unmarshal(body, &gotPayload))
				w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
			}))
			defer server.Close()

			params := tt.params
			params.BaseURL = server.URL
			c := New("key", time.Minute, nullLogger())
			_, err := c.GenerateAllResults(context.Background(), tt.properties, "task", params, false, nil)
			require.NoError(t, err)

			assert.Equal(t, "/v1/chat/completions", gotPath)
			assert.Equal(t, "Bearer key", gotAuth)
			assert.Equal(t, tt.expectedPayload, gotPayload)
		})
	}
}

func TestGenerateRejectsUnknownReasoningEffort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("request must not reach the API")
	}))
	defer server.Close()

	properties := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "value"}}
	c := New("key", time.Minute, nullLogger())
	_, err := c.GenerateSingleResult(context.Background(), properties, "prompt",
		metaparams.Params{BaseURL: server.URL, ReasoningEffort: ptr("extreme")}, false, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong reasoningEffort configuration")
}

func TestMetaInfo(t *testing.T) {
	meta, err := New("key", 0, nullLogger()).MetaInfo()
	require.NoError(t, err)
	assert.Equal(t, "Generative Search - Meta", meta["name"])
	assert.Contains(t, meta["documentationHref"], "dev.meta.ai")
}

func ptr[T any](v T) *T {
	return &v
}
