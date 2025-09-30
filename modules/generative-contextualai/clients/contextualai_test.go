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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

const (
	baseURLKey        contextKey = "X-Contextual-Baseurl"
	testAPIKey        string     = "test-key"
	contentTypeHeader string     = "Content-Type"
	applicationJSON   string     = "application/json"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGenerateAllResults(t *testing.T) {
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is John"}}}

	tests := []struct {
		name           string
		response       generateResponse
		statusCode     int
		timeout        time.Duration
		expectedResult string
		expectError    bool
	}{
		{
			name: "successful response",
			response: generateResponse{
				Response: "John",
			},
			statusCode:     200,
			expectedResult: "John",
		},
		{
			name: "error response with message",
			response: generateResponse{
				Response: "",
			},
			statusCode:  422,
			expectError: true,
		},
		{
			name: "timeout",
			response: generateResponse{
				Response: "delayed response",
			},
			statusCode:  200,
			timeout:     1 * time.Millisecond,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(t, tt.statusCode, tt.timeout, tt.response)
			defer server.Close()

			timeout := getTestTimeout(tt.timeout)
			c := New(testAPIKey, timeout, nullLogger())
			cfg := fakeClassConfig{classConfig: map[string]interface{}{}}
			ctx := context.WithValue(context.Background(), baseURLKey, []string{server.URL})

			res, err := c.GenerateAllResults(ctx, props, "What is my name?", nil, false, cfg)

			assertTestResult(t, tt.expectError, tt.expectedResult, res, err)
		})
	}
}

func createTestServer(t *testing.T, statusCode int, timeout time.Duration, response generateResponse) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if timeout > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		assert.Equal(t, "/v1/generate", r.URL.Path)
		assert.Equal(t, applicationJSON, r.Header.Get(contentTypeHeader))
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer")

		w.Header().Set(contentTypeHeader, applicationJSON)
		w.WriteHeader(statusCode)

		if statusCode == 200 {
			json.NewEncoder(w).Encode(response)
		} else {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"detail": "validation error",
			})
		}
	}))
}

func getTestTimeout(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	return 30 * time.Second
}

func assertTestResult(t *testing.T, expectError bool, expectedResult string, res *modulecapabilities.GenerateResponse, err error) {
	if expectError {
		assert.Error(t, err)
	} else {
		require.NoError(t, err)
		assert.Equal(t, expectedResult, *res.Result)
	}
}

func TestGenerateSingleResult(t *testing.T) {
	prop := &modulecapabilities.GenerateProperties{Text: map[string]string{"content": "Hello world"}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/generate", r.URL.Path)

		var reqBody generateInput
		bodyBytes, _ := io.ReadAll(r.Body)
		json.Unmarshal(bodyBytes, &reqBody)

		assert.Len(t, reqBody.Messages, 1)
		assert.Equal(t, "user", reqBody.Messages[0].Role)

		w.Header().Set(contentTypeHeader, applicationJSON)
		json.NewEncoder(w).Encode(generateResponse{
			Response: "Generated response",
		})
	}))
	defer server.Close()

	c := New(testAPIKey, 30*time.Second, nullLogger())
	cfg := fakeClassConfig{classConfig: map[string]interface{}{}}

	ctx := context.WithValue(context.Background(), baseURLKey, []string{server.URL})
	res, err := c.GenerateSingleResult(ctx, prop, "test prompt", nil, false, cfg)

	require.NoError(t, err)
	assert.Equal(t, "Generated response", *res.Result)
}

func TestClientGetParameters(t *testing.T) {
	fakeConfig := fakeClassConfig{
		classConfig: map[string]interface{}{
			"model":       "v1",
			"temperature": 0.5,
		},
	}

	client := &contextualai{
		apiKey:     testAPIKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     logrus.New(),
	}

	params := client.getParameters(fakeConfig, nil)

	assert.Equal(t, "v1", params.Model)
	assert.Equal(t, 0.5, *params.Temperature)
}

func TestClientMetaInfo(t *testing.T) {
	client := &contextualai{}

	meta, err := client.MetaInfo()
	require.NoError(t, err)

	assert.Equal(t, "Generative Search - Contextual AI", meta["name"])
	assert.Equal(t, "https://contextual.ai/generate/", meta["documentationHref"])
}

func TestAPIErrorHandling(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		responseBody map[string]interface{}
		expectedErr  string
	}{
		{
			name:       "error with message field",
			statusCode: 422,
			responseBody: map[string]interface{}{
				"message": "Invalid model specified",
			},
			expectedErr: "Invalid model specified",
		},
		{
			name:       "error with detail field",
			statusCode: 422,
			responseBody: map[string]interface{}{
				"detail": []map[string]interface{}{
					{
						"loc":  []string{"body", "model"},
						"msg":  "Validation failed",
						"type": "value_error",
					},
				},
			},
			expectedErr: "Validation failed",
		},
		{
			name:       "error without message",
			statusCode: 500,
			responseBody: map[string]interface{}{
				"error": "Internal server error",
			},
			expectedErr: "500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(contentTypeHeader, applicationJSON)
				w.WriteHeader(tt.statusCode)
				json.NewEncoder(w).Encode(tt.responseBody)
			}))
			defer server.Close()

			c := New(testAPIKey, 30*time.Second, nullLogger())
			cfg := fakeClassConfig{classConfig: map[string]interface{}{}}
			ctx := context.WithValue(context.Background(), baseURLKey, []string{server.URL})

			prop := &modulecapabilities.GenerateProperties{Text: map[string]string{"content": "test"}}
			_, err := c.GenerateSingleResult(ctx, prop, "test prompt", nil, false, cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}
