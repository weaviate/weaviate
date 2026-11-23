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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	xaiparams "github.com/weaviate/weaviate/modules/generative-xai/parameters"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestGetAnswer(t *testing.T) {
	tests := []struct {
		name           string
		answer         generateResponse
		errorResponse  *generateResponse
		timeout        time.Duration
		expectedResult string
	}{
		{
			name: "when the server has a successful answer",
			answer: generateResponse{
				Choices: []choice{
					{
						Message: &responseMessage{
							Role:    "user",
							Content: "John",
						},
					},
				},
			},
			expectedResult: "John",
		},
		{
			name: "when the server has an error",
			errorResponse: &generateResponse{
				Error: &openAIApiError{
					Message: "Account 'x': Cloud credits expired - Please contact xAI representatives",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &testAnswerHandler{
				t:             t,
				answer:        tt.answer,
				errorResponse: tt.errorResponse,
				timeout:       tt.timeout,
			}
			server := httptest.NewServer(handler)
			defer server.Close()

			c := New("apiKey", tt.timeout, nullLogger())

			settings := &fakeClassConfig{baseURL: server.URL}
			props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}
			res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, settings)

			if tt.errorResponse != nil {
				assert.Contains(t, err.Error(), tt.errorResponse.Error.Message)
			} else {
				assert.Equal(t, tt.expectedResult, *res.Result)
			}
		})
	}
	t.Run("when X-Xai-BaseURL header is passed", func(t *testing.T) {
		c := New("apiKey", 5*time.Second, nullLogger())
		baseUrl := "https://integrate.api.xai.com"

		ctxWithValue := context.WithValue(context.Background(),
			"X-Xai-BaseURL", []string{"https://integrate.api.xai.com"})
		buildURL, err := c.getXaiUrl(ctxWithValue, baseUrl)
		assert.NoError(t, err)
		assert.Equal(t, "https://integrate.api.xai.com/v1/chat/completions", buildURL)

		buildURL, err = c.getXaiUrl(context.Background(), baseUrl)
		assert.NoError(t, err)
		assert.Equal(t, "https://integrate.api.xai.com/v1/chat/completions", buildURL)
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer        generateResponse
	errorResponse *generateResponse
	timeout       time.Duration
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	time.Sleep(f.timeout)

	if f.errorResponse != nil {
		outBytes, err := json.Marshal(f.errorResponse)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusPaymentRequired)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	outBytes, err := json.Marshal(f.answer)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

type fakeClassConfig struct {
	baseURL string
}

func (cfg *fakeClassConfig) Tenant() string {
	return ""
}

func (cfg *fakeClassConfig) Class() map[string]interface{} {
	return nil
}

func (cfg *fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	settings := map[string]interface{}{
		"baseURL": cfg.baseURL,
	}
	return settings
}

func (cfg *fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

// The baseURL a request falls back to without a header (class config or
// per-query parameter) must be validated like the header itself: a plain-HTTP
// one must never be sent the API key and the payload.
func TestGenerateRejectsInsecureBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "value"}}}

	tests := []struct {
		name    string
		options any
		cfg     moduletools.ClassConfig
	}{
		{name: "baseURL from class config", cfg: &fakeClassConfig{baseURL: server.URL}},
		{name: "baseURL from per-query params", options: xaiparams.Params{BaseURL: server.URL}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("apiKey", time.Minute, nullLogger()).GenerateAllResults(context.Background(), props, "task", tt.options, false, tt.cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "baseURL must use HTTPS")
			assert.Zero(t, requests.Load(), "no request may reach a non-HTTPS baseURL")
		})
	}
}
