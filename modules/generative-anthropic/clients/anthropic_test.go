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
	"github.com/weaviate/weaviate/entities/schema"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGenerate(t *testing.T) {
	properties := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is John"}}}

	tests := []struct {
		name           string
		answer         generateResponse
		timeout        time.Duration
		expectedResult string
	}{
		{
			name: "when the server has a successful answer",
			answer: generateResponse{
				Content: []content{{Text: "John"}},
			},
			expectedResult: "John",
		},
		{
			name: "when the server has an error",
			answer: generateResponse{
				Error: errorMessage{Type: "error", Message: "some error from the server"},
			},
		},
		{
			name:    "when the server does not respond in time",
			answer:  generateResponse{},
			timeout: time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &testAnthropicHandler{
				t:       t,
				answer:  test.answer,
				timeout: test.timeout,
			}
			server := httptest.NewServer(handler)
			defer server.Close()

			a := New("apiKey", test.timeout, nullLogger())

			settings := &fakeClassConfig{baseURL: server.URL}
			res, err := a.GenerateAllResults(context.Background(), properties, "What is my name?", nil, false, settings)

			if len(test.answer.Content) == 0 {
				assert.Error(t, err)
				assert.Nil(t, res)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedResult, *res.Result)
			}
		})
	}

	t.Run("when X-Anthropic-BaseURL header is passed", func(t *testing.T) {
		a := New("apiKey", 5*time.Second, nullLogger())

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Anthropic-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := a.getAnthropicURL(ctxWithValue, baseURL)
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/messages", buildURL)

		buildURL, err = a.getAnthropicURL(context.TODO(), baseURL)
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/v1/messages", buildURL)
	})
}

type testAnthropicHandler struct {
	t       *testing.T
	answer  generateResponse
	timeout time.Duration
}

func (f *testAnthropicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/v1/messages", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	time.Sleep(f.timeout)

	if len(f.answer.Content) == 0 {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b generateInput
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

// Add more methods to implement the moduletools.ClassConfig interface
func (f fakeClassConfig) BaseURL() string {
	return f.baseURL
}

func (f fakeClassConfig) Model() string {
	return "claude-2"
}

func (f fakeClassConfig) MaxTokens() int {
	return 100
}

func (f fakeClassConfig) StopSequences() []string {
	return []string{}
}

func (f fakeClassConfig) Temperature() float64 {
	return 0.7
}

func (f fakeClassConfig) K() int {
	return 0
}

func (f fakeClassConfig) P() float64 {
	return 1.0
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
