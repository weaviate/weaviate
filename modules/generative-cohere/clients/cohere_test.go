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
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	textProperties := []map[string]string{{"prop": "My name is john"}}

	tests := []struct {
		name           string
		answer         generateResponse
		timeout        time.Duration
		expectedResult string
	}{
		{
			name: "when the server has a successful aner",
			answer: generateResponse{
				Generations: []generation{{Text: "John"}},
				Error:       nil,
			},
			expectedResult: "John",
		},
		{
			name: "when the server has a an error",
			answer: generateResponse{
				Error: &cohereApiError{
					Message: "some error from the server",
				},
			},
		},
		{
			name:    "when the server does not respond in time",
			answer:  generateResponse{Error: &cohereApiError{Message: "context deadline exceeded"}},
			timeout: time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &testAnswerHandler{
				t:       t,
				answer:  test.answer,
				timeout: test.timeout,
			}
			server := httptest.NewServer(handler)
			defer server.Close()

			c := New("apiKey", test.timeout, nullLogger())

			settings := &fakeClassConfig{baseURL: server.URL}
			res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", settings)

			if test.answer.Error != nil {
				assert.Contains(t, err.Error(), test.answer.Error.Message)
			} else {
				assert.Equal(t, test.expectedResult, *res.Result)
			}
		})
	}

	t.Run("when X-Cohere-BaseURL header is passed", func(t *testing.T) {
		c := New("apiKey", 5*time.Second, nullLogger())

		baseURL := "http://default-url.com"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Cohere-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.getCohereUrl(ctxWithValue, baseURL)
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/generate", buildURL)

		buildURL, err = c.getCohereUrl(context.TODO(), baseURL)
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/v1/generate", buildURL)
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer  generateResponse
	timeout time.Duration
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/v1/generate", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	time.Sleep(f.timeout)

	if f.answer.Error != nil && f.answer.Error.Message != "" {
		outBytes, err := json.Marshal(f.answer)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
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
