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

func TestGetAnswer(t *testing.T) {
	tests := []struct {
		name           string
		answer         generateResponse
		errorResponse  *generateResponseError
		timeout        time.Duration
		expectedResult string
	}{
		{
			name: "when the server has a successful answer",
			answer: generateResponse{
				Choices: []choice{
					{
						Message: message{
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
			errorResponse: &generateResponseError{
				Status: 402,
				Title:  "Payment Required",
				Detail: "Account 'x': Cloud credits expired - Please contact NVIDIA representatives",
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
				assert.Contains(t, err.Error(), tt.errorResponse.Title, tt.errorResponse.Detail)
			} else {
				assert.Equal(t, tt.expectedResult, *res.Result)
			}
		})
	}
	t.Run("when X-Nvidia-BaseURL header is passed", func(t *testing.T) {
		c := New("apiKey", 5*time.Second, nullLogger())
		baseUrl := "https://integrate.api.nvidia.com"

		ctxWithValue := context.WithValue(context.Background(),
			"X-Nvidia-BaseURL", []string{"https://integrate.api.nvidia.com"})
		buildURL := c.getNvidiaUrl(ctxWithValue, baseUrl)
		assert.Equal(t, "https://integrate.api.nvidia.com/v1/chat/completions", buildURL)

		buildURL = c.getNvidiaUrl(context.Background(), baseUrl)
		assert.Equal(t, "https://integrate.api.nvidia.com/v1/chat/completions", buildURL)
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer        generateResponse
	errorResponse *generateResponseError
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

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
