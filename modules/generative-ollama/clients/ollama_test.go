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

package ollama

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

func TestGetAnswer(t *testing.T) {
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

	tests := []struct {
		name           string
		answer         generateResponse
		timeout        time.Duration
		expectedResult string
	}{
		{
			name: "when the server has a successful aner",
			answer: generateResponse{
				Response: "Test test",
			},
			expectedResult: "Test test",
		},
		{
			name: "when the server has a an error",
			answer: generateResponse{
				Error: "some error from the server",
			},
		},
		{
			name:    "when the server does not respond in time",
			answer:  generateResponse{Error: "context deadline exceeded"},
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

			c := New(test.timeout, nullLogger())

			settings := &fakeClassConfig{apiEndpoint: server.URL}
			res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, settings)

			if test.answer.Error != "" {
				assert.Contains(t, err.Error(), test.answer.Error)
			} else {
				assert.Equal(t, test.expectedResult, *res.Result)
			}
		})
	}
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer  generateResponse
	timeout time.Duration
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/api/generate", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	time.Sleep(f.timeout)

	if f.answer.Error != "" {
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
	apiEndpoint string
}

func (cfg *fakeClassConfig) Tenant() string {
	return ""
}

func (cfg *fakeClassConfig) Class() map[string]interface{} {
	return nil
}

func (cfg *fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	settings := map[string]interface{}{
		"apiEndpoint": cfg.apiEndpoint,
	}
	return settings
}

func (cfg *fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}

func (cfg *fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
