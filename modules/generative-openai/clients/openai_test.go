//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func fakeBuildUrl(serverURL string, isLegacy bool, resourceName, deploymentID, baseURL string) (string, error) {
	endpoint, err := buildUrlFn(isLegacy, resourceName, deploymentID, baseURL)
	if err != nil {
		return "", err
	}
	endpoint = strings.Replace(endpoint, "https://api.openai.com", serverURL, 1)
	return endpoint, nil
}

func TestBuildUrlFn(t *testing.T) {
	t.Run("buildUrlFn returns default OpenAI Client", func(t *testing.T) {
		url, err := buildUrlFn(false, "", "", "")
		assert.Nil(t, err)
		assert.Equal(t, "https://api.openai.com/v1/chat/completions", url)
	})
	t.Run("buildUrlFn returns Azure Client", func(t *testing.T) {
		url, err := buildUrlFn(false, "resourceID", "deploymentID", "")
		assert.Nil(t, err)
		assert.Equal(t, "https://resourceID.openai.azure.com/openai/deployments/deploymentID/chat/completions?api-version=2023-03-15-preview", url)
	})

	t.Run("buildUrlFn loads from environment variable", func(t *testing.T) {
		url, err := buildUrlFn(false, "", "", "https://foobar.some.proxy")
		assert.Nil(t, err)
		assert.Equal(t, "https://foobar.some.proxy/v1/chat/completions", url)
		os.Unsetenv("OPENAI_BASE_URL")
	})
}

func TestGetAnswer(t *testing.T) {
	textProperties := []map[string]string{{"prop": "My name is john"}}
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Choices: []choice{{
					FinishReason: "test",
					Index:        0,
					Logprobs:     "",
					Text:         "John",
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("", "openAIApiKey", "", "", 0, nullLogger())
		c.buildUrl = func(isLegacy bool, resourceName, deploymentID, baseURL string) (string, error) {
			return fakeBuildUrl(server.URL, isLegacy, resourceName, deploymentID, baseURL)
		}

		expected := generativemodels.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: generateResponse{
				Error: &openAIApiError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := New("", "openAIApiKey", "", "", 0, nullLogger())
		c.buildUrl = func(isLegacy bool, resourceName, deploymentID, baseURL string) (string, error) {
			return fakeBuildUrl(server.URL, isLegacy, resourceName, deploymentID, baseURL)
		}

		_, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		require.NotNil(t, err)
		assert.Error(t, err, "connection to OpenAI failed with status: 500 error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer generateResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/v1/chat/completions", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

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

func ptString(in string) *string {
	return &in
}
