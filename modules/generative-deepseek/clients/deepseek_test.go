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
	deepseekparams "github.com/weaviate/weaviate/modules/generative-deepseek/parameters"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetApiUrl(t *testing.T) {
	c := New("apiKey", 0, nullLogger())

	t.Run("returns default DeepSeek URL", func(t *testing.T) {
		params := deepseekparams.Params{
			BaseURL: "https://api.deepseek.com",
		}
		url, err := c.getApiUrl(context.Background(), params.BaseURL)
		assert.Nil(t, err)
		assert.Equal(t, "https://api.deepseek.com/chat/completions", url)
	})

	t.Run("returns custom URL from params", func(t *testing.T) {
		params := deepseekparams.Params{
			BaseURL: "https://custom.deepseek.com",
		}
		url, err := c.getApiUrl(context.Background(), params.BaseURL)
		assert.Nil(t, err)
		assert.Equal(t, "https://custom.deepseek.com/chat/completions", url)
	})

	t.Run("returns custom URL from header", func(t *testing.T) {
		params := deepseekparams.Params{
			BaseURL: "https://ignored.com",
		}
		ctx := context.WithValue(context.Background(), "X-Deepseek-Baseurl", []string{"https://header.url"})

		url, err := c.getApiUrl(ctx, params.BaseURL)
		assert.Nil(t, err)
		assert.Equal(t, "https://header.url/chat/completions", url)
	})
}

func TestGetAnswer(t *testing.T) {
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

	t.Run("when the server has a successful answer", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: chatResponse{
				Choices: []choice{{
					FinishReason: "stop",
					Message: message{
						Role:    "assistant",
						Content: "John",
					},
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", time.Minute, nullLogger())

		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		params := deepseekparams.Params{
			BaseURL: server.URL,
		}

		res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", params, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected.Result, res.Result)
	})

	t.Run("when the server has an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: chatResponse{
				Error: &deepSeekError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := New("apiKey", time.Minute, nullLogger())
		params := deepseekparams.Params{
			BaseURL: server.URL,
		}

		_, err := c.GenerateAllResults(context.Background(), props, "What is my name?", params, false, nil)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "connection to: DeepSeek API failed with status: 500")
		assert.Contains(t, err.Error(), "error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer          chatResponse
	headerRequestID string
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/chat/completions", r.URL.Path)
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.answer.Error != nil && f.answer.Error.Message != "" {
		outBytes, err := json.Marshal(f.answer)
		require.Nil(f.t, err)

		if f.headerRequestID != "" {
			w.Header().Add("x-request-id", f.headerRequestID)
		}
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
