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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/qna-openai/ent"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: answersResponse{
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

		c := New("openAIApiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string) (string, error) {
			return buildUrl(server.URL, resourceName, deploymentID)
		}

		expected := ent.AnswerResult{
			Text:     "My name is John",
			Question: "What is my name?",
			Answer:   ptString("John"),
		}

		res, err := c.Answer(context.Background(), "My name is John", "What is my name?", nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: answersResponse{
				Error: &openAIApiError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := New("openAIApiKey", "", "", 0, nullLogger())
		c.buildUrlFn = func(baseURL, resourceName, deploymentID string) (string, error) {
			return buildUrl(server.URL, resourceName, deploymentID)
		}

		_, err := c.Answer(context.Background(), "My name is John", "What is my name?", nil)

		require.NotNil(t, err)
		assert.Error(t, err, "connection to OpenAI failed with status: 500 error: some error from the server")
	})

	t.Run("when X-OpenAI-BaseURL header is passed", func(t *testing.T) {
		c := New("openAIApiKey", "", "", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Baseurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.buildOpenAIUrl(ctxWithValue, "http://default-url.com", "", "")
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com/v1/completions", buildURL)

		buildURL, err = c.buildOpenAIUrl(context.TODO(), "http://default-url.com", "", "")
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/v1/completions", buildURL)
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer answersResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/v1/completions", r.URL.String())
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

func TestOpenAIApiErrorDecode(t *testing.T) {
	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			response []byte
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Error code: missing property",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg..."}`),
				},
				want: "",
			},
			{
				name: "Error code: as int",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": 500}`),
				},
				want: "500",
			},
			{
				name: "Error code as string",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "500"}`),
				},
				want: "500",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got *openAIApiError
				err := json.Unmarshal(tt.args.response, &got)
				require.NoError(t, err)

				if got.Code.String() != tt.want {
					t.Errorf("OpenAIerror.code = %v, want %v", got.Code, tt.want)
				}
			})
		}
	})
}

func ptString(in string) *string {
	return &in
}
