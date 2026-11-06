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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func fakeBuildUrl(serverURL string) (string, error) {
	return serverURL, nil
}

func TestGetAnswer(t *testing.T) {
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Choices: []choice{{
					FinishReason: "test",
					Index:        0,
					Text:         ptString("John"),
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("databricksToken", 0, nullLogger())
		c.buildEndpoint = func(endpoint string) (string, error) {
			return fakeBuildUrl(server.URL)
		}

		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when X-Databricks-Endpoint header is passed", func(t *testing.T) {
		c := New("databricksToken", 0, nullLogger())

		endpoint := "http://default"
		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Endpoint", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.buildDatabricksEndpoint(ctxWithValue, endpoint)
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com", buildURL)

		buildURL, err = c.buildDatabricksEndpoint(context.TODO(), endpoint)
		require.NoError(t, err)
		assert.Equal(t, "http://default", buildURL)

		buildURL, err = c.buildDatabricksEndpoint(context.TODO(), "")
		require.Error(t, err)
		assert.ErrorContains(t, err, "endpoint cannot be empty")
		assert.Equal(t, "", buildURL)
	})

	t.Run("when X-Databricks-User-Agent header is passed", func(t *testing.T) {
		userAgent := "weaviate+spark_connector"
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Choices: []choice{{
					FinishReason: "test",
					Index:        0,
					Text:         ptString("John"),
				}},
				Error: nil,
			},
			userAgent: userAgent,
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("databricksToken", 0, nullLogger())
		c.buildEndpoint = func(endpoint string) (string, error) {
			return fakeBuildUrl(server.URL)
		}

		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-User-Agent", []string{userAgent})

		res, err := c.GenerateAllResults(ctxWithValue, props, "What is my name?", nil, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})
}

func TestDatabricksApiErrorDecode(t *testing.T) {
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
				name: "Error code as string text",
				args: args{
					response: []byte(`{"message": "Human-readable error message.", "error_code": "Error code"}`),
				},
				want: "Error code",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got *databricksApiError
				err := json.Unmarshal(tt.args.response, &got)
				require.NoError(t, err)

				if got.ErrorCode.String() != tt.want {
					t.Errorf("databricksApiError.ErrorCode = %v, want %v", got.ErrorCode, tt.want)
				}
			})
		}
	})
}

func TestGenerateResponseShapes(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    string
		wantErr string
	}{
		{name: "empty choices", body: `{"choices":[]}`, wantErr: "empty choices in response"},
		{name: "no choices", body: `{}`, wantErr: "empty choices in response"},
		{name: "choice without text or message", body: `{"choices":[{}]}`, wantErr: "empty message in response"},
		{
			name:    "choice without text or message, with finish_reason",
			body:    `{"choices":[{"finish_reason":"content_filter"}]}`,
			wantErr: "empty message in response (finish_reason: content_filter)",
		},
		{
			name:    "message without content, with finish_reason",
			body:    `{"choices":[{"finish_reason":"content_filter","message":{"role":"assistant"}}]}`,
			wantErr: "empty content in response (finish_reason: content_filter)",
		},
		{
			name:    "message with null content",
			body:    `{"choices":[{"message":{"role":"assistant","content":null}}]}`,
			wantErr: "empty content in response",
		},
		{name: "message with empty content", body: `{"choices":[{"message":{"role":"assistant","content":""}}]}`, want: ""},
		{name: "message with content", body: `{"choices":[{"message":{"role":"assistant","content":"John"}}]}`, want: "John"},
		{
			name: "message with content and logprobs object",
			body: `{"choices":[{"message":{"role":"assistant","content":"John"},"logprobs":{"content":[{"token":"John","logprob":-0.1}]}}]}`,
			want: "John",
		},
		{name: "completion with empty text", body: `{"choices":[{"text":"","finish_reason":"stop"}]}`, want: ""},
		{name: "completion with text", body: `{"choices":[{"text":"John"}]}`, want: "John"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := New("databricksToken", 0, nullLogger())
			c.buildEndpoint = func(endpoint string) (string, error) {
				return fakeBuildUrl(server.URL)
			}
			props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "My name is john"}}

			generators := []struct {
				name     string
				generate func() (*modulecapabilities.GenerateResponse, error)
			}{
				{
					name: "GenerateSingleResult",
					generate: func() (*modulecapabilities.GenerateResponse, error) {
						return c.GenerateSingleResult(context.Background(), props, "What is my name?", nil, false, nil)
					},
				},
				{
					name: "GenerateAllResults",
					generate: func() (*modulecapabilities.GenerateResponse, error) {
						return c.GenerateAllResults(context.Background(), []*modulecapabilities.GenerateProperties{props}, "What is my name?", nil, false, nil)
					},
				},
			}
			for _, gen := range generators {
				t.Run(gen.name, func(t *testing.T) {
					var res *modulecapabilities.GenerateResponse
					var err error
					require.NotPanics(t, func() { res, err = gen.generate() })
					if tt.wantErr != "" {
						require.EqualError(t, err, tt.wantErr)
						assert.Nil(t, res)
						return
					}
					require.NoError(t, err)
					require.NotNil(t, res.Result)
					assert.Equal(t, tt.want, *res.Result)
				})
			}
		})
	}
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer    generateResponse
	userAgent string
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.userAgent != "" {
		assert.Equal(f.t, f.userAgent, r.UserAgent())
	}
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
