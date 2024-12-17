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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/modules/generative-zhipuai/config"
	zhipuaiparams "github.com/weaviate/weaviate/modules/generative-zhipuai/parameters"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func fakeBuildUrl(serverURL string, baseURL string) (string, error) {
	endpoint, err := buildUrlFn(baseURL)
	if err != nil {
		return "", err
	}
	endpoint = strings.Replace(endpoint, "https://open.bigmodel.cn", serverURL, 1)
	return endpoint, nil
}

func TestBuildUrlFn(t *testing.T) {
	t.Run("buildUrlFn returns default ZhipuAI Client", func(t *testing.T) {
		url, err := buildUrlFn(config.DefaultZhipuAIBaseURL)
		assert.Nil(t, err)
		assert.Equal(t, "https://open.bigmodel.cn/api/paas/v4/chat/completions", url)
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
					Text:         "John",
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("zhipuAIApiKey", 0, nullLogger())
		c.buildUrl = func(baseURL string) (string, error) {
			return fakeBuildUrl(server.URL, baseURL)
		}

		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: generateResponse{
				Error: &zhipuAIApiError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := New("zhipuAIApiKey",0, nullLogger())
		c.buildUrl = func(baseURL string) (string, error) {
			return fakeBuildUrl(server.URL, baseURL)
		}

		_, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil, false, nil)

		require.NotNil(t, err)
		assert.Error(t, err, "connection to ZhipuAI failed with status: 500 error: some error from the server")
	})

	t.Run("when the server has a an error and request id is present", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: generateResponse{
				Error: &zhipuAIApiError{
					Message: "some error from the server",
				},
			},
			headerRequestID: "some-request-id",
		})
		defer server.Close()

		c := New("zhipuAIApiKey",0, nullLogger())
		c.buildUrl = func(baseURL string) (string, error) {
			return fakeBuildUrl(server.URL, baseURL)
		}

		_, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil, false, nil)

		require.NotNil(t, err)
		assert.Error(t, err, "connection to ZhipuAI failed with status: 500 request-id: some-request-id error: some error from the server")
	})

	t.Run("when X-ZhipuAI-BaseURL header is passed", func(t *testing.T) {
		params := zhipuaiparams.Params{
			BaseURL: "http://default-url.com/api/paas/v4",
		}
		c := New("zhipuAIApiKey",0, nullLogger())
  
		buildURL, err :=  c.buildZhipuAIUrl(context.TODO(), params)
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/api/paas/v4/chat/completions", buildURL)
	})
 
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer          generateResponse
	headerRequestID string
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/api/paas/v4/chat/completions", r.URL.String())
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

func TestZhipuAIApiErrorDecode(t *testing.T) {
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
				name: "Error code as string number",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "500"}`),
				},
				want: "500",
			},
			{
				name: "Error code as string text",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "invalid_api_key"}`),
				},
				want: "invalid_api_key",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got *zhipuAIApiError
				err := json.Unmarshal(tt.args.response, &got)
				require.NoError(t, err)

				if got.Code.String() != tt.want {
					t.Errorf("ZhipuAIerror.code = %v, want %v", got.Code, tt.want)
				}
			})
		}
	})
}

func ptString(in string) *string {
	return &in
}
