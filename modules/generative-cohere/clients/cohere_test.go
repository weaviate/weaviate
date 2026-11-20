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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
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
				Message: responseMessage{
					Content: []responseContent{
						{Text: "John"},
					},
				},
			},
			expectedResult: "John",
		},
		{
			name: "when the server has a an error",
			answer: generateResponse{
				Message: responseMessage{
					ErrorMessage: "some error from the server",
				},
			},
		},
		{
			name: "when the server does not respond in time",
			answer: generateResponse{
				Message: responseMessage{
					ErrorMessage: "context deadline exceeded",
				},
			},
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

			cfg := &fakeClassConfig{baseURL: server.URL}
			res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, cfg)

			if test.answer.Message.ErrorMessage != "" {
				assert.Contains(t, err.Error(), test.answer.Message.GetMessage())
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
		assert.Equal(t, "http://base-url-passed-in-header.com/v2/chat", buildURL)

		buildURL, err = c.getCohereUrl(context.TODO(), baseURL)
		require.NoError(t, err)
		assert.Equal(t, "http://default-url.com/v2/chat", buildURL)
	})

	t.Run("unmarshal error messages", func(t *testing.T) {
		// Error message
		var resp generateResponse
		jsonStr := `{"id":"456","message":"invalid key"}`
		err := json.Unmarshal([]byte(jsonStr), &resp)
		require.NoError(t, err)
		assert.Equal(t, "invalid key", resp.Message.ErrorMessage)
		assert.Empty(t, resp.Message.Content)
		assert.Equal(t, "invalid key", resp.Message.GetMessage())

		jsonStr = `{"id": "c14c80c3-18eb-4519-9460-6c92edd8cfb4","finish_reason": "COMPLETE","message": {"role": "assistant","content": [{"type": "text","text": "Hello"}]}}`
		err = json.Unmarshal([]byte(jsonStr), &resp)
		require.NoError(t, err)
		assert.Empty(t, resp.Message.ErrorMessage)
		require.Len(t, resp.Message.Content, 1)
		assert.Equal(t, "Hello", resp.Message.Content[0].Text)
		assert.Equal(t, "Hello", resp.Message.GetMessage())
	})
}

func TestGenerateResponseShapes(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    string
		wantErr string
	}{
		{name: "no message", body: `{"id":"1"}`, wantErr: "empty content in response"},
		{name: "message without content", body: `{"id":"1","message":{"role":"assistant"}}`, wantErr: "empty content in response"},
		{
			name:    "message with empty content, with finish_reason",
			body:    `{"id":"1","finish_reason":"MAX_TOKENS","message":{"role":"assistant","content":[]}}`,
			wantErr: "empty content in response (finish_reason: MAX_TOKENS)",
		},
		{
			name:    "thinking without text, with finish_reason",
			body:    `{"id":"1","finish_reason":"MAX_TOKENS","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Let me think"}]}}`,
			wantErr: "empty content in response (finish_reason: MAX_TOKENS)",
		},
		{
			name: "thinking before text",
			body: `{"id":"1","finish_reason":"COMPLETE","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Let me think"},{"type":"text","text":"John"}]}}`,
			want: "John",
		},
		{
			name:    "error message with status 200",
			body:    `{"id":"1","message":"invalid request"}`,
			wantErr: "connection to Cohere API failed with status: 200 error: invalid request",
		},
		{
			name: "text with empty value",
			body: `{"id":"1","finish_reason":"COMPLETE","message":{"role":"assistant","content":[{"type":"text","text":""}]}}`,
			want: "",
		},
		{
			name: "text",
			body: `{"id":"1","finish_reason":"COMPLETE","message":{"role":"assistant","content":[{"type":"text","text":"John"}]}}`,
			want: "John",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := New("apiKey", 0, nullLogger())
			cfg := &fakeClassConfig{baseURL: server.URL}
			props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "My name is john"}}

			generators := []struct {
				name     string
				generate func() (*modulecapabilities.GenerateResponse, error)
			}{
				{
					name: "GenerateSingleResult",
					generate: func() (*modulecapabilities.GenerateResponse, error) {
						return c.GenerateSingleResult(context.Background(), props, "What is my name?", nil, false, cfg)
					},
				},
				{
					name: "GenerateAllResults",
					generate: func() (*modulecapabilities.GenerateResponse, error) {
						return c.GenerateAllResults(context.Background(), []*modulecapabilities.GenerateProperties{props}, "What is my name?", nil, false, cfg)
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
	answer  generateResponse
	timeout time.Duration
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/v2/chat", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	time.Sleep(f.timeout)

	if f.answer.Message.ErrorMessage != "" {
		outBytes, err := json.Marshal(f.answer)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]any
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

func (cfg *fakeClassConfig) Class() map[string]any {
	return nil
}

func (cfg *fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	settings := map[string]any{
		"baseURL": cfg.baseURL,
	}
	return settings
}

func (cfg *fakeClassConfig) Property(propName string) map[string]any {
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
