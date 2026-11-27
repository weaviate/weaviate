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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	deepseekparams "github.com/weaviate/weaviate/modules/generative-deepseek/parameters"
	"github.com/weaviate/weaviate/usecases/config"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestUrlResolution(t *testing.T) {
	c := New("key", 0, nullLogger())

	t.Run("default", func(t *testing.T) {
		u, err := c.url(context.Background(), "https://api.deepseek.com")
		assert.NoError(t, err)
		assert.Equal(t, "https://api.deepseek.com/chat/completions", u)
	})

	t.Run("header override", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "X-Deepseek-Baseurl", []string{"https://override.com"})
		u, err := c.url(ctx, "https://api.deepseek.com")
		assert.NoError(t, err)
		assert.Equal(t, "https://override.com/chat/completions", u)
	})
}

func TestGeneration(t *testing.T) {
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"p": "test"}}}

	t.Run("success", func(t *testing.T) {
		handler := &mockHandler{
			t: t,
			resp: chatResp{
				Choices: []choice{{
					Message: chatMessage{Role: "assistant", Content: "Hello"},
				}},
			},
		}
		srv := httptest.NewServer(handler)
		defer srv.Close()

		c := New("key", time.Minute, nullLogger())
		res, err := c.GenerateAllResults(context.Background(), props, "hi", deepseekparams.Params{BaseURL: srv.URL}, false, nil)

		assert.NoError(t, err)
		assert.Equal(t, "Hello", *res.Result)
	})

	t.Run("error", func(t *testing.T) {
		srv := httptest.NewServer(&mockHandler{
			t:    t,
			resp: chatResp{Error: &apiErr{Message: "fail"}},
		})
		defer srv.Close()

		c := New("key", time.Minute, nullLogger())
		_, err := c.GenerateAllResults(context.Background(), props, "hi", deepseekparams.Params{BaseURL: srv.URL}, false, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fail")
	})
}

type mockHandler struct {
	t    *testing.T
	resp chatResp
}

func (m *mockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.resp.Error != nil {
		w.WriteHeader(500)
	}
	json.NewEncoder(w).Encode(m.resp)
}

// The baseURL a request falls back to without a header (class config or
// per-query parameter) must be validated like the header itself: a plain-HTTP
// one must never be sent the API key and the payload.
func TestGenerateRejectsInsecureBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "value"}}}

	tests := []struct {
		name    string
		options any
		cfg     moduletools.ClassConfig
	}{
		{name: "baseURL from class config", cfg: &fakeClassConfig{baseURL: server.URL}},
		{name: "baseURL from per-query params", options: deepseekparams.Params{BaseURL: server.URL}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New("key", time.Minute, nullLogger()).GenerateAllResults(context.Background(), props, "task", tt.options, false, tt.cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "baseURL must use HTTPS")
			assert.Zero(t, requests.Load(), "no request may reach a non-HTTPS baseURL")
		})
	}
}

type fakeClassConfig struct {
	baseURL string
}

func (f fakeClassConfig) Class() map[string]any { return nil }

func (f fakeClassConfig) ClassByModuleName(string) map[string]any {
	return map[string]any{"baseURL": f.baseURL}
}

func (f fakeClassConfig) Property(string) map[string]any { return nil }

func (f fakeClassConfig) Tenant() string { return "" }

func (f fakeClassConfig) TargetVector() string { return "" }

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }

func (f fakeClassConfig) Config() *config.Config { return nil }
