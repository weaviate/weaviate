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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	deepseekparams "github.com/weaviate/weaviate/modules/generative-deepseek/parameters"
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
