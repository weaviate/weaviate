//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/modules/text2vec-openai/ent"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()

		c := New("apiKEy", nullLogger())
		c.host = server.URL

		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("apiKEy", nullLogger())
		c.host = server.URL
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := New("apiKEy", nullLogger())
		c.host = server.URL
		_, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when OpenAI key is passed using X-Openai-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", nullLogger())
		c.host = server.URL
		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when OpenAI key is empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", nullLogger())
		c.host = server.URL
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "OpenAI API Key: no api key found "+
			"neither in request header: X-OpenAI-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})

	t.Run("when X-Openai-Api-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := New("", nullLogger())
		c.host = server.URL

		ctxWithValue := context.WithValue(context.Background(),
			"X-Openai-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Type:  "text",
				Model: "ada",
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "OpenAI API Key: no api key found "+
			"neither in request header: X-OpenAI-Api-Key "+
			"nor in environment variable under OPENAI_APIKEY")
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		embeddingError := map[string]interface{}{
			"message": f.serverError.Error(),
			"type":    "invalid_request_error",
		}
		embedding := map[string]interface{}{
			"error": embeddingError,
		}
		outBytes, err := json.Marshal(embedding)
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

	textInput := b["input"].(string)
	assert.Greater(f.t, len(textInput), 0)

	embeddingData := map[string]interface{}{
		"object":    "embedding",
		"index":     0,
		"embedding": []float32{0.1, 0.2, 0.3},
	}
	embedding := map[string]interface{}{
		"object": "list",
		"data":   []interface{}{embeddingData},
	}

	outBytes, err := json.Marshal(embedding)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func Test_getModelString(t *testing.T) {
	t.Run("getModelStringDocument", func(t *testing.T) {
		type args struct {
			docType string
			model   string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: document",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-doc-001",
			},
			{
				name: "Document type: text model: babbage vectorizationType: document",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-doc-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: document",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-doc-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: document",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-doc-001",
			},
			{
				name: "Document type: code model: ada vectorizationType: code",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-code-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: code",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-code-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v := New("apiKey", nullLogger())
				if got := v.getModelString(tt.args.docType, tt.args.model, "document"); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			docType string
			model   string
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Document type: text model: ada vectorizationType: query",
				args: args{
					docType: "text",
					model:   "ada",
				},
				want: "text-search-ada-query-001",
			},
			{
				name: "Document type: text model: babbage vectorizationType: query",
				args: args{
					docType: "text",
					model:   "babbage",
				},
				want: "text-search-babbage-query-001",
			},
			{
				name: "Document type: text model: curie vectorizationType: query",
				args: args{
					docType: "text",
					model:   "curie",
				},
				want: "text-search-curie-query-001",
			},
			{
				name: "Document type: text model: davinci vectorizationType: query",
				args: args{
					docType: "text",
					model:   "davinci",
				},
				want: "text-search-davinci-query-001",
			},

			{
				name: "Document type: code model: ada vectorizationType: text",
				args: args{
					docType: "code",
					model:   "ada",
				},
				want: "code-search-ada-text-001",
			},
			{
				name: "Document type: code model: babbage vectorizationType: text",
				args: args{
					docType: "code",
					model:   "babbage",
				},
				want: "code-search-babbage-text-001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				v := New("apiKey", nullLogger())
				if got := v.getModelString(tt.args.docType, tt.args.model, "query"); got != tt.want {
					t.Errorf("vectorizer.getModelString() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
