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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				Model:        "sentence-transformers/gtr-t5-xxl",
				WaitForModel: false,
				UseGPU:       false,
				UseCache:     true,
				EndpointURL:  server.URL,
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the context is expired", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, "This is my text", ent.VectorizationConfig{
			EndpointURL: server.URL,
		})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				EndpointURL: server.URL,
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to HuggingFace failed with status: 500 error: nope, not gonna happen estimated time: 20")
	})

	t.Run("when HuggingFace key is passed using X-Huggingface-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Huggingface-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Model:        "sentence-transformers/gtr-t5-xxl",
				WaitForModel: true,
				UseGPU:       false,
				UseCache:     true,
				EndpointURL:  server.URL,
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when a request requires an API KEY", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("A valid user or organization token is required"),
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Huggingface-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, "This is my text",
			ent.VectorizationConfig{
				Model:       "sentence-transformers/gtr-t5-xxl",
				EndpointURL: server.URL,
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "failed with status: 401 error: A valid user or organization token is required")
	})

	t.Run("when the server returns an error with warnings", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("with warnings"),
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			logger:     nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), "This is my text",
			ent.VectorizationConfig{
				EndpointURL: server.URL,
			})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "connection to HuggingFace failed with status: 500 error: with warnings "+
			"warnings: [There was an inference error: CUDA error: all CUDA-capable devices are busy or unavailable\n"+
			"CUDA kernel errors might be asynchronously reported at some other API call,so the stacktrace below might be incorrect.\n"+
			"For debugging consider passing CUDA_LAUNCH_BLOCKING=1.]")
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		switch f.serverError.Error() {
		case "with warnings":
			embeddingError := map[string]interface{}{
				"error": f.serverError.Error(),
				"warnings": []string{
					"There was an inference error: CUDA error: all CUDA-capable devices are busy or unavailable\n" +
						"CUDA kernel errors might be asynchronously reported at some other API call,so the stacktrace below might be incorrect.\n" +
						"For debugging consider passing CUDA_LAUNCH_BLOCKING=1.",
				},
			}
			outBytes, err := json.Marshal(embeddingError)
			require.Nil(f.t, err)

			w.WriteHeader(http.StatusInternalServerError)
			w.Write(outBytes)
			return
		case "A valid user or organization token is required":
			embeddingError := map[string]interface{}{
				"error": "A valid user or organization token is required",
			}
			outBytes, err := json.Marshal(embeddingError)
			require.Nil(f.t, err)

			w.WriteHeader(http.StatusUnauthorized)
			w.Write(outBytes)
			return
		default:
			embeddingError := map[string]interface{}{
				"error":          f.serverError.Error(),
				"estimated_time": 20.0,
			}
			outBytes, err := json.Marshal(embeddingError)
			require.Nil(f.t, err)

			w.WriteHeader(http.StatusInternalServerError)
			w.Write(outBytes)
			return
		}
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	textInputs := b["inputs"].([]interface{})
	assert.Greater(f.t, len(textInputs), 0)
	textInput := textInputs[0].(string)
	assert.Greater(f.t, len(textInput), 0)

	// TODO: fix this
	embedding := [][]float32{{0.1, 0.2, 0.3}}
	outBytes, err := json.Marshal(embedding)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func Test_getURL(t *testing.T) {
	v := &vectorizer{}

	tests := []struct {
		name   string
		config ent.VectorizationConfig
		want   string
	}{
		{
			name: "Facebook DPR model",
			config: ent.VectorizationConfig{
				Model: "sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
			},
			want: "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
		},
		{
			name: "BERT base model (uncased)",
			config: ent.VectorizationConfig{
				Model: "bert-base-uncased",
			},
			want: "https://api-inference.huggingface.co/pipeline/feature-extraction/bert-base-uncased",
		},
		{
			name: "BERT base model (uncased)",
			config: ent.VectorizationConfig{
				EndpointURL: "https://self-hosted-instance.com/bert-base-uncased",
			},
			want: "https://self-hosted-instance.com/bert-base-uncased",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, v.getURL(tt.config))
		})
	}
}
