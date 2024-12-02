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
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/multimodalembeddings",
			},
			logger: nullLogger(),
		}
		expected := &ent.VectorizationResult{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			ImageVectors: [][]float32{{0.4, 0.5, 0.6}},
		}
		res, err := c.Vectorize(context.Background(),
			[]string{"This is my text"}, []string{"base64image"},
			ent.VectorizationConfig{
				Model:   "voyage-multimodal-3",
				BaseURL: server.URL,
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
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/multimodalembeddings",
			},
			logger: nullLogger(),
		}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		_, err := c.Vectorize(ctx, []string{"text"}, []string{"image"},
			ent.VectorizationConfig{
				Model: "voyage-multimodal-3",
			})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: "nope, not gonna happen",
		})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "apiKey",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/multimodalembeddings",
			},
			logger: nullLogger(),
		}
		_, err := c.Vectorize(context.Background(), []string{"text"}, []string{"image"},
			ent.VectorizationConfig{
				Model:   "voyage-multimodal-3",
				BaseURL: server.URL,
			})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "nope, not gonna happen")
	})

	t.Run("when VoyageAI key is passed using header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &vectorizer{
			apiKey:     "",
			httpClient: &http.Client{},
			urlBuilder: &voyageaiUrlBuilder{
				origin:   server.URL,
				pathMask: "/multimodalembeddings",
			},
			logger: nullLogger(),
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Voyageai-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			TextVectors:  [][]float32{{0.1, 0.2, 0.3}},
			ImageVectors: [][]float32{{0.4, 0.5, 0.6}},
		}
		res, err := c.Vectorize(ctxWithValue, []string{"text"}, []string{"image"},
			ent.VectorizationConfig{
				Model: "voyage-multimodal-3",
			})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError string
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != "" {
		resp := embeddingsResponse{
			Detail: f.serverError,
		}
		outBytes, err := json.Marshal(resp)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var req embeddingsRequest
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	resp := embeddingsResponse{
		Data: []embeddingData{
			{Embedding: []float32{0.1, 0.2, 0.3}},
			{Embedding: []float32{0.4, 0.5, 0.6}},
		},
	}
	outBytes, err := json.Marshal(resp)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
