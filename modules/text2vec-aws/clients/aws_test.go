//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
)

func TestClient(t *testing.T) {
	t.Run("when all is fine", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "access_key",
			awsSecret:    "secret",
			buildUrlFn: func(service, region, model string) string {
				return server.URL
			},
		}
		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{
				Service: "service",
				Region:  "region",
				Model:   "model",
			})

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server returns an error", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{
			t:           t,
			serverError: errors.Errorf("nope, not gonna happen"),
		})
		defer server.Close()
		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "access_key",
			awsSecret:    "secret",
			buildUrlFn: func(service, region, model string) string {
				return server.URL
			},
		}
		_, err := c.Vectorize(context.Background(), []string{"This is my text"},
			ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to AWS failed with status: 500 error: nope, not gonna happen")
	})

	t.Run("when Aws key is passed using X-Aws-Api-Key header", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "access_key",
			awsSecret:    "secret",
			buildUrlFn: func(service, region, model string) string {
				return server.URL
			},
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Aws-Api-Key", []string{"some-key"})

		expected := &ent.VectorizationResult{
			Text:       "This is my text",
			Vector:     []float32{0.1, 0.2, 0.3},
			Dimensions: 3,
		}
		res, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{})

		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when X-Aws-Access-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "",
			awsSecret:    "123",
			buildUrlFn: func(service, region, model string) string {
				return server.URL
			},
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Aws-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "AWS Access Key: no access key found neither in request header: "+
			"X-Aws-Access-Key nor in environment variable under AWS_ACCESS_KEY_ID")
	})

	t.Run("when X-Aws-Secret-Key header is passed but empty", func(t *testing.T) {
		server := httptest.NewServer(&fakeHandler{t: t})
		defer server.Close()
		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "123",
			awsSecret:    "",
			buildUrlFn: func(service, region, model string) string {
				return server.URL
			},
		}
		ctxWithValue := context.WithValue(context.Background(),
			"X-Aws-Api-Key", []string{""})

		_, err := c.Vectorize(ctxWithValue, []string{"This is my text"}, ent.VectorizationConfig{})

		require.NotNil(t, err)
		assert.Equal(t, err.Error(), "AWS Secret Key: no secret found neither in request header: "+
			"X-Aws-Access-Secret nor in environment variable under AWS_SECRET_ACCESS_KEY")
	})
}

type fakeHandler struct {
	t           *testing.T
	serverError error
}

func (f *fakeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.serverError != nil {
		embeddingResponse := &embeddingsResponse{
			Message: ptString(f.serverError.Error()),
		}

		outBytes, err := json.Marshal(embeddingResponse)
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

	textInput := req.InputText
	assert.Greater(f.t, len(textInput), 0)

	embeddingResponse := &embeddingsResponse{
		Embedding: []float32{0.1, 0.2, 0.3},
	}

	outBytes, err := json.Marshal(embeddingResponse)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func ptString(in string) *string {
	return &in
}
