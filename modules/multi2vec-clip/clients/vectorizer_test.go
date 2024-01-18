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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/multi2vec-clip/ent"
)

func TestVectorize(t *testing.T) {
	t.Run("when the response is successful", func(t *testing.T) {
		server := httptest.NewServer(&testVectorizeHandler{
			t: t,
			res: vecResponse{
				TextVectors: [][]float32{
					{0, 1, 2},
				},
				ImageVectors: [][]float32{
					{1, 2, 3},
				},
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.Vectorize(context.Background(), []string{"hello"},
			[]string{"image-encoding"})

		assert.Nil(t, err)
		assert.Equal(t, &ent.VectorizationResult{
			TextVectors: [][]float32{
				{0, 1, 2},
			},
			ImageVectors: [][]float32{
				{1, 2, 3},
			},
		}, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testVectorizeHandler{
			t: t,
			res: vecResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		_, err := c.Vectorize(context.Background(), []string{"hello"},
			[]string{"image-encoding"})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testVectorizeHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	res vecResponse
}

func (f *testVectorizeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/vectorize", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.res.Error != "" {
		w.WriteHeader(500)
	}
	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}
