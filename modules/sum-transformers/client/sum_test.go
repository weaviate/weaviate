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

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/sum-transformers/ent"
)

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testSUMHandler{
			t: t,
			res: sumResponse{
				sumInput: sumInput{
					Text: "I work at Apple",
				},
				Summary: []summaryResponse{
					{
						Result: "Apple",
					},
				},
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.GetSummary(context.Background(), "prop",
			"I work at Apple")

		assert.Nil(t, err)
		assert.Equal(t, []ent.SummaryResult{
			{
				Result:   "Apple",
				Property: "prop",
			},
		}, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testSUMHandler{
			t: t,
			res: sumResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		_, err := c.GetSummary(context.Background(), "prop",
			"I work at Apple")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testSUMHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	res sumResponse
}

func (f *testSUMHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/sum/", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.res.Error != "" {
		w.WriteHeader(500)
	}

	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}
