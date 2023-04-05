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

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/ent"
)

func TestGetScore(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			res: RankResponse{
				RankPropertyValue: "I work at Apple",
				Query:             "Where do I work?",
				Score:             0.15,
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		res, err := c.Rank(context.Background(), "prop", "I work at Apple")

		assert.Nil(t, err)
		assert.Equal(t, ent.RankResult{
			Query:             "Where do I work?",
			RankPropertyValue: "I work at Apple",
			Score:             0.15,
		}, *res)
	})

	t.Run("when the server has an error", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			res: RankResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		_, err := c.Rank(context.Background(), "prop",
			"I work at Apple")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testCrossRankerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	res RankResponse
}

func (f *testCrossRankerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/crossrank", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.res.Error != "" {
		w.WriteHeader(500)
	}

	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}
