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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/modules/reranker-cohere/ent"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestRank(t *testing.T) {
	t.Run("when the server has a successful response", func(t *testing.T) {
		handler := &testRankHandler{
			t: t,
			response: RankResponse{
				Results: []Result{
					{
						Index:          0,
						RelevanceScore: 0.9,
					},
				},
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", nullLogger())
		c.host = server.URL

		expected := &ent.RankResult{
			RankPropertyValue: "I work at Apple",
			Query:             "Where do I work?",
			Score:             0.9,
		}

		res, err := c.Rank(context.Background(), nil, "I work at Apple", "Where do I work?")

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server has an error", func(t *testing.T) {
		handler := &testRankHandler{
			t: t,
			response: RankResponse{
				Results: []Result{},
			},
			errorMessage: "some error from the server",
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", nullLogger())
		c.host = server.URL

		_, err := c.Rank(context.Background(), nil, "I work at Apple", "Where do I work?")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testRankHandler struct {
	t            *testing.T
	response     RankResponse
	errorMessage string
}

func (f *testRankHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.errorMessage != "" {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"` + f.errorMessage + `"}`))
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b RankInput
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	outBytes, err := json.Marshal(f.response)
	require.Nil(f.t, err)

	w.Write(outBytes)
}
