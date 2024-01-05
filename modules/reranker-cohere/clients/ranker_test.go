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
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
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

		c := New("apiKey", 0, nullLogger())
		c.host = server.URL

		expected := &ent.RankResult{
			DocumentScores: []ent.DocumentScore{
				{
					Document: "I work at Apple",
					Score:    0.9,
				},
			},
			Query: "Where do I work?",
		}

		res, err := c.Rank(context.Background(), "Where do I work?", []string{"I work at Apple"}, nil)

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

		c := New("apiKey", 0, nullLogger())
		c.host = server.URL

		_, err := c.Rank(context.Background(), "I work at Apple", []string{"Where do I work?"}, nil)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})

	t.Run("when we send requests in batches", func(t *testing.T) {
		handler := &testRankHandler{
			t: t,
			batchedResults: [][]Result{
				{
					{
						Index:          0,
						RelevanceScore: 0.99,
					},
					{
						Index:          1,
						RelevanceScore: 0.89,
					},
				},
				{
					{
						Index:          0,
						RelevanceScore: 0.19,
					},
					{
						Index:          1,
						RelevanceScore: 0.29,
					},
				},
				{
					{
						Index:          0,
						RelevanceScore: 0.79,
					},
					{
						Index:          1,
						RelevanceScore: 0.789,
					},
				},
				{
					{
						Index:          0,
						RelevanceScore: 0.0001,
					},
				},
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		c.host = server.URL
		// this will trigger 4 go routines
		c.maxDocuments = 2

		query := "Where do I work?"
		documents := []string{
			"Response 1", "Response 2", "Response 3", "Response 4",
			"Response 5", "Response 6", "Response 7",
		}

		resp, err := c.Rank(context.Background(), query, documents, nil)

		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.DocumentScores)
		for i := range resp.DocumentScores {
			assert.Equal(t, documents[i], resp.DocumentScores[i].Document)
			if i == 0 {
				assert.Equal(t, 0.99, resp.DocumentScores[i].Score)
			}
			if i == len(documents)-1 {
				assert.Equal(t, 0.0001, resp.DocumentScores[i].Score)
			}
		}
	})
}

type testRankHandler struct {
	lock           sync.RWMutex
	t              *testing.T
	response       RankResponse
	batchedResults [][]Result
	errorMessage   string
}

func (f *testRankHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.errorMessage != "" {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"` + f.errorMessage + `"}`))
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var req RankInput
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	containsDocument := func(req RankInput, in string) bool {
		for _, doc := range req.Documents {
			if doc == in {
				return true
			}
		}
		return false
	}

	index := 0
	if len(f.batchedResults) > 0 {
		if containsDocument(req, "Response 3") {
			index = 1
		}
		if containsDocument(req, "Response 5") {
			index = 2
		}
		if containsDocument(req, "Response 7") {
			index = 3
		}
		f.response.Results = f.batchedResults[index]
	}

	outBytes, err := json.Marshal(f.response)
	require.Nil(f.t, err)

	w.Write(outBytes)
}
