//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func TestGetScore(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			res: RankResponse{
				Query: "Where do I work?",
				Scores: []DocumentScore{
					{
						Document: "I work at Apple",
						Score:    0.15,
					},
				},
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.Rank(context.Background(), "Where do I work?", []string{"I work at Apple"}, nil)

		assert.Nil(t, err)
		assert.Equal(t, ent.RankResult{
			Query: "Where do I work?",
			DocumentScores: []ent.DocumentScore{
				{
					Document: "I work at Apple",
					Score:    0.15,
				},
			},
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
		c := New(server.URL, 0, nullLogger())
		_, err := c.Rank(context.Background(), "prop",
			[]string{"I work at Apple"}, nil)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})

	t.Run("when we send requests in batches", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			res: RankResponse{
				Query: "Where do I work?",
				Scores: []DocumentScore{
					{
						Document: "I work at Apple",
						Score:    0.15,
					},
				},
			},
			batchedResults: [][]DocumentScore{
				{
					{
						Document: "Response 1",
						Score:    0.99,
					},
					{
						Document: "Response 2",
						Score:    0.89,
					},
				},
				{
					{
						Document: "Response 3",
						Score:    0.19,
					},
					{
						Document: "Response 4",
						Score:    0.29,
					},
				},
				{
					{
						Document: "Response 5",
						Score:    0.79,
					},
					{
						Document: "Response 6",
						Score:    0.789,
					},
				},
				{
					{
						Document: "Response 7",
						Score:    0.0001,
					},
				},
			},
		})
		defer server.Close()

		c := New(server.URL, 0, nullLogger())
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

func TestRankScoreCount(t *testing.T) {
	documents := []string{"I work at Apple", "I work at Google"}
	tests := []struct {
		name    string
		scores  []DocumentScore
		wantErr string
	}{
		{
			name:   "one score per document",
			scores: []DocumentScore{{Document: documents[0], Score: 0.9}, {Document: documents[1], Score: 0.2}},
		},
		{
			name:    "fewer scores than documents",
			scores:  []DocumentScore{{Document: documents[0], Score: 0.9}},
			wantErr: "1 scores for 2 documents",
		},
		{
			name: "more scores than documents",
			scores: []DocumentScore{
				{Document: documents[0], Score: 0.9},
				{Document: documents[1], Score: 0.2},
				{Document: documents[1], Score: 0.1},
			},
			wantErr: "3 scores for 2 documents",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(&testCrossRankerHandler{t: t, res: RankResponse{Scores: tt.scores}})
			defer server.Close()
			c := New(server.URL, 0, nullLogger())

			res, err := c.Rank(context.Background(), "Where do I work?", documents, nil)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Nil(t, res)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []ent.DocumentScore{
				{Document: documents[0], Score: 0.9},
				{Document: documents[1], Score: 0.2},
			}, res.DocumentScores)
		})
	}
}

type testCrossRankerHandler struct {
	lock           sync.RWMutex
	t              *testing.T
	res            RankResponse
	batchedResults [][]DocumentScore
}

func (f *testCrossRankerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lock.Lock()
	defer f.lock.Unlock()

	assert.Equal(f.t, "/rerank", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.res.Error != "" {
		w.WriteHeader(500)
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
		f.res.Scores = f.batchedResults[index]
	}

	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}
