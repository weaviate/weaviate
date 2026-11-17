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
		w.Write([]byte(`{"detail":"` + f.errorMessage + `"}`))
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

func TestRankResponseShapes(t *testing.T) {
	tests := []struct {
		name     string
		response string
		want     []float64
		wantErr  string
	}{
		{
			name:     "document returned as an object",
			response: `{"results":[{"index":1,"relevance_score":0.9,"document":{"text":"b"}},{"index":0,"relevance_score":0.1,"document":{"text":"a"}}]}`,
			want:     []float64{0.1, 0.9},
		},
		{
			name:     "document returned as a string",
			response: `{"results":[{"index":0,"relevance_score":0.1,"document":"a"},{"index":1,"relevance_score":0.9,"document":"b"}]}`,
			want:     []float64{0.1, 0.9},
		},
		{
			name:     "no document returned",
			response: `{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":0.9}]}`,
			want:     []float64{0.1, 0.9},
		},
		{
			name:     "index out of range",
			response: `{"results":[{"index":0,"relevance_score":0.1},{"index":2,"relevance_score":0.9}]}`,
			wantErr:  "invalid or repeated index 2",
		},
		{
			name:     "negative index",
			response: `{"results":[{"index":0,"relevance_score":0.1},{"index":-1,"relevance_score":0.9}]}`,
			wantErr:  "invalid or repeated index -1",
		},
		{
			name:     "repeated index",
			response: `{"results":[{"index":0,"relevance_score":0.1},{"index":0,"relevance_score":0.9}]}`,
			wantErr:  "invalid or repeated index 0",
		},
		{
			name:     "fewer results than documents",
			response: `{"results":[{"index":0,"relevance_score":0.1}]}`,
			wantErr:  "1 results for 2 documents",
		},
		{
			name:     "more results than documents",
			response: `{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":0.9},{"index":2,"relevance_score":0.5}]}`,
			wantErr:  "3 results for 2 documents",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent RankInput
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
				w.Write([]byte(tt.response))
			}))
			defer server.Close()
			c := New("apiKey", 0, nullLogger())
			c.host = server.URL

			resp, err := c.Rank(context.Background(), "q", []string{"a", "b"}, nil)

			assert.False(t, sent.ReturnDocuments)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, resp.DocumentScores, 2)
			for i, score := range tt.want {
				assert.Equal(t, []string{"a", "b"}[i], resp.DocumentScores[i].Document)
				assert.Equal(t, score, resp.DocumentScores[i].Score)
			}
		})
	}
}
