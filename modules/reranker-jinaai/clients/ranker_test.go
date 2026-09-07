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
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

// buildResponse and buildError encode Jinaai's own wire format (a "results"
// array, and a {"detail": ...} error envelope); everything else about
// TestRank's stub server is shared via rerankertest.Handler.
func buildResponse(results []Result) ([]byte, error) {
	return json.Marshal(RankResponse{Results: results})
}

func buildError(message string) []byte {
	return []byte(`{"detail":"` + message + `"}`)
}

func TestRank(t *testing.T) {
	t.Run("when the server has a successful response", func(t *testing.T) {
		handler := rerankertest.NewHandler[Result](t,
			[]Result{{Index: 0, RelevanceScore: 0.9}}, nil, "", buildResponse, buildError)
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		cfg := rerankertest.FakeClassConfig{ClassConfig: map[string]interface{}{"baseURL": server.URL}}

		expected := &ent.RankResult{
			DocumentScores: []ent.DocumentScore{
				{
					Document: "I work at Apple",
					Score:    0.9,
				},
			},
			Query: "Where do I work?",
		}

		res, err := c.Rank(context.Background(), "Where do I work?", []string{"I work at Apple"}, cfg)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server has an error", func(t *testing.T) {
		handler := rerankertest.NewHandler[Result](t,
			nil, nil, "some error from the server", buildResponse, buildError)
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		cfg := rerankertest.FakeClassConfig{ClassConfig: map[string]interface{}{"baseURL": server.URL}}

		_, err := c.Rank(context.Background(), "I work at Apple", []string{"Where do I work?"}, cfg)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})

	t.Run("when we send requests in batches", func(t *testing.T) {
		batchedResults := [][]Result{
			{
				{Index: 0, RelevanceScore: 0.99},
				{Index: 1, RelevanceScore: 0.89},
			},
			{
				{Index: 0, RelevanceScore: 0.19},
				{Index: 1, RelevanceScore: 0.29},
			},
			{
				{Index: 0, RelevanceScore: 0.79},
				{Index: 1, RelevanceScore: 0.789},
			},
			{
				{Index: 0, RelevanceScore: 0.0001},
			},
		}
		handler := rerankertest.NewHandler[Result](t, nil, batchedResults, "", buildResponse, buildError)
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		cfg := rerankertest.FakeClassConfig{ClassConfig: map[string]interface{}{"baseURL": server.URL}}
		// this will trigger 4 go routines
		c.maxDocuments = 2

		query := "Where do I work?"
		documents := []string{
			"Response 1", "Response 2", "Response 3", "Response 4",
			"Response 5", "Response 6", "Response 7",
		}

		resp, err := c.Rank(context.Background(), query, documents, cfg)

		require.Nil(t, err)
		rerankertest.AssertBatchScores(t, documents, resp, 0.99, 0.0001)
	})
}

func TestRank_client_getJinaaiUrl(t *testing.T) {
	c := New("", 1*time.Second, nil)
	rerankertest.AssertBaseURLOverride(t, c.getJinaaiUrl,
		"https://api.jina.ai", "https://api.jina.ai/v1/rerank",
		"X-Jinaai-Baseurl", "https://base-url-from-ctx.com", "https://base-url-from-ctx.com/v1/rerank")
}
