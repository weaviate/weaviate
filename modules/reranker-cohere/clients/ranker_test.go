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
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
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
		c.urlBuilder = &fakeUrlBuilder{server.URL}

		expected := &ent.RankResult{
			DocumentScores: []ent.DocumentScore{
				{
					Document: "I work at Apple",
					Score:    0.9,
				},
			},
			Query: "Where do I work?",
		}

		res, err := c.Rank(context.Background(), "Where do I work?", "text", []string{"I work at Apple"}, nil)

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
		c.urlBuilder = &fakeUrlBuilder{server.URL}

		_, err := c.Rank(context.Background(), "I work at Apple", "text", []string{"Where do I work?"}, nil)

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
		c.urlBuilder = &fakeUrlBuilder{server.URL}
		// this will trigger 4 go routines
		c.maxDocuments = 2

		query := "Where do I work?"
		documents := []string{
			"Response 1", "Response 2", "Response 3", "Response 4",
			"Response 5", "Response 6", "Response 7",
		}

		resp, err := c.Rank(context.Background(), query, "text", documents, nil)

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

type fakeUrlBuilder struct {
	url string
}

func (b *fakeUrlBuilder) URL(baseURL string) string {
	return b.url
}

func TestRank_client_getCohereUrl(t *testing.T) {
	ctx := context.Background()
	c := New("", 1*time.Second, nil)

	url, err := c.getCohereUrl(ctx, "baseurl")
	assert.NoError(t, err)
	assert.Equal(t, "baseurl/v2/rerank", url)

	ctxWithBaseURL := context.WithValue(ctx, "X-Cohere-Baseurl", []string{"base-url-from-ctx"})
	url, err = c.getCohereUrl(ctxWithBaseURL, "base-url")
	assert.NoError(t, err)
	assert.Equal(t, "base-url-from-ctx/v2/rerank", url)
}

// A class-config baseURL is the fallback when no header is sent, so it must be
// validated like the header itself: a plain-HTTP one must never be sent the API
// key and the payload.
func TestRankRejectsInsecureBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	_, err := New("apiKey", 0, nullLogger()).Rank(context.Background(), "q", "text", []string{"a"},
		fakeClassConfig{baseURL: server.URL})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "baseURL must use HTTPS")
	assert.Zero(t, requests.Load(), "no request may reach a non-HTTPS baseURL")
}

type fakeClassConfig struct {
	baseURL string
}

func (f fakeClassConfig) Class() map[string]any { return nil }

func (f fakeClassConfig) ClassByModuleName(string) map[string]any {
	return map[string]any{"baseURL": f.baseURL}
}

func (f fakeClassConfig) Property(string) map[string]any { return nil }

func (f fakeClassConfig) Tenant() string { return "" }

func (f fakeClassConfig) TargetVector() string { return "" }

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }

func (f fakeClassConfig) Config() *config.Config { return nil }
