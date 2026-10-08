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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJudgmentCache(t *testing.T) {
	key := func(i int) judgmentKey {
		return newJudgmentKey("url", "apiKey", "m", "q", fmt.Sprintf("doc %d", i))
	}

	t.Run("returns what was stored", func(t *testing.T) {
		c := newJudgmentCache[float64](2)
		c.putIfAbsent(key(1), 0.4)

		got, ok := c.get(key(1))
		require.True(t, ok)
		assert.Equal(t, 0.4, got)
		_, ok = c.get(key(2))
		assert.False(t, ok)
	})

	t.Run("every part changes the key", func(t *testing.T) {
		base := []string{"url", "apiKey", "model", "query", "document"}
		seen := map[judgmentKey]string{newJudgmentKey(base...): "base"}
		for i := range base {
			changed := append([]string(nil), base...)
			changed[i] += "x"
			key := newJudgmentKey(changed...)
			assert.NotContains(t, seen, key, "changing part %d", i)
			seen[key] = base[i]
		}
	})

	t.Run("moving text between parts changes the key", func(t *testing.T) {
		assert.NotEqual(t, newJudgmentKey("ab", "c"), newJudgmentKey("a", "bc"))
		assert.NotEqual(t, newJudgmentKey("ab", ""), newJudgmentKey("", "ab"))
	})

	t.Run("evicts the least recently used entry at capacity", func(t *testing.T) {
		c := newJudgmentCache[float64](2)
		c.putIfAbsent(key(1), 0.1)
		c.putIfAbsent(key(2), 0.2)
		_, _ = c.get(key(1))
		c.putIfAbsent(key(3), 0.3)

		_, ok := c.get(key(2))
		assert.False(t, ok, "key 2 was the least recently used")
		_, ok = c.get(key(1))
		assert.True(t, ok)
		_, ok = c.get(key(3))
		assert.True(t, ok)
		assert.Equal(t, 2, c.len())
	})

	t.Run("the first value stored for a key stays", func(t *testing.T) {
		c := newJudgmentCache[float64](2)

		assert.Equal(t, 0.1, c.putIfAbsent(key(1), 0.1))
		assert.Equal(t, 0.1, c.putIfAbsent(key(1), 0.9), "the later value is not stored and not returned")

		got, _ := c.get(key(1))
		assert.Equal(t, 0.1, got)
		assert.Equal(t, 1, c.len())
	})

	t.Run("never exceeds its capacity under concurrent use", func(t *testing.T) {
		c := newJudgmentCache[float64](16)
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 500 {
					c.putIfAbsent(key(g*1000+i), 0.5)
					c.get(key(i))
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, 16, c.len())
	})
}

func TestRankUsesTheCache(t *testing.T) {
	scores := map[string]float64{"a": 0.9, "b": 0.2, "c": 0.6}

	t.Run("a repeated request sends nothing", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		first, err := c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, nil))
		require.NoError(t, err)
		second, err := c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, nil))
		require.NoError(t, err)

		assert.Equal(t, first, second)
		assert.Len(t, handler.received(), 2)
	})

	t.Run("only the documents not seen before are sent", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		_, err := c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, nil))
		require.NoError(t, err)
		res, err := c.Rank(context.Background(), "q", []string{"c", "a"}, classConfig(server.URL, nil))
		require.NoError(t, err)

		assert.Equal(t, 0.6, res.DocumentScores[0].Score)
		assert.Equal(t, 0.9, res.DocumentScores[1].Score)
		requests := handler.received()
		require.Len(t, requests, 3)
		assert.Equal(t, "c", requests[2].body.State)
	})

	t.Run("another query or model is judged again", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		_, err := c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)
		_, err = c.Rank(context.Background(), "another q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)
		_, err = c.Rank(context.Background(), "q", []string{"a"},
			classConfig(server.URL, map[string]any{"model": "jev-1.13.0"}))
		require.NoError(t, err)

		assert.Len(t, handler.received(), 3)
	})

	t.Run("a failed judgment is not cached", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores, statuses: []int{http.StatusUnauthorized}}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		_, err := c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))
		require.Error(t, err)
		res, err := c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)

		assert.Equal(t, 0.9, res.DocumentScores[0].Score)
		assert.Len(t, handler.received(), 2)
	})

	t.Run("a cached request still needs an api key", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")
		_, err := c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)

		c.apiKey = ""
		_, err = c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))

		require.ErrorContains(t, err, "no api key found")
	})
}

func TestRankCacheIsSeparatedByEndpointAndAPIKey(t *testing.T) {
	t.Run("another endpoint does not read the judgments of the first", func(t *testing.T) {
		real := &typesafeaiHandler{t: t, scores: map[string]float64{"a": 0.1}}
		realServer := httptest.NewServer(real)
		defer realServer.Close()
		other := &typesafeaiHandler{t: t, scores: map[string]float64{"a": 1}}
		otherServer := httptest.NewServer(other)
		defer otherServer.Close()
		c := newTestClient("apiKey")

		// A caller points the module at their own server first.
		otherCtx := context.WithValue(context.Background(), "X-Typesafeai-Baseurl", []string{otherServer.URL})
		res, err := c.Rank(otherCtx, "q", []string{"a"}, classConfig(realServer.URL, nil))
		require.NoError(t, err)
		require.Equal(t, 1.0, res.DocumentScores[0].Score)

		res, err = c.Rank(context.Background(), "q", []string{"a"}, classConfig(realServer.URL, nil))
		require.NoError(t, err)

		assert.Equal(t, 0.1, res.DocumentScores[0].Score)
		assert.Len(t, real.received(), 1)
		assert.Len(t, other.received(), 1)
	})

	t.Run("another api key is checked by the API again", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"a": 0.4}}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		_, err := c.Rank(context.Background(), "q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)
		otherKey := context.WithValue(context.Background(), "X-Typesafeai-Api-Key", []string{"other"})
		_, err = c.Rank(otherKey, "q", []string{"a"}, classConfig(server.URL, nil))
		require.NoError(t, err)

		requests := handler.received()
		require.Len(t, requests, 2)
		assert.Equal(t, "Bearer other", requests[1].authorization)
	})
}

// Jev can give two probabilities for the same document. Queries that ask at
// the same time must all end up with one of them.
func TestRankConcurrentQueriesAgreeOnAProbability(t *testing.T) {
	handler := &typesafeaiHandler{t: t, delay: 50 * time.Millisecond, scoreByRequest: []float64{0.45, 0.55, 0.65}}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := newTestClient("apiKey")

	const queries = 3
	got := make([]float64, queries)
	var wg sync.WaitGroup
	for i := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))
			if assert.NoError(t, err) {
				got[i] = res.DocumentScores[0].Score
			}
		}()
	}
	wg.Wait()
	later, err := c.Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))
	require.NoError(t, err)

	require.Len(t, handler.received(), queries, "every query must have missed the cache")
	for _, score := range got {
		assert.Equal(t, got[0], score)
	}
	assert.Equal(t, got[0], later.DocumentScores[0].Score)
}

// A probability judged in a batch differs from one judged alone, so they
// must not be served for each other.
func TestRankCacheIsSeparatedByBatchSize(t *testing.T) {
	handler := &typesafeaiHandler{t: t, scores: map[string]float64{"a": 0.3, "b": 0.6}}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := newTestClient("apiKey")

	_, err := c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, map[string]any{"batchSize": 2}))
	require.NoError(t, err)
	_, err = c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, map[string]any{"batchSize": 1}))
	require.NoError(t, err)
	_, err = c.Rank(context.Background(), "q", []string{"a", "b"}, classConfig(server.URL, map[string]any{"batchSize": 1}))
	require.NoError(t, err)

	// One batch of two, then two single requests, then nothing.
	assert.Len(t, handler.received(), 3)
}

// A document that ends up alone in a request is judged like any single
// document, whatever batch size the class has. Its answer belongs with the
// single-document answers, which every batch size may read.
func TestRankCachesLoneDocumentsAsSingleJudgments(t *testing.T) {
	documents := make([]string, 11)
	scores := map[string]float64{}
	for i := range documents {
		documents[i] = fmt.Sprintf("doc %d", i)
		scores[documents[i]] = 0.5
	}

	t.Run("the tail of a batched request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		// 11 documents in batches of 10: one batch of 10 and doc 10 alone.
		_, err := c.Rank(context.Background(), "q", documents, classConfig(server.URL, map[string]any{"batchSize": 10}))
		require.NoError(t, err)
		require.Len(t, handler.received(), 2)

		// Judged alone before, so a class without batching can use it.
		_, err = c.Rank(context.Background(), "q", documents[10:], classConfig(server.URL, nil))
		require.NoError(t, err)
		assert.Len(t, handler.received(), 2)

		// Judged in a batch before, so a class without batching asks again.
		_, err = c.Rank(context.Background(), "q", documents[:1], classConfig(server.URL, nil))
		require.NoError(t, err)
		assert.Len(t, handler.received(), 3)
	})

	t.Run("a batched class reads single judgments", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		_, err := c.Rank(context.Background(), "q", documents[:1], classConfig(server.URL, nil))
		require.NoError(t, err)
		_, err = c.Rank(context.Background(), "q", documents[:3], classConfig(server.URL, map[string]any{"batchSize": 10}))
		require.NoError(t, err)

		requests := handler.received()
		require.Len(t, requests, 2)
		assert.Len(t, requests[1].body.State, 2, "doc 0 was judged alone before and is not sent again")
	})
}

func withCache(mode string) context.Context {
	return context.WithValue(context.Background(), "X-Typesafeai-Cache", []string{mode})
}

// X-Typesafeai-Cache lets a request judge again: "refresh" replaces the stored
// answers, "off" neither reads nor stores.
func TestRankCacheModes(t *testing.T) {
	// Every request gets another score, as if Jev changed its answer.
	newHandler := func(t *testing.T) *typesafeaiHandler {
		return &typesafeaiHandler{t: t, scoreByRequest: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6}}
	}
	score := func(t *testing.T, c *client, ctx context.Context, url string) float64 {
		res, err := c.Rank(ctx, "q", []string{"doc"}, classConfig(url, nil))
		require.NoError(t, err)
		return res.DocumentScores[0].Score
	}

	t.Run("on by default and when asked for", func(t *testing.T) {
		handler := newHandler(t)
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		assert.Equal(t, 0.1, score(t, c, context.Background(), server.URL))
		assert.Equal(t, 0.1, score(t, c, withCache("on"), server.URL))
		assert.Len(t, handler.received(), 1)
	})

	t.Run("refresh judges again and replaces the stored answer", func(t *testing.T) {
		handler := newHandler(t)
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		assert.Equal(t, 0.1, score(t, c, context.Background(), server.URL))
		assert.Equal(t, 0.2, score(t, c, withCache("refresh"), server.URL))
		assert.Equal(t, 0.2, score(t, c, context.Background(), server.URL), "the refreshed answer is the stored one")
		assert.Len(t, handler.received(), 2)
	})

	t.Run("refresh of one document replaces the answer a batched class reads", func(t *testing.T) {
		// The batch answers from scores; the second request, a alone,
		// answers 0.9.
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"a": 0.3, "b": 0.6}, scoreByRequest: []float64{0, 0.9}}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")
		batched := classConfig(server.URL, map[string]any{"batchSize": 2})

		res, err := c.Rank(context.Background(), "q", []string{"a", "b"}, batched)
		require.NoError(t, err)
		assert.Equal(t, 0.3, res.DocumentScores[0].Score)
		res, err = c.Rank(withCache("refresh"), "q", []string{"a"}, batched)
		require.NoError(t, err)
		assert.Equal(t, 0.9, res.DocumentScores[0].Score)

		res, err = c.Rank(context.Background(), "q", []string{"a", "b"}, batched)

		require.NoError(t, err)
		assert.Equal(t, 0.9, res.DocumentScores[0].Score, "the batched answer of a was replaced")
		assert.Equal(t, 0.6, res.DocumentScores[1].Score, "b keeps its answer")
		assert.Len(t, handler.received(), 2, "the last query is served from the cache")
	})

	t.Run("off judges every time and stores nothing", func(t *testing.T) {
		handler := newHandler(t)
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")

		assert.Equal(t, 0.1, score(t, c, withCache("off"), server.URL))
		assert.Equal(t, 0.2, score(t, c, withCache("off"), server.URL))
		assert.Equal(t, 0.3, score(t, c, context.Background(), server.URL), "nothing was stored by the two requests before")
		assert.Equal(t, 0.4, score(t, c, withCache("off"), server.URL), "a stored answer is not read either")
		assert.Equal(t, 0.3, score(t, c, context.Background(), server.URL), "and not replaced")
		assert.Len(t, handler.received(), 4)
	})

	t.Run("an unknown mode fails before any request", func(t *testing.T) {
		handler := newHandler(t)
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(withCache("sometimes"), "q", []string{"doc"}, classConfig(server.URL, nil))

		require.EqualError(t, err, `X-Typesafeai-Cache must be "on", "refresh" or "off", got "sometimes"`)
		assert.Empty(t, handler.received())
	})

	t.Run("identical documents still share one request when the cache is off", func(t *testing.T) {
		handler := newHandler(t)
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").Rank(withCache("off"), "q", []string{"doc", "doc"}, classConfig(server.URL, nil))

		require.NoError(t, err)
		assert.Equal(t, res.DocumentScores[0].Score, res.DocumentScores[1].Score)
		assert.Len(t, handler.received(), 1)
	})
}

func TestJudgmentCachePutReplaces(t *testing.T) {
	c := newJudgmentCache[float64](2)
	key := newJudgmentKey("doc")
	c.putIfAbsent(key, 0.1)

	c.put(key, 0.9)

	got, ok := c.get(key)
	require.True(t, ok)
	assert.Equal(t, 0.9, got)
	assert.Equal(t, 1, c.len())
}
