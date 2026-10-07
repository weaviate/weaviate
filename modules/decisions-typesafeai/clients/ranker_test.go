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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func newTestClient(apiKey string) *client {
	c := New(apiKey, 0, DefaultMaxConcurrentRequests, nullLogger())
	c.retryBackoff = time.Millisecond
	return c
}

func TestRank(t *testing.T) {
	query := "the customer is angry"

	t.Run("scores every document in input order", func(t *testing.T) {
		scores := map[string]float64{
			"I want a refund now":      0.97,
			"Thanks, all good":         0.02,
			"Where is my order?":       0.41,
			"This is unacceptable!!":   0.99,
			"Could you update my plan": 0.05,
		}
		documents := []string{
			"I want a refund now", "Thanks, all good", "Where is my order?",
			"This is unacceptable!!", "Could you update my plan",
		}
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("apiKey")
		res, err := c.Rank(context.Background(), query, documents, classConfig(server.URL, nil))

		require.NoError(t, err)
		require.Equal(t, query, res.Query)
		expected := make([]ent.DocumentScore, len(documents))
		for i, doc := range documents {
			expected[i] = ent.DocumentScore{Document: doc, Score: scores[doc]}
		}
		assert.Equal(t, expected, res.DocumentScores)

		requests := handler.received()
		require.Len(t, requests, len(documents))
		for _, req := range requests {
			assert.Equal(t, "Bearer apiKey", req.authorization)
			assert.Equal(t, "jev-latest", req.body.Model)
			require.Len(t, req.body.Questions, 1)
			question := req.body.Questions[questionKey]
			assert.Equal(t, "noul", question.Type)
			assert.Equal(t, query, question.Instructions)
		}
	})

	t.Run("uses the model from the class config", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.5}}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("apiKey")
		_, err := c.Rank(context.Background(), query, []string{"doc"},
			classConfig(server.URL, map[string]any{"model": "jev-1.13.0"}))

		require.NoError(t, err)
		require.Len(t, handler.received(), 1)
		assert.Equal(t, "jev-1.13.0", handler.received()[0].body.Model)
	})

	t.Run("empty document scores zero without a request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.8}}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("apiKey")
		res, err := c.Rank(context.Background(), query, []string{"", "doc", ""}, classConfig(server.URL, nil))

		require.NoError(t, err)
		assert.Equal(t, []ent.DocumentScore{
			{Document: "", Score: 0},
			{Document: "doc", Score: 0.8},
			{Document: "", Score: 0},
		}, res.DocumentScores)
		assert.Len(t, handler.received(), 1)
	})

	t.Run("only empty documents is an error, not an empty answer", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{}}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("apiKey")
		_, err := c.Rank(context.Background(), query, []string{"", ""}, classConfig(server.URL, nil))

		require.EqualError(t, err, "no document has text: check the rerank property")
		assert.Empty(t, handler.received())
	})

	t.Run("identical documents share one request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"same": 0.7, "other": 0.1}}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("apiKey")
		res, err := c.Rank(context.Background(), query, []string{"same", "other", "same"}, classConfig(server.URL, nil))

		require.NoError(t, err)
		assert.Equal(t, []ent.DocumentScore{
			{Document: "same", Score: 0.7},
			{Document: "other", Score: 0.1},
			{Document: "same", Score: 0.7},
		}, res.DocumentScores)
		assert.Len(t, handler.received(), 2)
	})

	t.Run("api key from the request header wins", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.5}}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := newTestClient("envKey")
		ctx := context.WithValue(context.Background(), "X-Typesafeai-Api-Key", []string{"headerKey"})
		_, err := c.Rank(ctx, query, []string{"doc"}, classConfig(server.URL, nil))

		require.NoError(t, err)
		require.Len(t, handler.received(), 1)
		assert.Equal(t, "Bearer headerKey", handler.received()[0].authorization)
	})
}

func TestRankRejectedBeforeAnyRequest(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    string
		query     string
		documents []string
		settings  map[string]any
		wantErr   string
	}{
		{
			name:      "more documents than maxDocuments",
			apiKey:    "apiKey",
			query:     "q",
			documents: []string{"a", "b", "c"},
			settings:  map[string]any{"maxDocuments": 2},
			wantErr:   "3 documents exceed maxDocuments 2",
		},
		{
			name:      "more documents than the default maxDocuments",
			apiKey:    "apiKey",
			query:     "q",
			documents: make([]string, 101),
			wantErr:   "101 documents exceed maxDocuments 100",
		},
		{
			name:      "maxDocuments above the upper bound is capped",
			apiKey:    "apiKey",
			query:     "q",
			documents: make([]string, 1001),
			settings:  map[string]any{"maxDocuments": 5000},
			wantErr:   "1001 documents exceed maxDocuments 1000",
		},
		{
			name:      "empty query",
			apiKey:    "apiKey",
			query:     "",
			documents: []string{"a"},
			wantErr:   "no query provided",
		},
		{
			name:      "no api key",
			apiKey:    "",
			query:     "q",
			documents: []string{"a"},
			wantErr:   "no api key found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &typesafeaiHandler{t: t}
			server := httptest.NewServer(handler)
			defer server.Close()

			c := newTestClient(tt.apiKey)
			_, err := c.Rank(context.Background(), tt.query, tt.documents, classConfig(server.URL, tt.settings))

			require.ErrorContains(t, err, tt.wantErr)
			assert.Empty(t, handler.received())
		})
	}
}

func TestRankErrorResponses(t *testing.T) {
	tests := []struct {
		name         string
		statuses     []int
		wantRequests int
		wantErr      string
	}{
		{
			name:         "401 is not retried",
			statuses:     []int{http.StatusUnauthorized},
			wantRequests: 1,
			wantErr:      "status 401",
		},
		{
			name:         "422 is not retried",
			statuses:     []int{http.StatusUnprocessableEntity},
			wantRequests: 1,
			wantErr:      "status 422",
		},
		{
			name:         "429 then success",
			statuses:     []int{http.StatusTooManyRequests, http.StatusOK},
			wantRequests: 2,
		},
		{
			name:         "529 then 500 then success",
			statuses:     []int{529, http.StatusInternalServerError, http.StatusOK},
			wantRequests: 3,
		},
		{
			name:         "retries are bounded",
			statuses:     []int{529, 529, 529, 529, 529},
			wantRequests: maxAttempts,
			wantErr:      "status 529",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.6}, statuses: tt.statuses}
			server := httptest.NewServer(handler)
			defer server.Close()

			c := newTestClient("apiKey")
			res, err := c.Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

			assert.Len(t, handler.received(), tt.wantRequests)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), "error from jev")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 0.6, res.DocumentScores[0].Score)
		})
	}
}

func TestRankMalformedResponses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "not json",
			body:    "<html>",
			wantErr: "parse response",
		},
		{
			name:    "answer missing",
			body:    `{"model":"jev-latest","answers":{},"usage":{"input_tokens":1,"output_tokens":1}}`,
			wantErr: "no answer in response",
		},
		{
			name:    "answer has another type",
			body:    `{"answers":{"q":{"type":"choice","choice":"a"}}}`,
			wantErr: `unexpected answer type "choice"`,
		},
		{
			name:    "probability missing",
			body:    `{"answers":{"q":{"type":"noul"}}}`,
			wantErr: "no probability in response",
		},
		{
			name:    "probability out of range",
			body:    `{"answers":{"q":{"type":"noul","noul":1.5}}}`,
			wantErr: "probability 1.5 out of range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &typesafeaiHandler{t: t, rawBody: tt.body}
			server := httptest.NewServer(handler)
			defer server.Close()

			c := newTestClient("apiKey")
			_, err := c.Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

			require.ErrorContains(t, err, tt.wantErr)
			assert.Len(t, handler.received(), 1)
		})
	}
}

func TestRankStopsOnContextCancel(t *testing.T) {
	handler := &typesafeaiHandler{t: t, statuses: []int{529, 529, 529, 529, 529}}
	server := httptest.NewServer(handler)
	defer server.Close()

	c := newTestClient("apiKey")
	c.retryBackoff = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.Rank(ctx, "q", []string{"doc"}, classConfig(server.URL, nil))

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Len(t, handler.received(), 1)
}

func TestRankBoundsRequestsInFlight(t *testing.T) {
	const limit = 4
	documents := func(prefix string) ([]string, map[string]float64) {
		out := make([]string, 30)
		scores := map[string]float64{}
		for i := range out {
			out[i] = fmt.Sprintf("%s %d", prefix, i)
			scores[out[i]] = 0.5
		}
		return out, scores
	}
	first, scores := documents("first")
	second, moreScores := documents("second")
	for document, score := range moreScores {
		scores[document] = score
	}
	handler := &typesafeaiHandler{t: t, scores: scores, delay: 20 * time.Millisecond}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 0, limit, nullLogger())

	// Two queries at once share the limit.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, docs := range [][]string{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Rank(context.Background(), "q", docs, classConfig(server.URL, nil))
		}()
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Len(t, handler.received(), len(first)+len(second))
	assert.LessOrEqual(t, handler.maxInFlight, limit)
	assert.Greater(t, handler.maxInFlight, 1)
}

func TestRankBatches(t *testing.T) {
	documents := make([]string, 25)
	scores := map[string]float64{}
	for i := range documents {
		documents[i] = fmt.Sprintf("doc %d", i)
		scores[documents[i]] = float64(i) / 100
	}

	tests := []struct {
		name         string
		settings     map[string]any
		wantRequests []int
	}{
		{name: "default is one document per request", settings: map[string]any{"batchSize": nil}, wantRequests: make([]int, 25)},
		{name: "batch size of 10", settings: map[string]any{"batchSize": 10}, wantRequests: []int{5, 10, 10}},
		{name: "batch size of 7", settings: map[string]any{"batchSize": 7}, wantRequests: []int{4, 7, 7, 7}},
		{name: "batch larger than the documents", settings: map[string]any{"batchSize": 25}, wantRequests: []int{25}},
		{name: "batch size above the maximum is capped", settings: map[string]any{"batchSize": 500}, wantRequests: []int{25}},
		{name: "batch size of 1", settings: map[string]any{"batchSize": 1}, wantRequests: make([]int, 25)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &typesafeaiHandler{t: t, scores: scores}
			server := httptest.NewServer(handler)
			defer server.Close()

			res, err := newTestClient("apiKey").Rank(context.Background(), "q", documents,
				classConfig(server.URL, tt.settings))

			require.NoError(t, err)
			for i, document := range documents {
				assert.Equal(t, ent.DocumentScore{Document: document, Score: scores[document]}, res.DocumentScores[i])
			}
			var sizes []int
			for _, request := range handler.received() {
				if state, ok := request.body.State.(map[string]any); ok {
					sizes = append(sizes, len(state))
				} else {
					sizes = append(sizes, 0)
				}
			}
			sort.Ints(sizes)
			assert.Equal(t, tt.wantRequests, sizes)
		})
	}

	t.Run("a batch the API rejects for its size is sent as halves", func(t *testing.T) {
		// Jev accepts at most 3 documents per request here: 10 -> 5 + 5 -> 2 + 3 each.
		handler := &typesafeaiHandler{t: t, scores: scores, maxDocumentsPerRequest: 3}
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").Rank(context.Background(), "q", documents[:10],
			classConfig(server.URL, map[string]any{"batchSize": 10}))

		require.NoError(t, err)
		for i, document := range documents[:10] {
			assert.Equal(t, ent.DocumentScore{Document: document, Score: scores[document]}, res.DocumentScores[i])
		}
		var sizes []int
		for _, request := range handler.received() {
			if state, ok := request.body.State.(map[string]any); ok {
				sizes = append(sizes, len(state))
			}
		}
		sort.Ints(sizes)
		assert.Equal(t, []int{2, 2, 3, 3, 5, 5, 10}, sizes)
	})

	t.Run("a single document the API rejects for its size is an error", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores, maxDocumentsPerRequest: -1}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", documents[:1],
			classConfig(server.URL, map[string]any{"batchSize": 1}))

		require.ErrorContains(t, err, "max_tokens_exceeded")
		assert.Len(t, handler.received(), 1)
	})

	t.Run("a batch of one document uses the single-document request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", documents[:11],
			classConfig(server.URL, map[string]any{"batchSize": 10}))

		require.NoError(t, err)
		requests := handler.received()
		require.Len(t, requests, 2)
		single := 0
		for _, request := range requests {
			if state, ok := request.body.State.(string); ok {
				single++
				assert.Equal(t, "doc 10", state)
				assert.Equal(t, "q", request.body.Questions[questionKey].Instructions)
			}
		}
		assert.Equal(t, 1, single)
	})

	t.Run("a batch with an answer missing fails the request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores, omitAnswerFor: "document_2"}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", documents[:5],
			classConfig(server.URL, map[string]any{"batchSize": 5}))

		require.ErrorContains(t, err, `no answer in response for "document_2"`)
	})

	t.Run("only documents not judged before are batched", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()
		c := newTestClient("apiKey")
		cfg := classConfig(server.URL, map[string]any{"batchSize": 10})

		_, err := c.Rank(context.Background(), "q", documents[:8], cfg)
		require.NoError(t, err)
		res, err := c.Rank(context.Background(), "q", documents[:12], cfg)
		require.NoError(t, err)

		requests := handler.received()
		require.Len(t, requests, 2)
		assert.Len(t, requests[1].body.State, 4)
		assert.Equal(t, scores["doc 3"], res.DocumentScores[3].Score)
		assert.Equal(t, scores["doc 11"], res.DocumentScores[11].Score)
	})
}

func TestRankDoesNotRetryATimeout(t *testing.T) {
	handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.5}, delay: 300 * time.Millisecond}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 50*time.Millisecond, DefaultMaxConcurrentRequests, nullLogger())
	c.retryBackoff = time.Millisecond

	start := time.Now()
	_, err := c.Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

	require.Error(t, err)
	assert.Less(t, time.Since(start), 250*time.Millisecond, "a retry would wait for a second timeout")
	// The handler records a request when it arrives, before its delay.
	assert.Len(t, handler.received(), 1)
}

func TestRetryDelay(t *testing.T) {
	c := newTestClient("apiKey")
	c.retryBackoff = 400 * time.Millisecond

	tests := []struct {
		name       string
		attempt    int
		retryAfter time.Duration
		wantMin    time.Duration
		wantMax    time.Duration
	}{
		{name: "first retry", attempt: 1, wantMin: 200 * time.Millisecond, wantMax: 400 * time.Millisecond},
		{name: "second retry doubles", attempt: 2, wantMin: 400 * time.Millisecond, wantMax: 800 * time.Millisecond},
		{name: "third retry doubles again", attempt: 3, wantMin: 800 * time.Millisecond, wantMax: 1600 * time.Millisecond},
		{name: "Retry-After above the backoff wins", attempt: 1, retryAfter: 3 * time.Second, wantMin: 3 * time.Second, wantMax: 3 * time.Second},
		{name: "Retry-After below the backoff is ignored", attempt: 3, retryAfter: time.Millisecond, wantMin: 800 * time.Millisecond, wantMax: 1600 * time.Millisecond},
		{name: "Retry-After is capped", attempt: 1, retryAfter: time.Hour, wantMin: maxRetryAfter, wantMax: maxRetryAfter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := map[time.Duration]bool{}
			for range 200 {
				delay := c.retryDelay(tt.attempt, tt.retryAfter)
				require.GreaterOrEqual(t, delay, tt.wantMin)
				require.LessOrEqual(t, delay, tt.wantMax)
				seen[delay] = true
			}
			if tt.wantMin != tt.wantMax {
				assert.Greater(t, len(seen), 1, "delays must differ between batches")
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := map[string]time.Duration{
		"":                              0,
		"2":                             2 * time.Second,
		"0":                             0,
		"-5":                            0,
		"soon":                          0,
		"Wed, 21 Oct 2026 07:28:00 GMT": 0,
	}
	for header, want := range tests {
		t.Run(header, func(t *testing.T) {
			assert.Equal(t, want, parseRetryAfter(header))
		})
	}
}

func TestRankHonoursRetryAfter(t *testing.T) {
	handler := &typesafeaiHandler{
		t: t, scores: map[string]float64{"doc": 0.5},
		statuses: []int{http.StatusTooManyRequests}, retryAfter: "1",
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	start := time.Now()
	_, err := newTestClient("apiKey").Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), time.Second)
	assert.Len(t, handler.received(), 2)
}

type receivedRequest struct {
	authorization string
	body          typesafeaiRequest
}

// typesafeaiHandler fakes the TypeSafeAI API. statuses[i] is the status of the i-th request;
// requests beyond len(statuses) get 200.
type typesafeaiHandler struct {
	t        *testing.T
	scores   map[string]float64
	statuses []int
	rawBody  string
	delay    time.Duration
	// scoreByRequest gives the n-th request the n-th score, whatever the
	// document. It stands for Jev answering the same question differently.
	scoreByRequest []float64
	// omitAnswerFor leaves one document of a batch without an answer.
	omitAnswerFor string
	retryAfter    string
	// maxDocumentsPerRequest makes the handler answer 400 max_tokens_exceeded
	// to a larger request; 0 accepts every request, -1 rejects every request.
	maxDocumentsPerRequest int

	lock        sync.Mutex
	requests    []receivedRequest
	inFlight    int
	maxInFlight int
}

func (h *typesafeaiHandler) received() []receivedRequest {
	h.lock.Lock()
	defer h.lock.Unlock()
	return append([]receivedRequest(nil), h.requests...)
}

func (h *typesafeaiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	assert.NoError(h.t, err)
	defer r.Body.Close()

	var req typesafeaiRequest
	assert.NoError(h.t, json.Unmarshal(bodyBytes, &req))
	assert.Equal(h.t, "/v1/systemone", r.URL.Path)
	assert.Equal(h.t, http.MethodPost, r.Method)

	h.lock.Lock()
	status := http.StatusOK
	if n := len(h.requests); n < len(h.statuses) {
		status = h.statuses[n]
	}
	var fixedScore *float64
	if n := len(h.requests); n < len(h.scoreByRequest) {
		fixedScore = &h.scoreByRequest[n]
	}
	h.requests = append(h.requests, receivedRequest{authorization: r.Header.Get("Authorization"), body: req})
	h.inFlight++
	if h.inFlight > h.maxInFlight {
		h.maxInFlight = h.inFlight
	}
	h.lock.Unlock()

	defer func() {
		h.lock.Lock()
		h.inFlight--
		h.lock.Unlock()
	}()
	time.Sleep(h.delay)

	if limit := h.maxDocumentsPerRequest; limit != 0 && status == http.StatusOK {
		sent := 1
		if state, ok := req.State.(map[string]any); ok {
			sent = len(state)
		}
		if limit < 0 || sent > limit {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"detail":{"error_type":"max_tokens_exceeded"}}`))
			return
		}
	}
	if status != http.StatusOK {
		if h.retryAfter != "" {
			w.Header().Set("Retry-After", h.retryAfter)
		}
		w.WriteHeader(status)
		w.Write([]byte(`{"error":"error from jev"}`))
		return
	}
	if h.rawBody != "" {
		w.Write([]byte(h.rawBody))
		return
	}

	answers := map[string]typesafeaiAnswer{}
	switch state := req.State.(type) {
	case string:
		score, ok := h.scores[state]
		if fixedScore != nil {
			score, ok = *fixedScore, true
		}
		assert.True(h.t, ok, "unexpected state %q", state)
		answers[questionKey] = answerFor(req.Questions[questionKey], score)
	case map[string]any:
		assert.Len(h.t, req.Questions, len(state), "one question per document")
		for key, document := range state {
			score, ok := h.scores[document.(string)]
			assert.True(h.t, ok, "unexpected document %q", document)
			assert.True(h.t, strings.HasPrefix(req.Questions[key].Instructions, "In "+key+": "),
				"question %q must name its document", key)
			if key == h.omitAnswerFor {
				continue
			}
			answers[key] = answerFor(req.Questions[key], score)
		}
	default:
		assert.Fail(h.t, "state must be a string or a map", "%T", req.State)
	}
	// The fields the client does not read are part of the real reply.
	resp := map[string]any{
		"model":   req.Model,
		"answers": answers,
		"usage":   map[string]int{"input_tokens": 300, "output_tokens": 20},
	}
	assert.NoError(h.t, json.NewEncoder(w).Encode(resp))
}

// classConfig sends one document per request unless the settings say
// otherwise, which keeps the request count of a test equal to its documents.
// answerFor answers in the type of the question.
func answerFor(question typesafeaiQuestion, value float64) typesafeaiAnswer {
	if question.Type == scoreType {
		return typesafeaiAnswer{Type: scoreType, Score: &value}
	}
	return typesafeaiAnswer{Type: noulType, Noul: &value}
}

func classConfig(baseURL string, settings map[string]any) moduletools.ClassConfig {
	cfg := map[string]any{"baseURL": baseURL, "batchSize": 1}
	for k, v := range settings {
		if v == nil {
			delete(cfg, k)
			continue
		}
		cfg[k] = v
	}
	return fakeClassConfig{classConfig: cfg}
}

type fakeClassConfig struct {
	classConfig map[string]any
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	return nil
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}
