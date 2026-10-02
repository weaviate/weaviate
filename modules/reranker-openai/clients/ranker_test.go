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

const testQuery = "how do I reset my password"

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func newTestClient(apiKey string) *client {
	c := New(apiKey, 0, DefaultMaxConcurrentRequests, nullLogger())
	c.retryBackoff = time.Millisecond
	return c
}

// answer is what the fake API returns for a document: a predicate
// probability, or a refusal.
type answer struct {
	probability float64
	refused     bool
}

func probability(p float64) answer {
	return answer{probability: p}
}

func refusal() answer {
	return answer{refused: true}
}

func TestScore(t *testing.T) {
	tests := []struct {
		name         string
		response     string
		want         float64
		wantAnswered bool
		wantErr      string
	}{
		{
			name:         "predicate",
			response:     `{"answers":[{"type":"predicate","name":"relevance","probability":0.92}]}`,
			want:         0.92,
			wantAnswered: true,
		},
		{
			name:         "probability zero",
			response:     `{"answers":[{"type":"predicate","name":"relevance","probability":0}]}`,
			want:         0,
			wantAnswered: true,
		},
		{
			name:         "probability one",
			response:     `{"answers":[{"type":"predicate","name":"relevance","probability":1.0}]}`,
			want:         1,
			wantAnswered: true,
		},
		{
			name:     "refusal",
			response: `{"answers":[{"type":"refusal","name":"relevance"}]}`,
			want:     0,
		},
		{
			name:     "no answers",
			response: `{"answers":[]}`,
			wantErr:  "expected one answer in response, got 0",
		},
		{
			name:     "answers missing",
			response: `{"model":"gpt-6-luna"}`,
			wantErr:  "expected one answer in response, got 0",
		},
		{
			name: "two answers",
			response: `{"answers":[{"type":"predicate","name":"relevance","probability":0.9},` +
				`{"type":"predicate","name":"relevance","probability":0.1}]}`,
			wantErr: "expected one answer in response, got 2",
		},
		{
			name:     "answer to another question",
			response: `{"answers":[{"type":"predicate","name":"other","probability":0.9}]}`,
			wantErr:  `answer in response is for "other", not for "relevance"`,
		},
		{
			name:     "answer without a name",
			response: `{"answers":[{"type":"predicate","name":null,"probability":0.9}]}`,
			wantErr:  `answer in response is for no name, not for "relevance"`,
		},
		{
			name:     "refusal to another question",
			response: `{"answers":[{"type":"refusal","name":"other"}]}`,
			wantErr:  `answer in response is for "other", not for "relevance"`,
		},
		{
			name:     "predicate without a probability",
			response: `{"answers":[{"type":"predicate","name":"relevance"}]}`,
			wantErr:  "no probability in response",
		},
		{
			name:     "null probability",
			response: `{"answers":[{"type":"predicate","name":"relevance","probability":null}]}`,
			wantErr:  "no probability in response",
		},
		{
			name:     "probability above one",
			response: `{"answers":[{"type":"predicate","name":"relevance","probability":1.5}]}`,
			wantErr:  "probability 1.5 in response is outside [0, 1]",
		},
		{
			name:     "negative probability",
			response: `{"answers":[{"type":"predicate","name":"relevance","probability":-0.1}]}`,
			wantErr:  "probability -0.1 in response is outside [0, 1]",
		},
		{
			name:     "choice answer",
			response: `{"answers":[{"type":"choice","name":"relevance","choice":"yes","confidence":0.9}]}`,
			wantErr:  `unexpected answer type "choice" in response`,
		},
		{
			name:     "answer without a type",
			response: `{"answers":[{"name":"relevance","probability":0.9}]}`,
			wantErr:  `unexpected answer type "" in response`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var response decisionResponse
			require.NoError(t, json.Unmarshal([]byte(tt.response), &response))

			got, answered, err := response.score(relevanceName)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tt.want, got, 1e-9)
			assert.Equal(t, tt.wantAnswered, answered)
		})
	}
}

func TestRankScoresInInputOrder(t *testing.T) {
	documents := []string{"reset it in settings", "our office hours", "password rules", "click forgot password"}
	handler := &openAIHandler{t: t, answers: map[string]answer{
		"reset it in settings":  probability(0.8),
		"our office hours":      probability(0.01),
		"password rules":        probability(0.5),
		"click forgot password": probability(0.9),
	}}
	server := httptest.NewServer(handler)
	defer server.Close()

	res, err := newTestClient("apiKey").Rank(context.Background(), testQuery, documents, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.Equal(t, testQuery, res.Query)
	require.Len(t, res.DocumentScores, len(documents))
	for i, want := range []float64{0.8, 0.01, 0.5, 0.9} {
		assert.Equal(t, documents[i], res.DocumentScores[i].Document)
		assert.InDelta(t, want, res.DocumentScores[i].Score, 1e-9)
	}
	assert.Len(t, handler.received(), len(documents))
}

func TestRankRequestShape(t *testing.T) {
	tests := []struct {
		name      string
		envKey    string
		ctx       context.Context
		settings  map[string]any
		wantAuth  string
		wantModel string
	}{
		{
			name:      "api key from the environment, default model",
			envKey:    "envKey",
			ctx:       context.Background(),
			wantAuth:  "Bearer envKey",
			wantModel: "gpt-6-luna",
		},
		{
			name:      "api key from the header wins",
			envKey:    "envKey",
			ctx:       context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"headerKey"}),
			wantAuth:  "Bearer headerKey",
			wantModel: "gpt-6-luna",
		},
		{
			name:      "api key from the header only",
			ctx:       context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"headerKey"}),
			wantAuth:  "Bearer headerKey",
			wantModel: "gpt-6-luna",
		},
		{
			name:      "model from the class settings",
			envKey:    "envKey",
			ctx:       context.Background(),
			settings:  map[string]any{"model": "gpt-7-luna"},
			wantAuth:  "Bearer envKey",
			wantModel: "gpt-7-luna",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.5)}}
			server := httptest.NewServer(handler)
			defer server.Close()

			_, err := newTestClient(tt.envKey).Rank(tt.ctx, testQuery, []string{"doc"}, classConfig(server.URL, tt.settings))

			require.NoError(t, err)
			requests := handler.received()
			require.Len(t, requests, 1)
			request := requests[0]
			assert.Equal(t, http.MethodPost, request.method)
			assert.Equal(t, "/v1/decisions", request.path)
			assert.Equal(t, tt.wantAuth, request.authorization)
			assert.Equal(t, "application/json", request.contentType)
			assert.JSONEq(t, `{
				"model": "`+tt.wantModel+`",
				"input": "doc",
				"questions": [{
					"type": "predicate",
					"name": "relevance",
					"instructions": "Does the document answer or directly address this query? Query: how do I reset my password"
				}]
			}`, request.rawBody)
		})
	}
}

func TestRankStatementQuestion(t *testing.T) {
	handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.9)}}
	server := httptest.NewServer(handler)
	defer server.Close()

	res, err := newTestClient("key").Rank(context.Background(), "the customer is angry", []string{"doc"},
		classConfig(server.URL, map[string]any{"question": "statement"}))

	require.NoError(t, err)
	assert.InDelta(t, 0.9, res.DocumentScores[0].Score, 1e-9)
	requests := handler.received()
	require.Len(t, requests, 1)
	assert.JSONEq(t, `{
		"model": "gpt-6-luna",
		"input": "doc",
		"questions": [{
			"type": "predicate",
			"name": "statement",
			"instructions": "the customer is angry"
		}]
	}`, requests[0].rawBody)
}

// The API echoes the question's name. An answer named for the other
// question is a mismatch, whichever question was asked.
func TestRankChecksTheAnswerName(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		answered string
		wantErr  string
	}{
		{name: "relevance answered", answered: "relevance"},
		{name: "statement answered", settings: map[string]any{"question": "statement"}, answered: "statement"},
		{
			name:     "relevance asked, statement answered",
			answered: "statement",
			wantErr:  `answer in response is for "statement", not for "relevance"`,
		},
		{
			name:     "statement asked, relevance answered",
			settings: map[string]any{"question": "statement"},
			answered: "relevance",
			wantErr:  `answer in response is for "relevance", not for "statement"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.4)}, answerName: tt.answered}
			server := httptest.NewServer(handler)
			defer server.Close()

			res, err := newTestClient("key").Rank(context.Background(), testQuery, []string{"doc"},
				classConfig(server.URL, tt.settings))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, 0.4, res.DocumentScores[0].Score, 1e-9)
		})
	}
}

func TestRankBaseURLFromHeader(t *testing.T) {
	tests := []struct {
		name      string
		envKey    string
		headerKey string
		wantAuth  string
		wantErr   string
	}{
		{
			name:      "the header key is sent to the header base URL",
			envKey:    "envKey",
			headerKey: "headerKey",
			wantAuth:  "Bearer headerKey",
		},
		{
			name:      "header key without a server key",
			headerKey: "headerKey",
			wantAuth:  "Bearer headerKey",
		},
		{
			name:    "the server key is not sent to the header base URL",
			envKey:  "envKey",
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "no key at all",
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.5)}}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx := context.WithValue(context.Background(), "X-Openai-Baseurl", []string{server.URL})
			if tt.headerKey != "" {
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{tt.headerKey})
			}

			_, err := newTestClient(tt.envKey).Rank(ctx, testQuery, []string{"doc"},
				classConfig("https://unreachable.invalid", nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, handler.received())
				return
			}
			require.NoError(t, err)
			requests := handler.received()
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantAuth, requests[0].authorization)
		})
	}
}

func TestRankBaseURLForms(t *testing.T) {
	tests := []struct {
		name   string
		suffix string
		// fromHeader passes the base URL in X-Openai-Baseurl instead of the
		// class setting.
		fromHeader bool
		wantPath   string
	}{
		{name: "host only", suffix: "", wantPath: "/v1/decisions"},
		{name: "trailing slash", suffix: "/", wantPath: "/v1/decisions"},
		{name: "v1", suffix: "/v1", wantPath: "/v1/decisions"},
		{name: "v1 and trailing slash", suffix: "/v1/", wantPath: "/v1/decisions"},
		{name: "path prefix", suffix: "/openai", wantPath: "/openai/v1/decisions"},
		{name: "path prefix and v1", suffix: "/openai/v1/", wantPath: "/openai/v1/decisions"},
		{name: "path that only ends in the letters v1", suffix: "/gatewayv1", wantPath: "/gatewayv1/v1/decisions"},
		{name: "v1 from the header", suffix: "/v1", fromHeader: true, wantPath: "/v1/decisions"},
		{name: "v1 and trailing slash from the header", suffix: "/v1/", fromHeader: true, wantPath: "/v1/decisions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.5)}}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx := context.Background()
			cfg := classConfig(server.URL+tt.suffix, nil)
			if tt.fromHeader {
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{server.URL + tt.suffix})
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				cfg = classConfig("https://unreachable.invalid", nil)
			}

			_, err := newTestClient("apiKey").Rank(ctx, testQuery, []string{"doc"}, cfg)

			require.NoError(t, err)
			requests := handler.received()
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantPath, requests[0].path)
		})
	}
}

func TestRankDocumentsSent(t *testing.T) {
	tests := []struct {
		name      string
		documents []string
		wantSent  []string
		want      []float64
		wantErr   string
	}{
		{
			name:      "one request per distinct document",
			documents: []string{"a", "b", "c"},
			wantSent:  []string{"a", "b", "c"},
			want:      []float64{0.9, 0.2, 0.6},
		},
		{
			name:      "identical documents share one request",
			documents: []string{"a", "b", "a", "a"},
			wantSent:  []string{"a", "b"},
			want:      []float64{0.9, 0.2, 0.9, 0.9},
		},
		{
			name:      "empty document is not sent and scores zero",
			documents: []string{"", "a", ""},
			wantSent:  []string{"a"},
			want:      []float64{0, 0.9, 0},
		},
		{
			name:      "only empty documents",
			documents: []string{"", ""},
			wantErr:   "no document has text: check the rerank property",
		},
		{
			name:      "no documents",
			documents: []string{},
			want:      []float64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{
				"a": probability(0.9), "b": probability(0.2), "c": probability(0.6),
			}}
			server := httptest.NewServer(handler)
			defer server.Close()

			res, err := newTestClient("apiKey").Rank(context.Background(), testQuery, tt.documents,
				classConfig(server.URL, nil))

			assert.ElementsMatch(t, tt.wantSent, handler.documents())
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, res.DocumentScores, len(tt.documents))
			for i, want := range tt.want {
				assert.Equal(t, tt.documents[i], res.DocumentScores[i].Document)
				assert.InDelta(t, want, res.DocumentScores[i].Score, 1e-9)
			}
		})
	}
}

func TestRankRejectedBeforeAnyRequest(t *testing.T) {
	const baseURLErr = `must be a URL with an http or https scheme and a host, such as https://api.openai.com, got `
	tests := []struct {
		name      string
		apiKey    string
		query     string
		documents []string
		settings  map[string]any
		headers   map[string]string
		wantErr   string
	}{
		{
			name:      "unknown question",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"question": "summary"},
			wantErr:   `question must be "relevance" or "statement", got "summary"`,
		},
		{
			name:      "empty question",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"question": ""},
			wantErr:   `question must be "relevance" or "statement", got ""`,
		},
		{
			name:      "empty model",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"model": ""},
			wantErr:   "no model provided",
		},
		{
			name:      "model of the wrong type",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"model": json.Number("4")},
			wantErr:   "model must be a string, got a number",
		},
		{
			name:      "question of the wrong type",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"question": true},
			wantErr:   "question must be a string, got a boolean",
		},
		{
			name:      "baseURL of the wrong type",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"baseURL": []any{"https://api.openai.com"}},
			wantErr:   "baseURL must be a string, got a list",
		},
		{
			name:      "maxDocuments with a fraction",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"maxDocuments": json.Number("2.5")},
			wantErr:   "maxDocuments must be a whole number between 1 and 1000, got 2.5",
		},
		{
			name:      "maxDocuments of the wrong type",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"maxDocuments": true},
			wantErr:   "maxDocuments must be a whole number between 1 and 1000, got true",
		},
		{
			name:      "class baseURL without a scheme",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			settings:  map[string]any{"baseURL": "api.openai.com"},
			wantErr:   "baseURL " + baseURLErr + `"api.openai.com"`,
		},
		{
			name:      "header baseURL without a scheme",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			headers:   map[string]string{"X-Openai-Baseurl": "api.openai.com/v1", "X-Openai-Api-Key": "headerKey"},
			wantErr:   "X-Openai-Baseurl " + baseURLErr + `"api.openai.com/v1"`,
		},
		{
			name:      "header baseURL without a host",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a"},
			headers:   map[string]string{"X-Openai-Baseurl": "https:///v1", "X-Openai-Api-Key": "headerKey"},
			wantErr:   "X-Openai-Baseurl " + baseURLErr + `"https:///v1"`,
		},
		{
			name:      "more documents than maxDocuments",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: []string{"a", "b", "c"},
			settings:  map[string]any{"maxDocuments": 2},
			wantErr:   "3 documents exceed maxDocuments 2",
		},
		{
			name:      "more documents than the default maxDocuments",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: make([]string, 101),
			wantErr:   "101 documents exceed maxDocuments 100",
		},
		{
			name:      "maxDocuments setting above the limit is capped",
			apiKey:    "apiKey",
			query:     testQuery,
			documents: make([]string, 1001),
			settings:  map[string]any{"maxDocuments": 5000},
			wantErr:   "1001 documents exceed maxDocuments 1000",
		},
		{
			name:      "no query",
			apiKey:    "apiKey",
			documents: []string{"a"},
			wantErr:   "no query provided",
		},
		{
			name:      "no api key",
			query:     testQuery,
			documents: []string{"a"},
			wantErr: "OpenAI API Key: no api key found neither in request header: X-Openai-Api-Key " +
				"nor in environment variable under OPENAI_APIKEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"a": probability(0.9)}}
			server := httptest.NewServer(handler)
			defer server.Close()

			ctx := context.Background()
			for header, value := range tt.headers {
				ctx = context.WithValue(ctx, header, []string{value})
			}

			_, err := newTestClient(tt.apiKey).Rank(ctx, tt.query, tt.documents,
				classConfig(server.URL, tt.settings))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, handler.received())
		})
	}
}

func TestRankAcceptsMaxDocuments(t *testing.T) {
	handler := &openAIHandler{t: t, answers: map[string]answer{"a": probability(0.9), "b": probability(0.2)}}
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := newTestClient("apiKey").Rank(context.Background(), testQuery, []string{"a", "b"},
		classConfig(server.URL, map[string]any{"maxDocuments": 2}))

	require.NoError(t, err)
	assert.Len(t, handler.received(), 2)
}

func TestRankErrorResponses(t *testing.T) {
	const openAIError = `{"error":{"message":"The model does not exist","type":"invalid_request_error","code":"model_not_found"}}`
	tests := []struct {
		name string
		// failures are answered in order before the handler starts to succeed.
		failures     []failure
		wantRequests int
		wantErr      string
	}{
		{
			name:         "429 then success",
			failures:     []failure{{status: 429, body: `{"error":{"message":"Rate limit reached"}}`}},
			wantRequests: 2,
		},
		{
			name:         "503 twice then success",
			failures:     []failure{{status: 503}, {status: 503}},
			wantRequests: 3,
		},
		{
			name: "500 until the attempts run out",
			failures: []failure{
				{status: 500, body: `{"error":{"message":"The server had an error"}}`},
				{status: 500, body: `{"error":{"message":"The server had an error"}}`},
				{status: 500, body: `{"error":{"message":"The server had an error"}}`},
				{status: 500, body: `{"error":{"message":"The server had an error"}}`},
			},
			wantRequests: 4,
			wantErr:      "after 4 attempts: connection to OpenAI API failed with status 500: The server had an error",
		},
		{
			name:         "400 is not retried",
			failures:     []failure{{status: 400, body: openAIError}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 400: The model does not exist",
		},
		{
			name:         "404 is not retried",
			failures:     []failure{{status: 404, body: openAIError}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 404: The model does not exist",
		},
		{
			name:         "401 hides the body",
			failures:     []failure{{status: 401, body: `{"error":{"message":"Incorrect API key provided: sk-abc***xyz"}}`}},
			wantRequests: 1,
			wantErr:      "OpenAI rejected the API key (status 401)",
		},
		{
			name:         "403 hides the body",
			failures:     []failure{{status: 403, body: `{"error":{"message":"Country not supported"}}`}},
			wantRequests: 1,
			wantErr:      "OpenAI rejected the API key (status 403)",
		},
		{
			name:         "429 for an exhausted quota is not retried",
			failures:     []failure{{status: 429, body: `{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 429: You exceeded your current quota",
		},
		{
			name:         "429 for an exhausted quota by type only",
			failures:     []failure{{status: 429, body: `{"error":{"message":"You exceeded your current quota","type":"insufficient_quota"}}`}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 429: You exceeded your current quota",
		},
		{
			name:         "error body that is not JSON",
			failures:     []failure{{status: 400, body: `<html>Bad Request</html>`}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 400",
		},
		{
			name:         "error body with a numeric code",
			failures:     []failure{{status: 400, body: `{"error":{"message":"bad request","code":400}}`}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 400: bad request",
		},
		{
			name:         "long error message is cut",
			failures:     []failure{{status: 400, body: `{"error":{"message":"` + strings.Repeat("x", 2000) + `"}}`}},
			wantRequests: 1,
			wantErr:      "connection to OpenAI API failed with status 400: " + strings.Repeat("x", 512),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.9)}, failures: tt.failures}
			server := httptest.NewServer(handler)
			defer server.Close()

			res, err := newTestClient("apiKey").Rank(context.Background(), testQuery, []string{"doc"},
				classConfig(server.URL, nil))

			assert.Len(t, handler.received(), tt.wantRequests)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, 0.9, res.DocumentScores[0].Score, 1e-9)
		})
	}
}

func TestRankMalformedResponses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "no answers", body: `{"answers":[],"usage":{"input_tokens":5}}`, wantErr: "expected one answer in response, got 0"},
		{name: "no probability", body: `{"answers":[{"type":"predicate","name":"relevance"}]}`, wantErr: "no probability in response"},
		{
			name:    "probability above one",
			body:    `{"answers":[{"type":"predicate","name":"relevance","probability":2}]}`,
			wantErr: "probability 2 in response is outside [0, 1]",
		},
		{
			name:    "error object",
			body:    `{"error":{"message":"The model is overloaded","type":"server_error"}}`,
			wantErr: "OpenAI API returned an error: The model is overloaded",
		},
		{
			name:    "error object next to answers",
			body:    `{"answers":[],"error":{"message":"The model is overloaded"}}`,
			wantErr: "OpenAI API returned an error: The model is overloaded",
		},
		{
			name:    "long error message is cut",
			body:    `{"error":{"message":"` + strings.Repeat("x", 2000) + `"}}`,
			wantErr: "OpenAI API returned an error: " + strings.Repeat("x", 512),
		},
		{
			name:    "error object without a message",
			body:    `{"error":{"type":"server_error"}}`,
			wantErr: "OpenAI API returned an error without a message",
		},
		{
			name:    "null error is ignored",
			body:    `{"error":null,"answers":[]}`,
			wantErr: "expected one answer in response, got 0",
		},
		{
			name: "body at the size limit",
			body: `{"answers":[{"type":"predicate","name":"relevance","probability":0.5}]}` +
				strings.Repeat(" ", maxResponseBodyBytes),
			wantErr: "response body reaches the limit of 1048576 bytes",
		},
		{name: "not JSON", body: `<html>`, wantErr: "parse response: invalid character '<' looking for beginning of value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &openAIHandler{t: t, rawResponse: tt.body}
			server := httptest.NewServer(handler)
			defer server.Close()

			_, err := newTestClient("apiKey").Rank(context.Background(), testQuery, []string{"doc"},
				classConfig(server.URL, nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, handler.received(), 1, "a malformed response is not retried")
		})
	}
}

func TestRankLogsUsage(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	handler := &openAIHandler{
		t: t,
		answers: map[string]answer{
			"a": probability(0.9),
			"b": refusal(),
			"c": probability(0.4),
		},
		usage: decisionUsage{InputTokens: 70},
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 0, DefaultMaxConcurrentRequests, logger)

	res, err := c.Rank(context.Background(), testQuery, []string{"a", "b", "c", "a", ""}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.Equal(t, ent.DocumentScore{Document: "b", Score: 0}, res.DocumentScores[1])
	finished := entriesWithMessage(hook, "openai rerank finished")
	require.Len(t, finished, 1)
	assert.Equal(t, logrus.DebugLevel, finished[0].Level)
	assert.Equal(t, logrus.Fields{
		"action":        "reranker_openai_rank",
		"documents":     5,
		"requests":      int64(3),
		"unanswered":    int64(1),
		"input_tokens":  int64(210),
		"output_tokens": int64(0),
	}, finished[0].Data)
}

func TestRankRefusalScoresZero(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	handler := &openAIHandler{t: t, answers: map[string]answer{"a": probability(0.75), "b": refusal()}}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 0, DefaultMaxConcurrentRequests, logger)

	res, err := c.Rank(context.Background(), testQuery, []string{"a", "b"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.InDelta(t, 0.75, res.DocumentScores[0].Score, 1e-9)
	assert.Equal(t, ent.DocumentScore{Document: "b", Score: 0}, res.DocumentScores[1])
	entries := entriesWithMessage(hook, "openai rerank finished")
	require.Len(t, entries, 1)
	assert.Equal(t, int64(1), entries[0].Data["unanswered"])
}

func TestRankLogsUsageOnFailure(t *testing.T) {
	const badRequest = `{"error":{"message":"The model does not exist"}}`
	tests := []struct {
		name      string
		documents []string
		failures  map[string]failure
		// wantFields is nil when no usage line is expected.
		wantFields logrus.Fields
	}{
		{
			name:      "failure after two answered documents",
			documents: []string{"a", "b", "c", "d"},
			failures:  map[string]failure{"c": {status: 400, body: badRequest}},
			wantFields: logrus.Fields{
				"action":        "reranker_openai_rank",
				"documents":     4,
				"requests":      int64(2),
				"unanswered":    int64(1),
				"input_tokens":  int64(140),
				"output_tokens": int64(0),
			},
		},
		{
			name:      "failure of the first document",
			documents: []string{"a", "b"},
			failures:  map[string]failure{"a": {status: 400, body: badRequest}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			handler := &openAIHandler{
				t: t,
				answers: map[string]answer{
					"a": probability(0.9),
					"b": refusal(),
					"c": probability(0.4),
					"d": probability(0.4),
				},
				usage:            decisionUsage{InputTokens: 70},
				documentFailures: tt.failures,
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			// One request at a time, so the documents are judged in order.
			c := New("apiKey", 0, 1, logger)

			_, err := c.Rank(context.Background(), testQuery, tt.documents, classConfig(server.URL, nil))

			require.EqualError(t, err, "connection to OpenAI API failed with status 400: The model does not exist")
			assert.Empty(t, entriesWithMessage(hook, "openai rerank finished"))
			failed := entriesWithMessage(hook, "openai rerank failed")
			if tt.wantFields == nil {
				assert.Empty(t, failed)
				return
			}
			require.Len(t, failed, 1)
			assert.Equal(t, logrus.DebugLevel, failed[0].Level)
			assert.Equal(t, tt.wantFields, failed[0].Data)
		})
	}
}

func entriesWithMessage(hook *test.Hook, message string) []*logrus.Entry {
	var entries []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Message == message {
			entries = append(entries, entry)
		}
	}
	return entries
}

func TestRankBoundsRequestsInFlight(t *testing.T) {
	const limit = 3
	documents := make([]string, 30)
	answers := make(map[string]answer, len(documents))
	for i := range documents {
		documents[i] = fmt.Sprintf("doc %d", i)
		answers[documents[i]] = probability(0.5)
	}
	handler := &openAIHandler{t: t, answers: answers, delay: 10 * time.Millisecond}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 0, limit, nullLogger())

	// Two queries at once: the limit is for the process, not per query.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Rank(context.Background(), testQuery, documents, classConfig(server.URL, nil))
		}()
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Len(t, handler.received(), 2*len(documents))
	assert.LessOrEqual(t, handler.maxInFlight(), limit)
	assert.Greater(t, handler.maxInFlight(), 1, "requests should run in parallel")
}

func TestRankDoesNotRetryATimeout(t *testing.T) {
	handler := &openAIHandler{t: t, answers: map[string]answer{"doc": probability(0.5)}, delay: 300 * time.Millisecond}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := New("apiKey", 50*time.Millisecond, DefaultMaxConcurrentRequests, nullLogger())
	c.retryBackoff = time.Millisecond

	start := time.Now()
	_, err := c.Rank(context.Background(), testQuery, []string{"doc"}, classConfig(server.URL, nil))

	require.Error(t, err)
	assert.Less(t, time.Since(start), 250*time.Millisecond, "a retry would wait for a second timeout")
	// The handler records a request when it arrives, before its delay.
	assert.Len(t, handler.received(), 1)
}

func TestRankStopsOnContextCancel(t *testing.T) {
	handler := &openAIHandler{t: t, failures: []failure{{status: 500}, {status: 500}, {status: 500}, {status: 500}}}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := newTestClient("apiKey")
	c.retryBackoff = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.Rank(ctx, testQuery, []string{"doc"}, classConfig(server.URL, nil))

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, handler.received(), 1)
}

func TestRetryDelay(t *testing.T) {
	c := newTestClient("apiKey")
	c.retryBackoff = 100 * time.Millisecond
	tests := []struct {
		name       string
		attempt    int
		retryAfter time.Duration
		wantMin    time.Duration
		wantMax    time.Duration
	}{
		{name: "first retry", attempt: 1, wantMin: 50 * time.Millisecond, wantMax: 100 * time.Millisecond},
		{name: "third retry", attempt: 3, wantMin: 200 * time.Millisecond, wantMax: 400 * time.Millisecond},
		{name: "Retry-After wins when longer", attempt: 1, retryAfter: 2 * time.Second, wantMin: 2 * time.Second, wantMax: 2 * time.Second},
		{name: "Retry-After is capped", attempt: 1, retryAfter: time.Hour, wantMin: 10 * time.Second, wantMax: 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 50 {
				delay := c.retryDelay(tt.attempt, tt.retryAfter)
				assert.GreaterOrEqual(t, delay, tt.wantMin)
				assert.LessOrEqual(t, delay, tt.wantMax)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		header string
		want   time.Duration
	}{
		{header: "", want: 0},
		{header: "3", want: 3 * time.Second},
		{header: "0", want: 0},
		{header: "-1", want: 0},
		{header: "Wed, 21 Oct 2026 07:28:00 GMT", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			assert.Equal(t, tt.want, parseRetryAfter(tt.header))
		})
	}
}

func TestRankHonoursRetryAfter(t *testing.T) {
	handler := &openAIHandler{
		t:        t,
		answers:  map[string]answer{"doc": probability(0.9)},
		failures: []failure{{status: 429, retryAfter: "1"}},
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	start := time.Now()
	_, err := newTestClient("apiKey").Rank(context.Background(), testQuery, []string{"doc"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), time.Second)
	assert.Len(t, handler.received(), 2)
}

func TestMetaInfo(t *testing.T) {
	meta, err := newTestClient("apiKey").MetaInfo()

	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"name":              "Reranker - OpenAI",
		"documentationHref": "https://developers.openai.com/api/docs/guides/decisions",
	}, meta)
}

type failure struct {
	status     int
	body       string
	retryAfter string
}

type receivedRequest struct {
	method        string
	path          string
	authorization string
	contentType   string
	rawBody       string
	document      string
}

// openAIHandler is a fake Decisions endpoint. It answers a request with the
// answer stored for the document in its input.
type openAIHandler struct {
	t       *testing.T
	answers map[string]answer
	usage   decisionUsage
	// answerName, when set, is the name of every answer. Empty echoes the
	// name of the question asked, as the API does.
	answerName string
	// rawResponse, when set, is the body of every 200 response.
	rawResponse string
	// documentFailures are answered for their document on every request.
	documentFailures map[string]failure
	delay            time.Duration

	mu          sync.Mutex
	failures    []failure
	requests    []receivedRequest
	inFlight    int
	maxObserved int
}

func (h *openAIHandler) received() []receivedRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]receivedRequest(nil), h.requests...)
}

func (h *openAIHandler) documents() []string {
	var out []string
	for _, request := range h.received() {
		out = append(out, request.document)
	}
	return out
}

func (h *openAIHandler) maxInFlight() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxObserved
}

func (h *openAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(r.Body)
	if !assert.NoError(h.t, err) {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var body decisionRequest
	if !assert.NoError(h.t, json.Unmarshal(rawBody, &body)) || !assert.Len(h.t, body.Questions, 1) {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	document := body.Input

	h.mu.Lock()
	h.requests = append(h.requests, receivedRequest{
		method:        r.Method,
		path:          r.URL.Path,
		authorization: r.Header.Get("Authorization"),
		contentType:   r.Header.Get("Content-Type"),
		rawBody:       string(rawBody),
		document:      document,
	})
	h.inFlight++
	h.maxObserved = max(h.maxObserved, h.inFlight)
	var failed *failure
	if len(h.failures) > 0 {
		failed = &h.failures[0]
		h.failures = h.failures[1:]
	} else if documentFailure, ok := h.documentFailures[document]; ok {
		failed = &documentFailure
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.inFlight--
		h.mu.Unlock()
	}()

	if h.delay > 0 {
		time.Sleep(h.delay)
	}

	if failed != nil {
		if failed.retryAfter != "" {
			w.Header().Set("Retry-After", failed.retryAfter)
		}
		w.WriteHeader(failed.status)
		_, _ = w.Write([]byte(failed.body))
		return
	}
	if h.rawResponse != "" {
		_, _ = w.Write([]byte(h.rawResponse))
		return
	}
	stored, ok := h.answers[document]
	if !assert.True(h.t, ok, "unexpected document %q", document) {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	name := body.Questions[0].Name
	if h.answerName != "" {
		name = h.answerName
	}
	answered := map[string]any{"type": "predicate", "name": name, "probability": stored.probability}
	if stored.refused {
		answered = map[string]any{"type": "refusal", "name": name}
	}
	response := map[string]any{
		"model":   body.Model,
		"answers": []map[string]any{answered},
		"usage":   h.usage,
	}
	assert.NoError(h.t, json.NewEncoder(w).Encode(response))
}

func classConfig(baseURL string, settings map[string]any) moduletools.ClassConfig {
	cfg := map[string]any{"baseURL": baseURL}
	for k, v := range settings {
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
