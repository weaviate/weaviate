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
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

var (
	angryQuestion = ent.DecisionQuestion{Name: "angry", Property: "text", Instructions: "the customer is angry", Kind: ent.DecisionPredicate}
	teamQuestion  = ent.DecisionQuestion{
		Name: "team", Property: "text", Instructions: "which team should handle this?", Kind: ent.DecisionChoice,
		Options: []ent.DecisionOption{{Value: "billing", Description: "payments and refunds"}, {Value: "other"}},
	}
	urgencyQuestion = ent.DecisionQuestion{
		Name: "urgency", Property: "text", Instructions: "how urgent is this?", Kind: ent.DecisionScore,
		Levels: []ent.DecisionLevel{{Label: "can wait"}, {Label: "today", Description: "before the end of the day"}, {Label: "now"}},
	}
	allQuestions = []ent.DecisionQuestion{angryQuestion, teamQuestion, urgencyQuestion}
)

// decideHandler fakes the TypeSafeAI API for decide requests. It answers a
// noul with scores[document], a choice with choices[document] (the first
// option when unset) and a score with scores[document] spread over the two
// nearest levels.
type decideHandler struct {
	t       *testing.T
	scores  map[string]float64
	choices map[string]string
	// positions are the score answers; scores[document] when unset.
	positions map[string]float64
	rawBody   string
	statuses  []int
	// omitKey leaves one question of a request without an answer.
	omitKey string
	// maxDocumentsPerRequest makes the handler answer 400 max_tokens_exceeded
	// to a larger request; 0 accepts every request.
	maxDocumentsPerRequest int

	lock     sync.Mutex
	requests []typesafeaiRequest
}

func (h *decideHandler) received() []typesafeaiRequest {
	h.lock.Lock()
	defer h.lock.Unlock()
	return append([]typesafeaiRequest(nil), h.requests...)
}

func (h *decideHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	assert.NoError(h.t, err)
	var req typesafeaiRequest
	assert.NoError(h.t, json.Unmarshal(bodyBytes, &req))
	assert.Equal(h.t, "/v1/systemone", r.URL.Path)

	h.lock.Lock()
	status := http.StatusOK
	if n := len(h.requests); n < len(h.statuses) {
		status = h.statuses[n]
	}
	h.requests = append(h.requests, req)
	h.lock.Unlock()

	documents := map[string]string{}
	switch state := req.State.(type) {
	case string:
		documents[""] = state
	case map[string]any:
		for key, document := range state {
			documents[key+"_"] = document.(string)
		}
	default:
		assert.Fail(h.t, "state must be a string or a map", "%T", req.State)
	}
	if limit := h.maxDocumentsPerRequest; limit != 0 && len(documents) > limit {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":{"error_type":"max_tokens_exceeded"}}`))
		return
	}
	if status != http.StatusOK {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"error from jev"}`))
		return
	}
	if h.rawBody != "" {
		_, _ = w.Write([]byte(h.rawBody))
		return
	}

	answers := map[string]typesafeaiAnswer{}
	for key, question := range req.Questions {
		if key == h.omitKey {
			continue
		}
		prefix := ""
		for documentKey := range documents {
			if documentKey != "" && strings.HasPrefix(key, documentKey) {
				prefix = documentKey
			}
		}
		document, ok := documents[prefix]
		assert.True(h.t, ok, "question %q names no document", key)
		if prefix != "" {
			assert.True(h.t, strings.HasPrefix(question.Instructions, "In "+strings.TrimSuffix(prefix, "_")+": "),
				"question %q must name its document", key)
		}
		answers[key] = h.answer(question, document)
	}
	assert.NoError(h.t, json.NewEncoder(w).Encode(typesafeaiResponse{Answers: answers, Usage: typesafeaiUsage{InputTokens: 10}}))
}

func (h *decideHandler) answer(question typesafeaiQuestion, document string) typesafeaiAnswer {
	value := h.scores[document]
	confidence := 0.6
	switch question.Type {
	case choiceType:
		criteria, ok := question.Criteria.(map[string]any)
		assert.True(h.t, ok, "choice criteria must be a map, got %T", question.Criteria)
		options := make([]string, 0, len(criteria))
		for option := range criteria {
			options = append(options, option)
		}
		sort.Strings(options)
		choice := h.choices[document]
		if choice == "" {
			choice = options[0]
		}
		probabilities := map[string]float64{}
		for _, option := range options {
			probabilities[option] = 0.3 / float64(len(options)-1)
		}
		probabilities[choice] = 0.7
		return typesafeaiAnswer{Type: choiceType, Choice: &choice, Probabilities: probabilities, Confidence: &confidence}
	case scoreType:
		levels, ok := question.Criteria.([]any)
		assert.True(h.t, ok, "score criteria must be a list, got %T", question.Criteria)
		if position, ok := h.positions[document]; ok {
			value = position
		}
		probabilities := map[string]float64{}
		for i := range levels {
			probabilities[fmt.Sprint(i)] = 0
		}
		lower := math.Floor(value)
		probabilities[fmt.Sprint(int(lower))] = 1 - (value - lower)
		if value != lower {
			probabilities[fmt.Sprint(int(lower)+1)] = value - lower
		}
		return typesafeaiAnswer{Type: scoreType, Score: &value, Probabilities: probabilities, Confidence: &confidence}
	default:
		assert.Nil(h.t, question.Criteria, "a noul has no criteria")
		return typesafeaiAnswer{Type: noulType, Noul: &value}
	}
}

func decideTestClient(t *testing.T, handler *decideHandler) (*client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return newTestClient("apiKey"), server
}

func TestDecideRequestShape(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a document": 0.9}}
	c, server := decideTestClient(t, handler)

	_, err := c.Decide(context.Background(), allQuestions, []string{"a document"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	requests := handler.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "jev-latest", requests[0].Model)
	assert.Equal(t, "a document", requests[0].State)
	require.Len(t, requests[0].Questions, 3)
	assert.Equal(t, typesafeaiQuestion{Type: "noul", Instructions: "the customer is angry"}, requests[0].Questions["q0"])
	assert.Equal(t, typesafeaiQuestion{
		Type: "choice", Instructions: "which team should handle this?",
		Criteria: map[string]any{"billing": "payments and refunds", "other": nil},
	}, requests[0].Questions["q1"])
	assert.Equal(t, typesafeaiQuestion{
		Type: "score", Instructions: "how urgent is this?",
		Criteria: []any{"can wait", "today: before the end of the day", "now"},
	}, requests[0].Questions["q2"])
}

func TestDecideAnswers(t *testing.T) {
	handler := &decideHandler{
		t:         t,
		scores:    map[string]float64{"a document": 0.9, "another": 0.5},
		positions: map[string]float64{"another": 1.5},
		choices:   map[string]string{"another": "other"},
	}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), allQuestions, []string{"a document", "another"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, out[0], 3)
	assert.Equal(t, ent.DecisionAnswer{Name: "angry", Kind: ent.DecisionPredicate, Probability: 0.9}, out[0][0])
	assert.Equal(t, ent.DecisionAnswer{
		Name: "team", Kind: ent.DecisionChoice, Choice: "billing", Confidence: 0.6,
		Probabilities: []ent.DecisionProbability{{Value: "billing", Probability: 0.7}, {Value: "other", Probability: 0.3}},
	}, out[0][1])
	urgency := out[0][2]
	assert.Equal(t, "urgency", urgency.Name)
	assert.Equal(t, ent.DecisionScore, urgency.Kind)
	assert.InDelta(t, 0.9, urgency.Score, 1e-9)
	assert.InDelta(t, 0.6, urgency.Confidence, 1e-9)
	require.Len(t, urgency.Probabilities, 3)
	for i, want := range []ent.DecisionProbability{{Value: "can wait", Probability: 0.1}, {Value: "today", Probability: 0.9}, {Value: "now", Probability: 0}} {
		assert.Equal(t, want.Value, urgency.Probabilities[i].Value)
		assert.InDelta(t, want.Probability, urgency.Probabilities[i].Probability, 1e-9)
	}
	assert.Equal(t, "other", out[1][1].Choice)
	assert.InDelta(t, 1.5, out[1][2].Score, 1e-9)
	assert.InDelta(t, 0.5, out[1][2].Probabilities[1].Probability, 1e-9)
	assert.InDelta(t, 0.5, out[1][2].Probabilities[2].Probability, 1e-9)
}

func TestDecideBatches(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a": 0.1, "b": 0.2, "c": 0.3}}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), []ent.DecisionQuestion{angryQuestion, teamQuestion}, []string{"a", "b", "c"},
		classConfig(server.URL, map[string]any{"batchSize": 2}))

	require.NoError(t, err)
	requests := handler.received()
	require.Len(t, requests, 2)
	sort.Slice(requests, func(i, j int) bool { return len(requests[i].Questions) > len(requests[j].Questions) })
	assert.Equal(t, map[string]any{"document_0": "a", "document_1": "b"}, requests[0].State)
	assert.Len(t, requests[0].Questions, 4)
	assert.Equal(t, "In document_1: which team should handle this?", requests[0].Questions["document_1_q1"].Instructions)
	assert.Equal(t, "c", requests[1].State)
	for i, want := range []float64{0.1, 0.2, 0.3} {
		assert.InDelta(t, want, out[i][0].Probability, 1e-9)
		assert.Equal(t, "billing", out[i][1].Choice)
	}
}

func TestDecideCache(t *testing.T) {
	tests := []struct {
		name         string
		settings     map[string]any
		header       string
		wantRequests int
	}{
		{name: "the second call reads the cache", wantRequests: 1},
		{name: "class cache off", settings: map[string]any{"cache": false}, wantRequests: 2},
		{name: "header off wins over the class", header: "off", wantRequests: 2},
		{name: "header refresh judges again", header: "refresh", wantRequests: 2},
		{name: "header on wins over a class with the cache off", settings: map[string]any{"cache": false}, header: "on", wantRequests: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &decideHandler{t: t, scores: map[string]float64{"a document": 0.9}}
			c, server := decideTestClient(t, handler)
			ctx := context.Background()
			if tt.header != "" {
				ctx = context.WithValue(ctx, cacheHeader, []string{tt.header})
			}
			cfg := classConfig(server.URL, tt.settings)

			first, err := c.Decide(ctx, allQuestions, []string{"a document"}, cfg)
			require.NoError(t, err)
			second, err := c.Decide(ctx, allQuestions, []string{"a document"}, cfg)
			require.NoError(t, err)

			assert.Len(t, handler.received(), tt.wantRequests)
			assert.Equal(t, first, second)
		})
	}
}

func TestDecideCacheKeyCoversTheQuestions(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a document": 0.9}}
	c, server := decideTestClient(t, handler)
	cfg := classConfig(server.URL, nil)
	other := angryQuestion
	other.Instructions = "the customer is happy"

	_, err := c.Decide(context.Background(), []ent.DecisionQuestion{angryQuestion}, []string{"a document"}, cfg)
	require.NoError(t, err)
	_, err = c.Decide(context.Background(), []ent.DecisionQuestion{other}, []string{"a document"}, cfg)
	require.NoError(t, err)

	assert.Len(t, handler.received(), 2)
}

func TestDecideDocumentsSent(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a": 0.1, "b": 0.2}}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), []ent.DecisionQuestion{angryQuestion}, []string{"a", "", "b", "a"},
		classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.Len(t, handler.received(), 2, "identical documents share a request, empty ones are not sent")
	assert.InDelta(t, 0.1, out[0][0].Probability, 1e-9)
	assert.Equal(t, ent.DecisionAnswer{Name: "angry", Kind: ent.DecisionPredicate, Refused: true}, out[1][0])
	assert.InDelta(t, 0.2, out[2][0].Probability, 1e-9)
	assert.InDelta(t, 0.1, out[3][0].Probability, 1e-9)
}

func TestDecideSplitsABatchTheAPIRejects(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a": 0.1, "b": 0.2, "c": 0.3}, maxDocumentsPerRequest: 1}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), []ent.DecisionQuestion{angryQuestion}, []string{"a", "b", "c"},
		classConfig(server.URL, map[string]any{"batchSize": 3}))

	require.NoError(t, err)
	// One rejected batch of three, one rejected half of two, three singles.
	assert.Len(t, handler.received(), 5)
	for i, want := range []float64{0.1, 0.2, 0.3} {
		assert.InDelta(t, want, out[i][0].Probability, 1e-9)
	}
}

func TestDecideRejectedBeforeAnyRequest(t *testing.T) {
	tests := []struct {
		name      string
		apiKey    string
		questions []ent.DecisionQuestion
		documents []string
		settings  map[string]any
		wantErr   string
	}{
		{name: "no questions", apiKey: "apiKey", documents: []string{"a"}, wantErr: "no questions provided"},
		{
			name: "more documents than maxDocuments", apiKey: "apiKey", questions: allQuestions, documents: []string{"a", "b"},
			settings: map[string]any{"maxDocuments": 1}, wantErr: "2 documents exceed maxDocuments 1",
		},
		{
			name: "no api key", questions: allQuestions, documents: []string{"a"},
			wantErr: "TypeSafeAI API Key: no api key found neither in request header: X-Typesafeai-Api-Key nor in environment variable under TYPESAFEAI_APIKEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &decideHandler{t: t, scores: map[string]float64{"a": 0.1}}
			server := httptest.NewServer(handler)
			defer server.Close()

			_, err := newTestClient(tt.apiKey).Decide(context.Background(), tt.questions, tt.documents, classConfig(server.URL, tt.settings))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, handler.received())
		})
	}
}

func TestDecideMalformedAnswers(t *testing.T) {
	tests := []struct {
		name      string
		questions []ent.DecisionQuestion
		body      string
		wantErr   string
	}{
		{name: "missing answer", questions: allQuestions, body: `{"answers":{}}`, wantErr: `no answer in response for "q0"`},
		{name: "predicate of another type", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":{"q0":{"type":"score","score":1}}}`, wantErr: `answer "q0": unexpected answer type "score"`},
		{name: "predicate without probability", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":{"q0":{"type":"noul"}}}`, wantErr: `answer "q0": no probability in response`},
		{name: "probability out of range", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":{"q0":{"type":"noul","noul":1.5}}}`, wantErr: `answer "q0": probability 1.5 out of range`},
		{name: "choice without choice", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":{"q0":{"type":"choice","probabilities":{"billing":1,"other":0}}}}`, wantErr: `answer "q0": no choice in response`},
		{name: "choice that is not an option", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":{"q0":{"type":"choice","choice":"sales","probabilities":{"billing":1,"other":0}}}}`, wantErr: `answer "q0": choice "sales" is not an option`},
		{name: "choice without the probability of an option", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":{"q0":{"type":"choice","choice":"billing","probabilities":{"billing":1}}}}`, wantErr: `answer "q0": no probability for option "other"`},
		{name: "choice probability out of range", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":{"q0":{"type":"choice","choice":"billing","probabilities":{"billing":2,"other":0}}}}`, wantErr: `answer "q0": probability 2 of option "billing" out of range`},
		{name: "score out of range", questions: []ent.DecisionQuestion{urgencyQuestion}, body: `{"answers":{"q0":{"type":"score","score":2.5,"probabilities":{"0":0,"1":0,"2":1}}}}`, wantErr: `answer "q0": score 2.5 out of range`},
		{name: "score without the probability of a level", questions: []ent.DecisionQuestion{urgencyQuestion}, body: `{"answers":{"q0":{"type":"score","score":1,"probabilities":{"0":0,"1":1}}}}`, wantErr: `answer "q0": no probability for level "2"`},
		{name: "confidence out of range", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":{"q0":{"type":"choice","choice":"billing","probabilities":{"billing":1,"other":0},"confidence":3}}}`, wantErr: `answer "q0": confidence 3 out of range`},
		{name: "not JSON", questions: allQuestions, body: `<html>`, wantErr: "parse response: invalid character '<' looking for beginning of value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &decideHandler{t: t, rawBody: tt.body}
			c, server := decideTestClient(t, handler)

			_, err := c.Decide(context.Background(), tt.questions, []string{"a document"}, classConfig(server.URL, nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, handler.received(), 1, "a malformed answer is not retried")
		})
	}
}

func TestDecideRetriesAServerError(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a document": 0.9}, statuses: []int{503}}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), []ent.DecisionQuestion{angryQuestion}, []string{"a document"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.Len(t, handler.received(), 2)
	assert.InDelta(t, 0.9, out[0][0].Probability, 1e-9)
}
