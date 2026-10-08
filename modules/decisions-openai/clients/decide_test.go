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
	"math"
	"net/http"
	"net/http/httptest"
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

// decideHandler fakes the Decisions API for requests with several
// questions. It answers a predicate with scores[input], a choice with
// choices[input] (the first option when unset), a score with
// positions[input] spread over the two nearest levels, and refuses the
// questions named in refused.
type decideHandler struct {
	t         *testing.T
	scores    map[string]float64
	choices   map[string]string
	positions map[string]float64
	refused   map[string]bool
	rawBody   string
	statuses  []int
	// answerNames, when set, replaces the names of the answers.
	answerNames []string

	lock     sync.Mutex
	requests []decisionRequest
}

func (h *decideHandler) received() []decisionRequest {
	h.lock.Lock()
	defer h.lock.Unlock()
	return append([]decisionRequest(nil), h.requests...)
}

func (h *decideHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	assert.NoError(h.t, err)
	var req decisionRequest
	assert.NoError(h.t, json.Unmarshal(bodyBytes, &req))
	assert.Equal(h.t, "/v1/decisions", r.URL.Path)

	h.lock.Lock()
	status := http.StatusOK
	if n := len(h.requests); n < len(h.statuses) {
		status = h.statuses[n]
	}
	h.requests = append(h.requests, req)
	h.lock.Unlock()

	if status != http.StatusOK {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":{"message":"The server had an error"}}`))
		return
	}
	if h.rawBody != "" {
		_, _ = w.Write([]byte(h.rawBody))
		return
	}

	answers := make([]map[string]any, len(req.Questions))
	for i, question := range req.Questions {
		name := question.Name
		if i < len(h.answerNames) {
			name = h.answerNames[i]
		}
		answers[i] = h.answer(question, name, req.Input)
	}
	response := map[string]any{
		"model":   req.Model,
		"answers": answers,
		"usage":   map[string]any{"input_tokens": 10, "output_tokens": 0},
	}
	assert.NoError(h.t, json.NewEncoder(w).Encode(response))
}

func (h *decideHandler) answer(question decisionQuestion, name, input string) map[string]any {
	if h.refused[question.Name] {
		return map[string]any{"type": "refusal", "name": name}
	}
	switch question.Type {
	case choiceType:
		choice := h.choices[input]
		if choice == "" {
			choice = question.Choices[0].Value
		}
		probabilities := make([]map[string]any, len(question.Choices))
		for i, option := range question.Choices {
			probability := 0.3 / float64(len(question.Choices)-1)
			if option.Value == choice {
				probability = 0.7
			}
			probabilities[i] = map[string]any{"value": option.Value, "probability": probability}
		}
		return map[string]any{"type": "choice", "name": name, "choice": choice, "probabilities": probabilities, "confidence": 0.6}
	case scoreType:
		position := h.positions[input]
		lower := math.Floor(position)
		probabilities := make([]map[string]any, len(question.Levels))
		for i, level := range question.Levels {
			probability := 0.0
			switch float64(i) {
			case lower:
				probability = 1 - (position - lower)
			case lower + 1:
				probability = position - lower
			}
			probabilities[i] = map[string]any{"value": i, "label": level.Label, "probability": probability}
		}
		return map[string]any{"type": "score", "name": name, "score": position, "probabilities": probabilities, "confidence": 0.6}
	default:
		return map[string]any{"type": "predicate", "name": name, "probability": h.scores[input]}
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
	assert.Equal(t, "gpt-6-luna", requests[0].Model)
	assert.Equal(t, "a document", requests[0].Input)
	description := "payments and refunds"
	levelDescription := "before the end of the day"
	assert.Equal(t, []decisionQuestion{
		{Type: "predicate", Name: "angry", Instructions: "the customer is angry"},
		{Type: "choice", Name: "team", Instructions: "which team should handle this?", Choices: []decisionChoice{
			{Value: "billing", Description: &description}, {Value: "other"},
		}},
		{Type: "score", Name: "urgency", Instructions: "how urgent is this?", Levels: []decisionLevel{
			{Label: "can wait"}, {Label: "today", Description: &levelDescription}, {Label: "now"},
		}},
	}, requests[0].Questions)
}

func TestDecideAnswers(t *testing.T) {
	handler := &decideHandler{
		t:         t,
		scores:    map[string]float64{"a document": 0.9, "another": 0.5},
		positions: map[string]float64{"a document": 0.9, "another": 1.5},
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
}

func TestDecideRefusal(t *testing.T) {
	handler := &decideHandler{t: t, scores: map[string]float64{"a document": 0.9}, refused: map[string]bool{"team": true}}
	c, server := decideTestClient(t, handler)

	out, err := c.Decide(context.Background(), allQuestions, []string{"a document"}, classConfig(server.URL, nil))

	require.NoError(t, err)
	assert.False(t, out[0][0].Refused)
	assert.Equal(t, ent.DecisionAnswer{Name: "team", Kind: ent.DecisionChoice, Refused: true}, out[0][1])
	assert.False(t, out[0][2].Refused)
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
			wantErr: "OpenAI API Key: no api key found neither in request header: X-Openai-Api-Key nor in environment variable under OPENAI_APIKEY",
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
	choiceOK := `{"type":"choice","name":"team","choice":"billing","probabilities":[{"value":"billing","probability":1},{"value":"other","probability":0}],"confidence":1}`
	tests := []struct {
		name      string
		questions []ent.DecisionQuestion
		body      string
		wantErr   string
	}{
		{name: "fewer answers than questions", questions: allQuestions, body: `{"answers":[]}`, wantErr: "expected 3 answers in response, got 0"},
		{name: "answer under another name", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":[{"type":"predicate","name":"other","probability":0.5}]}`, wantErr: `answer 0 in response is for "other", not for "angry"`},
		{name: "answer without a name", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":[{"type":"predicate","probability":0.5}]}`, wantErr: `answer 0 in response is for no name, not for "angry"`},
		{name: "predicate of another type", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":[{"type":"score","name":"angry","score":1}]}`, wantErr: `answer "angry": unexpected answer type "score"`},
		{name: "predicate without probability", questions: []ent.DecisionQuestion{angryQuestion}, body: `{"answers":[{"type":"predicate","name":"angry"}]}`, wantErr: `answer "angry": no probability in response`},
		{name: "choice that is not an option", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":[{"type":"choice","name":"team","choice":"sales","probabilities":[],"confidence":1}]}`, wantErr: `answer "team": choice "sales" in response is not one of the options`},
		{name: "choice that is a bool", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":[{"type":"choice","name":"team","choice":true,"probabilities":[],"confidence":1}]}`, wantErr: `answer "team": choice true in response is not one of the options`},
		{name: "choice without the probability of an option", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":[{"type":"choice","name":"team","choice":"billing","probabilities":[{"value":"billing","probability":1}],"confidence":1}]}`, wantErr: `answer "team": no probability for option "other"`},
		{name: "score out of range", questions: []ent.DecisionQuestion{urgencyQuestion}, body: `{"answers":[{"type":"score","name":"urgency","score":2.5,"probabilities":[],"confidence":1}]}`, wantErr: `answer "urgency": score 2.5 in response is out of range`},
		{name: "score without the probability of a level", questions: []ent.DecisionQuestion{urgencyQuestion}, body: `{"answers":[{"type":"score","name":"urgency","score":1,"probabilities":[{"value":0,"label":"can wait","probability":0},{"value":1,"label":"today","probability":1}],"confidence":1}]}`, wantErr: `answer "urgency": no probability for level "now"`},
		{name: "confidence out of range", questions: []ent.DecisionQuestion{teamQuestion}, body: `{"answers":[` + choiceOK[:len(choiceOK)-len(`"confidence":1}`)] + `"confidence":3}]}`, wantErr: `answer "team": confidence 3 in response is outside [0, 1]`},
		{name: "error object", questions: allQuestions, body: `{"error":{"message":"The model is overloaded"}}`, wantErr: "OpenAI API returned an error: The model is overloaded"},
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
