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
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func withLevels(levels string) context.Context {
	return context.WithValue(context.Background(), "X-Typesafeai-Score-Levels", []string{levels})
}

func TestRankWithARubric(t *testing.T) {
	const urgency = "can wait|this week|today|drop everything"
	levels := []string{"can wait", "this week", "today", "drop everything"}
	scores := map[string]float64{"data lost": 2.94, "dark theme": 0.34, "invoice typo": 1}
	documents := []string{"dark theme", "data lost", "invoice typo"}

	t.Run("sends a score question and returns the rubric position", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").Rank(withLevels(urgency), "how urgent is this ticket?", documents,
			classConfig(server.URL, nil))

		require.NoError(t, err)
		assert.Equal(t, []ent.DocumentScore{
			{Document: "dark theme", Score: 0.34},
			{Document: "data lost", Score: 2.94},
			{Document: "invoice typo", Score: 1},
		}, res.DocumentScores)
		requests := handler.received()
		require.Len(t, requests, 3)
		for _, request := range requests {
			question := request.body.Questions[questionKey]
			assert.Equal(t, "score", question.Type)
			assert.Equal(t, "how urgent is this ticket?", question.Instructions)
			assert.Equal(t, levels, question.Criteria)
		}
	})

	t.Run("levels from the class setting", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", documents[:1],
			classConfig(server.URL, map[string]any{"scoreLevels": []any{"low", "high"}}))

		require.NoError(t, err)
		question := handler.received()[0].body.Questions[questionKey]
		assert.Equal(t, "score", question.Type)
		assert.Equal(t, []string{"low", "high"}, question.Criteria)
	})

	t.Run("a batch carries the rubric in every question", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		res, err := newTestClient("apiKey").Rank(withLevels(urgency), "q", documents,
			classConfig(server.URL, map[string]any{"batchSize": 3}))

		require.NoError(t, err)
		assert.Equal(t, 2.94, res.DocumentScores[1].Score)
		requests := handler.received()
		require.Len(t, requests, 1)
		require.Len(t, requests[0].body.Questions, 3)
		for _, question := range requests[0].body.Questions {
			assert.Equal(t, "score", question.Type)
			assert.Equal(t, levels, question.Criteria)
		}
	})

	t.Run("a yes/no question carries no rubric", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 0.5}}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

		require.NoError(t, err)
		question := handler.received()[0].body.Questions[questionKey]
		assert.Equal(t, "noul", question.Type)
		assert.Empty(t, question.Criteria)
	})

	t.Run("an invalid rubric fails before any request", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, scores: scores}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(withLevels("only"), "q", documents, classConfig(server.URL, nil))

		require.ErrorContains(t, err, "X-Typesafeai-Score-Levels must list between 2 and 10 levels, got 1")
		assert.Empty(t, handler.received())
	})
}

// A yes/no answer, an answer for another rubric and an answer for the same
// rubric must not be served for each other.
func TestRankCacheIsSeparatedByQuestionType(t *testing.T) {
	handler := &typesafeaiHandler{t: t, scores: map[string]float64{"doc": 1}}
	server := httptest.NewServer(handler)
	defer server.Close()
	c := newTestClient("apiKey")
	cfg := classConfig(server.URL, nil)

	for _, ctx := range []context.Context{
		context.Background(),
		withLevels("low|high"),
		withLevels("low|medium|high"),
		withLevels("lowh|igh"),
		context.Background(),
		withLevels("low|high"),
	} {
		_, err := c.Rank(ctx, "q", []string{"doc"}, cfg)
		require.NoError(t, err)
	}

	// The last two repeat the first two and are served from the cache.
	assert.Len(t, handler.received(), 4)
}

func TestRankRubricMalformedResponses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "a yes/no answer to a score question",
			body:    `{"answers":{"q":{"type":"noul","noul":0.9}}}`,
			wantErr: `unexpected answer type "noul"`,
		},
		{
			name:    "score missing",
			body:    `{"answers":{"q":{"type":"score"}}}`,
			wantErr: "no score in response",
		},
		{
			name:    "score above the last level",
			body:    `{"answers":{"q":{"type":"score","score":3.5}}}`,
			wantErr: "score 3.5 out of range",
		},
		{
			name:    "negative score",
			body:    `{"answers":{"q":{"type":"score","score":-0.1}}}`,
			wantErr: "score -0.1 out of range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &typesafeaiHandler{t: t, rawBody: tt.body}
			server := httptest.NewServer(handler)
			defer server.Close()

			_, err := newTestClient("apiKey").Rank(withLevels("a|b|c"), "q", []string{"doc"}, classConfig(server.URL, nil))

			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("a score answer to a yes/no question", func(t *testing.T) {
		handler := &typesafeaiHandler{t: t, rawBody: `{"answers":{"q":{"type":"score","score":1}}}`}
		server := httptest.NewServer(handler)
		defer server.Close()

		_, err := newTestClient("apiKey").Rank(context.Background(), "q", []string{"doc"}, classConfig(server.URL, nil))

		require.ErrorContains(t, err, `unexpected answer type "score"`)
	})
}
