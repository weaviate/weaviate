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

package modrerankertypesafeai

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
)

// With a rubric the rerank score is a position on it, and the threshold is
// the minimum score instead of the minimum probability.
func TestRerankWithARubric(t *testing.T) {
	// Positions on "can wait | this week | today | drop everything".
	scores := map[string]float64{"font size": 0.1, "export bug": 1.4, "invoice": 1, "outage": 2.6, "data lost": 3}
	input := []string{"invoice", "data lost", "font size", "outage", "export bug"}
	const levels = "can wait|this week|today|drop everything"

	tests := []struct {
		name     string
		settings map[string]any
		headers  map[string]string
		want     []string
		wantErr  string
		// wantNoRank is set when the request must fail before any API spend.
		wantNoRank bool
	}{
		{
			name:    "search order keeps every result",
			headers: map[string]string{"X-Typesafeai-Score-Levels": levels},
			want:    []string{"invoice", "data lost", "font size", "outage", "export bug"},
		},
		{
			name:    "probability order sorts by score",
			headers: map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Order": "probability"},
			want:    []string{"data lost", "outage", "export bug", "invoice", "font size"},
		},
		{
			name:    "minimum score from the header",
			headers: map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Min-Score": "1"},
			want:    []string{"invoice", "data lost", "outage", "export bug"},
		},
		{
			name:     "minimum score from the class setting",
			settings: map[string]any{"minScore": 2},
			headers:  map[string]string{"X-Typesafeai-Score-Levels": levels},
			want:     []string{"data lost", "outage"},
		},
		{
			name:     "header wins over the class setting",
			settings: map[string]any{"minScore": 3},
			headers:  map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Min-Score": "1.2"},
			want:     []string{"data lost", "outage", "export bug"},
		},
		{
			name:     "the probability threshold does not apply to scores",
			settings: map[string]any{"minProbability": 0.9},
			headers:  map[string]string{"X-Typesafeai-Score-Levels": levels},
			want:     []string{"invoice", "data lost", "font size", "outage", "export bug"},
		},
		{
			name:     "rubric from the class setting",
			settings: map[string]any{"scoreLevels": []any{"can wait", "this week", "today", "drop everything"}, "minScore": 2.5},
			want:     []string{"data lost", "outage"},
		},
		{
			name:       "minimum score above the last level",
			headers:    map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Min-Score": "3.5"},
			wantErr:    `X-Typesafeai-Min-Score must be a number between 0 and 3, got "3.5"`,
			wantNoRank: true,
		},
		{
			name:       "minimum score is not a number",
			headers:    map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Min-Score": "today"},
			wantErr:    `X-Typesafeai-Min-Score must be a number between 0 and 3, got "today"`,
			wantNoRank: true,
		},
		{
			name:       "class minimum score above the last level",
			settings:   map[string]any{"minScore": 5},
			headers:    map[string]string{"X-Typesafeai-Score-Levels": levels},
			wantErr:    "minScore must be between 0 and 3, got 5",
			wantNoRank: true,
		},
		{
			// A malformed header fails the request even when the other
			// question type is asked.
			name:       "malformed probability threshold with a rubric",
			headers:    map[string]string{"X-Typesafeai-Score-Levels": levels, "X-Typesafeai-Min-Probability": "high"},
			wantErr:    `X-Typesafeai-Min-Probability must be a number between 0 and 1, got "high"`,
			wantNoRank: true,
		},
		{
			name:       "malformed score threshold without a rubric",
			headers:    map[string]string{"X-Typesafeai-Min-Score": "today"},
			wantErr:    `X-Typesafeai-Min-Score must be a number between 0 and 9, got "today"`,
			wantNoRank: true,
		},
		{
			name:    "score threshold without a rubric is not applied",
			headers: map[string]string{"X-Typesafeai-Min-Score": "5"},
			want:    []string{"invoice", "data lost", "font size", "outage", "export bug"},
		},
		{
			name:       "invalid rubric",
			headers:    map[string]string{"X-Typesafeai-Score-Levels": "only"},
			wantErr:    "X-Typesafeai-Score-Levels must list between 2 and 10 levels, got 1",
			wantNoRank: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeRanker{scores: scores}
			fn := newTestModule(client).AdditionalProperties()["rerank"].SearchFunctions.ExploreGet
			ctx := context.Background()
			for header, value := range tt.headers {
				ctx = context.WithValue(ctx, header, []string{value})
			}
			property, query := "text", "how urgent is this ticket?"

			out, err := fn(ctx, results(input), &rank.Params{Property: &property, Query: &query},
				nil, nil, classConfig(tt.settings))

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Equal(t, tt.wantNoRank, client.calls == 0)
				return
			}
			require.NoError(t, err)
			got := make([]string, len(out))
			for i, res := range out {
				got[i] = res.Schema.(map[string]any)["text"].(string)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
