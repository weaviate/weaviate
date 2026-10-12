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

package rank

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	rerankmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// scoreFromText scores a document "<score> <name>" with its leading number.
type scoreFromText struct{}

func (scoreFromText) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	out := &ent.RankResult{Query: query, DocumentScores: make([]ent.DocumentScore, len(documents))}
	for i, document := range documents {
		score, err := strconv.ParseFloat(strings.Fields(document)[0], 64)
		if err != nil {
			return nil, err
		}
		out.DocumentScores[i] = ent.DocumentScore{Document: document, Score: score}
	}
	return out, nil
}

func orderResults(texts ...string) []search.Result {
	out := make([]search.Result, len(texts))
	for i, text := range texts {
		out[i] = search.Result{Schema: map[string]any{"text": text}}
	}
	return out
}

func textsOf(results []search.Result) []string {
	out := make([]string, len(results))
	for i, result := range results {
		out[i] = result.Schema.(map[string]any)["text"].(string)
	}
	return out
}

func orderParams() *Params {
	property, query := "text", "q"
	return &Params{Property: &property, Query: &query}
}

// Results with the same score must keep the order the search gave them. The
// input is long and mixes three scores, so an unstable sort reorders it.
func TestRerankKeepsSearchOrderAmongEqualScores(t *testing.T) {
	scores := []string{"0.5", "0.9", "0.1"}
	byScore := map[string][]string{}
	var texts []string
	for i := range 300 {
		score := scores[(i*7+i/3)%len(scores)]
		text := fmt.Sprintf("%s item-%03d", score, i)
		texts = append(texts, text)
		byScore[score] = append(byScore[score], text)
	}
	want := append(append(append([]string{}, byScore["0.9"]...), byScore["0.5"]...), byScore["0.1"]...)

	out, err := New(scoreFromText{}).AdditionalPropertyFn(context.Background(),
		orderResults(texts...), orderParams(), nil, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, want, textsOf(out))
}

func TestScoreKeepsTheInputOrder(t *testing.T) {
	texts := []string{"0.2 c", "0.9 a", "0.1 d", "0.5 b"}

	out, err := New(scoreFromText{}).Score(context.Background(), nil, orderResults(texts...), orderParams())

	require.NoError(t, err)
	assert.Equal(t, texts, textsOf(out))
	for i, want := range []float64{0.2, 0.9, 0.1, 0.5} {
		scores := out[i].AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
		require.Len(t, scores, 1)
		assert.Equal(t, want, *scores[0].Score)
	}
}

func decisionProvider(settings DecisionSettings, err error) *ReRankerProvider {
	return NewForDecisions(scoreFromText{}, func(moduletools.ClassConfig) (DecisionSettings, error) {
		return settings, err
	})
}

func decisionParams(minProbability *float64, order *Order) *Params {
	params := orderParams()
	if minProbability != nil || order != nil {
		params.Decision = &Decision{MinProbability: minProbability, Order: order}
	}
	return params
}

func ptr[T any](v T) *T { return &v }

// A decisions module keeps the search order and drops nothing unless the
// class settings or the request say otherwise; the request wins.
func TestRerankByDecision(t *testing.T) {
	texts := []string{"0.2 c", "0.9 a", "0.1 d", "0.5 b"}
	tests := []struct {
		name     string
		settings DecisionSettings
		params   *Params
		want     []string
		wantErr  string
	}{
		{name: "defaults keep the search order and every result", settings: DecisionSettings{Order: OrderSearch}, params: decisionParams(nil, nil), want: texts},
		{name: "class order probability", settings: DecisionSettings{Order: OrderProbability}, params: decisionParams(nil, nil), want: []string{"0.9 a", "0.5 b", "0.2 c", "0.1 d"}},
		{name: "class threshold", settings: DecisionSettings{MinProbability: 0.5, Order: OrderSearch}, params: decisionParams(nil, nil), want: []string{"0.9 a", "0.5 b"}},
		{name: "request threshold overrides the class", settings: DecisionSettings{MinProbability: 0.5, Order: OrderSearch}, params: decisionParams(ptr(0.15), nil), want: []string{"0.2 c", "0.9 a", "0.5 b"}},
		{name: "request order overrides the class", settings: DecisionSettings{Order: OrderSearch}, params: decisionParams(nil, ptr(OrderProbability)), want: []string{"0.9 a", "0.5 b", "0.2 c", "0.1 d"}},
		{name: "request threshold and order", settings: DecisionSettings{}, params: decisionParams(ptr(0.3), ptr(OrderProbability)), want: []string{"0.9 a", "0.5 b"}},
		{name: "threshold above every score", settings: DecisionSettings{}, params: decisionParams(ptr(0.95), nil), want: []string{}},
		{name: "threshold out of range", settings: DecisionSettings{}, params: decisionParams(ptr(1.5), nil), wantErr: "rerank.decision.min_probability must be between 0 and 1, got 1.5"},
		{name: "unknown order", settings: DecisionSettings{}, params: decisionParams(nil, ptr(Order("newest"))), wantErr: `rerank.decision.order must be "search" or "probability", got "newest"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := decisionProvider(tt.settings, nil).AdditionalPropertyFn(context.Background(),
				orderResults(texts...), tt.params, nil, nil, nil)

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, textsOf(out))
		})
	}

	t.Run("invalid class settings fail before any document is scored", func(t *testing.T) {
		counting := &countingRanker{}
		provider := NewForDecisions(counting, func(moduletools.ClassConfig) (DecisionSettings, error) {
			return DecisionSettings{}, fmt.Errorf("order must be \"search\" or \"probability\", got \"newest\"")
		})

		_, err := provider.AdditionalPropertyFn(context.Background(), orderResults(texts...), decisionParams(nil, nil), nil, nil, nil)

		require.EqualError(t, err, `order must be "search" or "probability", got "newest"`)
		assert.Equal(t, 0, counting.calls)
	})

	t.Run("a reranker rejects a decision", func(t *testing.T) {
		_, err := New(scoreFromText{}).AdditionalPropertyFn(context.Background(), orderResults(texts...), decisionParams(ptr(0.5), nil), nil, nil, nil)

		require.EqualError(t, err, "rerank.decision needs a decisions module")
	})
}

type countingRanker struct{ calls int }

func (c *countingRanker) Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error) {
	c.calls++
	return scoreFromText{}.Rank(ctx, query, documents, cfg)
}
