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
