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

package moddecisionstypesafeai

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
	rerankmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestRerankDropsResultsBelowMinProbability(t *testing.T) {
	scores := map[string]float64{
		"angry":   0.97,
		"annoyed": 0.5,
		"neutral": 0.2,
		"happy":   0.01,
		"furious": 1,
	}
	input := []string{"neutral", "angry", "happy", "furious", "annoyed"}

	tests := []struct {
		name     string
		settings map[string]any
		header   string
		// order is sent as the X-Typesafeai-Order header.
		order   string
		want    []string
		wantErr string
		// wantNoRank is set when the request must fail before any API spend.
		wantNoRank bool
	}{
		{
			name:     "no threshold keeps every result in search order",
			settings: map[string]any{},
			want:     []string{"neutral", "angry", "happy", "furious", "annoyed"},
		},
		{
			name:     "threshold drops lower probabilities and keeps the search order",
			settings: map[string]any{"minProbability": 0.4},
			want:     []string{"angry", "furious", "annoyed"},
		},
		{
			name:     "order probability sorts by probability",
			settings: map[string]any{"order": "probability"},
			want:     []string{"furious", "angry", "annoyed", "neutral", "happy"},
		},
		{
			name:     "order probability with a threshold",
			settings: map[string]any{"order": "probability", "minProbability": 0.4},
			want:     []string{"furious", "angry", "annoyed"},
		},
		{
			name:     "order header wins over the class setting",
			settings: map[string]any{"order": "probability", "minProbability": 0.4},
			order:    "search",
			want:     []string{"angry", "furious", "annoyed"},
		},
		{
			name:     "order header selects probability",
			settings: map[string]any{"minProbability": 0.4},
			order:    "probability",
			want:     []string{"furious", "angry", "annoyed"},
		},
		{
			name:     "a probability equal to the threshold is kept",
			settings: map[string]any{"minProbability": 0.5},
			want:     []string{"angry", "furious", "annoyed"},
		},
		{
			name:     "threshold of 1 keeps only certain results",
			settings: map[string]any{"minProbability": 1},
			want:     []string{"furious"},
		},
		{
			name:     "threshold header wins over the class setting",
			settings: map[string]any{"minProbability": 0.99},
			header:   "0.1",
			want:     []string{"neutral", "angry", "furious", "annoyed"},
		},
		{
			name:     "threshold header of 0 disables the class threshold",
			settings: map[string]any{"minProbability": 0.99},
			header:   "0",
			want:     []string{"neutral", "angry", "happy", "furious", "annoyed"},
		},
		{
			name:       "threshold header is not a number",
			settings:   map[string]any{},
			header:     "high",
			wantErr:    `X-Typesafeai-Min-Probability must be a number between 0 and 1, got "high"`,
			wantNoRank: true,
		},
		{
			name:       "threshold header above 1",
			settings:   map[string]any{},
			header:     "1.5",
			wantErr:    `X-Typesafeai-Min-Probability must be a number between 0 and 1, got "1.5"`,
			wantNoRank: true,
		},
		{
			name:       "threshold header below 0",
			settings:   map[string]any{},
			header:     "-0.1",
			wantErr:    `X-Typesafeai-Min-Probability must be a number between 0 and 1, got "-0.1"`,
			wantNoRank: true,
		},
		{
			name:       "threshold header NaN",
			settings:   map[string]any{},
			header:     "NaN",
			wantErr:    `X-Typesafeai-Min-Probability must be a number between 0 and 1, got "NaN"`,
			wantNoRank: true,
		},
		{
			name:       "class threshold out of range",
			settings:   map[string]any{"minProbability": 3},
			wantErr:    "minProbability must be between 0 and 1, got 3",
			wantNoRank: true,
		},
		{
			name:       "unknown order header",
			settings:   map[string]any{},
			order:      "newest",
			wantErr:    `X-Typesafeai-Order must be "search" or "probability", got "newest"`,
			wantNoRank: true,
		},
		{
			name:       "unknown class order",
			settings:   map[string]any{"order": "random"},
			wantErr:    `order must be "search" or "probability", got "random"`,
			wantNoRank: true,
		},
	}

	for _, capability := range []string{"ExploreGet", "ExploreList"} {
		for _, tt := range tests {
			t.Run(capability+"/"+tt.name, func(t *testing.T) {
				client := &fakeRanker{scores: scores}
				rerank := newTestModule(client).AdditionalProperties()["rerank"]
				fn := rerank.SearchFunctions.ExploreGet
				if capability == "ExploreList" {
					fn = rerank.SearchFunctions.ExploreList
				}
				require.NotNil(t, fn)

				ctx := context.Background()
				if tt.header != "" {
					ctx = context.WithValue(ctx, "X-Typesafeai-Min-Probability", []string{tt.header})
				}
				if tt.order != "" {
					ctx = context.WithValue(ctx, "X-Typesafeai-Order", []string{tt.order})
				}
				property, query := "text", "the customer is angry"

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
					rerankResult := res.AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
					require.Len(t, rerankResult, 1)
					require.NotNil(t, rerankResult[0].Score)
					assert.Equal(t, scores[got[i]], *rerankResult[0].Score)
				}
				assert.Equal(t, tt.want, got)
				assert.Equal(t, 1, client.calls)
			})
		}
	}
}

func TestRerankDropsEveryResult(t *testing.T) {
	client := &fakeRanker{scores: map[string]float64{"a": 0.1, "b": 0.2}}
	fn := newTestModule(client).AdditionalProperties()["rerank"].SearchFunctions.ExploreGet
	property, query := "text", "q"

	out, err := fn(context.Background(), results([]string{"a", "b"}),
		&rank.Params{Property: &property, Query: &query}, nil, nil,
		classConfig(map[string]any{"minProbability": 0.9}))

	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestRerankKeepsGraphQLFunctions(t *testing.T) {
	rerank := newTestModule(&fakeRanker{}).AdditionalProperties()["rerank"]

	assert.Equal(t, []string{"rerank"}, rerank.GraphQLNames)
	assert.NotNil(t, rerank.GraphQLFieldFunction)
	assert.NotNil(t, rerank.GraphQLExtractFunction)
	assert.Nil(t, rerank.SearchFunctions.ObjectGet)
	assert.Nil(t, rerank.SearchFunctions.ObjectList)
}

func newTestModule(client DecisionsTypeSafeAIClient) *DecisionsTypeSafeAIModule {
	m := New()
	m.setClient(client)
	return m
}

type fakeRanker struct {
	scores map[string]float64
	calls  int
}

func (f *fakeRanker) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	f.calls++
	out := &ent.RankResult{Query: query, DocumentScores: make([]ent.DocumentScore, len(documents))}
	for i, document := range documents {
		out.DocumentScores[i] = ent.DocumentScore{Document: document, Score: f.scores[document]}
	}
	return out, nil
}

func (f *fakeRanker) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
	cfg moduletools.ClassConfig,
) ([][]ent.DecisionAnswer, error) {
	return nil, nil
}

func (f *fakeRanker) MetaInfo() (map[string]any, error) {
	return nil, nil
}

func results(texts []string) []search.Result {
	out := make([]search.Result, len(texts))
	for i, text := range texts {
		out[i] = search.Result{ClassName: "Ticket", Schema: map[string]any{"text": text}}
	}
	return out
}

func classConfig(settings map[string]any) moduletools.ClassConfig {
	class := &models.Class{
		Class:        "Ticket",
		ModuleConfig: map[string]any{Name: settings},
	}
	// The module name is empty here because that is how the modules provider
	// builds the config it passes to additional properties.
	return modules.NewClassBasedModuleConfig(class, "", "", "", &config.Config{})
}

var _ = modulecapabilities.AdditionalProperties(New())

// With the same probability, the search order decides in both orders.
func TestRerankKeepsSearchOrderAmongEqualProbabilities(t *testing.T) {
	scores := map[string]float64{}
	var input []string
	for i := range 120 {
		text := "ticket " + strconv.Itoa(i)
		input = append(input, text)
		scores[text] = []float64{0.7, 0.9}[i%2]
	}
	var want []string
	for _, score := range []float64{0.9, 0.7} {
		for _, text := range input {
			if scores[text] == score {
				want = append(want, text)
			}
		}
	}
	fn := newTestModule(&fakeRanker{scores: scores}).AdditionalProperties()["rerank"].SearchFunctions.ExploreGet
	property, query := "text", "q"

	out, err := fn(context.Background(), results(input), &rank.Params{Property: &property, Query: &query},
		nil, nil, classConfig(map[string]any{"order": "probability"}))

	require.NoError(t, err)
	got := make([]string, len(out))
	for i, res := range out {
		got[i] = res.Schema.(map[string]any)["text"].(string)
	}
	assert.Equal(t, want, got)
}

func TestRerankRejectsOtherParams(t *testing.T) {
	fn := newTestModule(&fakeRanker{}).AdditionalProperties()["rerank"].SearchFunctions.ExploreGet

	_, err := fn(context.Background(), results([]string{"a"}), "not rank params", nil, nil, classConfig(nil))

	require.ErrorContains(t, err, "wrong parameters")
}

// The module serves rerank and decide, both on the Get and the List search.
func TestModuleServesRerankAndDecide(t *testing.T) {
	properties := newTestModule(&fakeRanker{}).AdditionalProperties()

	require.Len(t, properties, 2)
	for _, name := range []string{"rerank", "decide"} {
		property, ok := properties[name]
		require.True(t, ok, name)
		assert.NotNil(t, property.SearchFunctions.ExploreGet, name)
		assert.NotNil(t, property.SearchFunctions.ExploreList, name)
	}
	// decide has no GraphQL field: it is served through gRPC only.
	assert.Nil(t, properties["decide"].GraphQLFieldFunction)
	assert.NotNil(t, properties["rerank"].GraphQLFieldFunction)
}
