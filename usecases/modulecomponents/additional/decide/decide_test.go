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

package decide

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// fakeClient answers every question with a probability that names the
// document and the question, and records what it was asked.
type fakeClient struct {
	calls []call
	err   error
	// short answers fewer questions than asked.
	short bool
	// renamed answers under another name.
	renamed bool
}

type call struct {
	questions []string
	documents []string
}

func (f *fakeClient) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
	cfg moduletools.ClassConfig,
) ([][]ent.DecisionAnswer, error) {
	names := make([]string, len(questions))
	for i, q := range questions {
		names[i] = q.Name
	}
	f.calls = append(f.calls, call{questions: names, documents: documents})
	if f.err != nil {
		return nil, f.err
	}
	out := make([][]ent.DecisionAnswer, len(documents))
	for d, document := range documents {
		for _, q := range questions {
			if f.short {
				break
			}
			name := q.Name
			if f.renamed {
				name = "other"
			}
			answer := ent.DecisionAnswer{Name: name, Kind: q.Kind}
			switch q.Kind {
			case ent.DecisionPredicate:
				answer.Probability = float64(len(document)) / 100
			case ent.DecisionChoice:
				answer.Choice = q.Options[0].Value
				answer.Confidence = 1
			case ent.DecisionScore:
				answer.Score = 1
			}
			out[d] = append(out[d], answer)
		}
	}
	return out, nil
}

func predicate(name, property string) ent.DecisionQuestion {
	return ent.DecisionQuestion{Name: name, Property: property, Instructions: "is it " + name, Kind: ent.DecisionPredicate}
}

func choice(name, property string) ent.DecisionQuestion {
	return ent.DecisionQuestion{
		Name: name, Property: property, Instructions: "which", Kind: ent.DecisionChoice,
		Options: []ent.DecisionOption{{Value: "a"}, {Value: "b"}},
	}
}

func result(class string, properties map[string]any) search.Result {
	return search.Result{ClassName: class, Schema: properties}
}

func TestDecideAttachesOneAnswerPerQuestion(t *testing.T) {
	client := &fakeClient{}
	in := []search.Result{
		result("C", map[string]any{"text": "a long document", "title": "t1"}),
		result("C", map[string]any{"text": "short", "title": "t2"}),
	}
	params := &Params{Questions: []ent.DecisionQuestion{
		predicate("angry", "text"), choice("team", "title"), predicate("urgent", "text"),
	}}

	out, err := New(client).decide(context.Background(), nil, in, params)

	require.NoError(t, err)
	require.Len(t, out, 2)
	// One call per property, with every question about that property.
	require.Equal(t, []call{
		{questions: []string{"angry", "urgent"}, documents: []string{"a long document", "short"}},
		{questions: []string{"team"}, documents: []string{"t1", "t2"}},
	}, client.calls)
	answers := out[0].AdditionalProperties[Property].([]ent.DecisionAnswer)
	require.Len(t, answers, 3)
	// The answers come back in question order, not in call order.
	assert.Equal(t, "angry", answers[0].Name)
	assert.Equal(t, "team", answers[1].Name)
	assert.Equal(t, "urgent", answers[2].Name)
	assert.InDelta(t, 0.15, answers[0].Probability, 1e-9)
	assert.Equal(t, "a", answers[1].Choice)
	assert.InDelta(t, 0.15, answers[2].Probability, 1e-9)
	short := out[1].AdditionalProperties[Property].([]ent.DecisionAnswer)
	assert.InDelta(t, 0.05, short[0].Probability, 1e-9)
}

func TestDecideRefusesWhatHasNoText(t *testing.T) {
	client := &fakeClient{}
	in := []search.Result{
		result("C", map[string]any{"text": "a document"}),
		result("C", map[string]any{"text": ""}),
		result("C", map[string]any{"text": 7}),
		result("C", map[string]any{}),
	}
	params := &Params{Questions: []ent.DecisionQuestion{predicate("angry", "text")}}

	out, err := New(client).decide(context.Background(), nil, in, params)

	require.NoError(t, err)
	require.Equal(t, []call{{questions: []string{"angry"}, documents: []string{"a document"}}}, client.calls)
	assert.False(t, out[0].AdditionalProperties[Property].([]ent.DecisionAnswer)[0].Refused)
	for _, i := range []int{1, 2, 3} {
		answer := out[i].AdditionalProperties[Property].([]ent.DecisionAnswer)[0]
		assert.True(t, answer.Refused, "result %d", i)
		assert.Equal(t, "angry", answer.Name)
		assert.Equal(t, ent.DecisionPredicate, answer.Kind)
	}
}

func TestDecideSendsNothingWhenNoResultHasText(t *testing.T) {
	client := &fakeClient{}
	in := []search.Result{result("C", map[string]any{"other": "x"})}
	params := &Params{Questions: []ent.DecisionQuestion{predicate("angry", "text")}}

	out, err := New(client).decide(context.Background(), nil, in, params)

	require.NoError(t, err)
	assert.Empty(t, client.calls)
	assert.True(t, out[0].AdditionalProperties[Property].([]ent.DecisionAnswer)[0].Refused)
}

func TestDecideClientErrors(t *testing.T) {
	tests := []struct {
		name    string
		client  *fakeClient
		wantErr string
	}{
		{name: "client fails", client: &fakeClient{err: errors.New("boom")}, wantErr: "client decide: boom"},
		{name: "fewer answers than questions", client: &fakeClient{short: true}, wantErr: "client decide: 1 questions asked, 0 answered"},
		{name: "answer under another name", client: &fakeClient{renamed: true}, wantErr: `client decide: answer for "other" where "angry" was asked`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := []search.Result{result("C", map[string]any{"text": "a document"})}
			params := &Params{Questions: []ent.DecisionQuestion{predicate("angry", "text")}}

			_, err := New(tt.client).decide(context.Background(), nil, in, params)

			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestDecideNoResults(t *testing.T) {
	client := &fakeClient{}

	out, err := New(client).decide(context.Background(), nil, nil, &Params{})

	require.NoError(t, err)
	assert.Nil(t, out)
	assert.Empty(t, client.calls)
}

func TestAdditionalPropertyFnRejectsWrongParams(t *testing.T) {
	_, err := New(&fakeClient{}).AdditionalPropertyFn(context.Background(), []search.Result{{}}, "params", nil, nil, nil)

	require.EqualError(t, err, "wrong parameters")
}

func TestParamsValidate(t *testing.T) {
	many := make([]ent.DecisionQuestion, MaxQuestions+1)
	for i := range many {
		many[i] = predicate("q"+strings.Repeat("x", i), "text")
	}
	options := func(n int) []ent.DecisionOption {
		out := make([]ent.DecisionOption, n)
		for i := range out {
			out[i] = ent.DecisionOption{Value: strings.Repeat("o", i+1)}
		}
		return out
	}
	levels := func(n int) []ent.DecisionLevel {
		out := make([]ent.DecisionLevel, n)
		for i := range out {
			out[i] = ent.DecisionLevel{Label: strings.Repeat("l", i+1)}
		}
		return out
	}
	tests := []struct {
		name      string
		questions []ent.DecisionQuestion
		wantErr   string
	}{
		{name: "one of each kind", questions: []ent.DecisionQuestion{
			predicate("a", "text"), choice("b", "text"),
			{Name: "c", Property: "text", Instructions: "how", Kind: ent.DecisionScore, Levels: levels(2)},
		}},
		{name: "no questions", wantErr: "no questions"},
		{name: "too many questions", questions: many, wantErr: "26 questions exceed the limit of 25"},
		{name: "no name", questions: []ent.DecisionQuestion{predicate("", "text")}, wantErr: "question 0 has no name"},
		{name: "duplicate name", questions: []ent.DecisionQuestion{predicate("a", "text"), predicate("a", "title")}, wantErr: `question name "a" is used twice`},
		{name: "no property", questions: []ent.DecisionQuestion{predicate("a", "")}, wantErr: `question "a": no property`},
		{name: "no instructions", questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Kind: ent.DecisionPredicate}}, wantErr: `question "a": no instructions`},
		{name: "unknown kind", questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Instructions: "x", Kind: "guess"}}, wantErr: `question "a": unknown kind "guess"`},
		{
			name:      "choice with one option",
			questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionChoice, Options: options(1)}},
			wantErr:   `question "a": needs between 2 and 100 options, got 1`,
		},
		{
			name:      "choice with too many options",
			questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionChoice, Options: options(MaxChoices + 1)}},
			wantErr:   `question "a": needs between 2 and 100 options, got 101`,
		},
		{
			name: "choice with an unnamed option",
			questions: []ent.DecisionQuestion{{
				Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionChoice,
				Options: []ent.DecisionOption{{Value: "x"}, {Value: ""}},
			}},
			wantErr: `question "a": option 1 has no name`,
		},
		{
			name: "choice with a repeated option",
			questions: []ent.DecisionQuestion{{
				Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionChoice,
				Options: []ent.DecisionOption{{Value: "x"}, {Value: "x"}},
			}},
			wantErr: `question "a": option "x" is listed twice`,
		},
		{
			name:      "score with one level",
			questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionScore, Levels: levels(1)}},
			wantErr:   `question "a": needs between 2 and 20 levels, got 1`,
		},
		{
			name:      "score with too many levels",
			questions: []ent.DecisionQuestion{{Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionScore, Levels: levels(MaxLevels + 1)}},
			wantErr:   `question "a": needs between 2 and 20 levels, got 21`,
		},
		{
			name: "score with a repeated level",
			questions: []ent.DecisionQuestion{{
				Name: "a", Property: "text", Instructions: "x", Kind: ent.DecisionScore,
				Levels: []ent.DecisionLevel{{Label: "x"}, {Label: "x"}},
			}},
			wantErr: `question "a": level "x" is listed twice`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Params{Questions: tt.questions}).Validate()

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestParamsPropertiesToExtract(t *testing.T) {
	params := &Params{Questions: []ent.DecisionQuestion{
		predicate("a", "text"), predicate("b", "title"), predicate("c", "text"), predicate("d", ""),
	}}

	assert.Equal(t, []string{"text", "title"}, params.GetPropertiesToExtract())
}
