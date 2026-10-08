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

// Package clients answers rerank and decide questions from the document
// text alone, so that a test can predict every answer:
//   - Rank scores a document with its length.
//   - A predicate's probability is the share of the words of its
//     instructions that appear in the document.
//   - A choice picks the first option whose value appears in the document,
//     or the first option, with all the probability on it.
//   - A score is the document length modulo the number of levels, with all
//     the probability on that level.
//   - A question whose instructions are "refuse" is refused.
package clients

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const RefuseInstructions = "refuse"

type Client struct{}

func New() *Client {
	return &Client{}
}

func (c *Client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	scores := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		scores[i] = ent.DocumentScore{Document: document, Score: float64(len(document))}
	}
	return &ent.RankResult{Query: query, DocumentScores: scores}, nil
}

func (c *Client) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
	cfg moduletools.ClassConfig,
) ([][]ent.DecisionAnswer, error) {
	out := make([][]ent.DecisionAnswer, len(documents))
	for d, document := range documents {
		out[d] = make([]ent.DecisionAnswer, len(questions))
		for q, question := range questions {
			out[d][q] = answer(question, document)
		}
	}
	return out, nil
}

func answer(question ent.DecisionQuestion, document string) ent.DecisionAnswer {
	out := ent.DecisionAnswer{Name: question.Name, Kind: question.Kind}
	if question.Instructions == RefuseInstructions {
		out.Refused = true
		return out
	}
	text := strings.ToLower(document)
	switch question.Kind {
	case ent.DecisionPredicate:
		words := strings.Fields(strings.ToLower(question.Instructions))
		found := 0
		for _, word := range words {
			if strings.Contains(text, word) {
				found++
			}
		}
		if len(words) > 0 {
			out.Probability = float64(found) / float64(len(words))
		}
	case ent.DecisionChoice:
		chosen := 0
		for i, option := range question.Options {
			if strings.Contains(text, strings.ToLower(option.Value)) {
				chosen = i
				break
			}
		}
		out.Choice = question.Options[chosen].Value
		out.Probabilities = make([]ent.DecisionProbability, len(question.Options))
		for i, option := range question.Options {
			out.Probabilities[i] = ent.DecisionProbability{Value: option.Value}
		}
		out.Probabilities[chosen].Probability = 1
		out.Confidence = 1
	case ent.DecisionScore:
		position := len(document) % len(question.Levels)
		out.Score = float64(position)
		out.Probabilities = make([]ent.DecisionProbability, len(question.Levels))
		for i, level := range question.Levels {
			out.Probabilities[i] = ent.DecisionProbability{Value: level.Label}
		}
		out.Probabilities[position].Probability = 1
		out.Confidence = 1
	}
	return out
}
