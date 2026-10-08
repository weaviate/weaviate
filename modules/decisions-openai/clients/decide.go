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
	"sync/atomic"

	"github.com/pkg/errors"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/decisions-openai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	choiceType = "choice"
	scoreType  = "score"
)

// decideRequest is what all requests of one Decide call have in common.
type decideRequest struct {
	url       string
	apiKey    string
	model     string
	questions []ent.DecisionQuestion
}

// Decide asks the Decisions API every question about every document, one
// request per document with all the questions, and returns the answers per
// document in the order of the questions. Identical documents share one
// request; a document without text gets refused answers and is not sent.
func (c *client) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
	cfg moduletools.ClassConfig,
) ([][]ent.DecisionAnswer, error) {
	settings, err := config.NewClassSettings(cfg).Resolve()
	if err != nil {
		return nil, err
	}
	if len(questions) == 0 {
		return nil, errors.New("no questions provided")
	}
	if maxDocuments := min(settings.MaxDocuments, config.MaxDocumentsLimit); len(documents) > maxDocuments {
		return nil, errors.Errorf("%d documents exceed maxDocuments %d", len(documents), maxDocuments)
	}
	openAIURL, apiKey, err := c.endpoint(ctx, settings.BaseURL)
	if err != nil {
		return nil, err
	}
	request := decideRequest{url: openAIURL, apiKey: apiKey, model: settings.Model, questions: questions}

	firstIndex := make(map[string]int, len(documents))
	answers := make([][]ent.DecisionAnswer, len(documents))
	var pending []int
	for i, document := range documents {
		if document == "" {
			continue
		}
		if _, ok := firstIndex[document]; ok {
			continue
		}
		firstIndex[document] = i
		pending = append(pending, i)
	}

	var inputTokens, outputTokens, requests atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.inFlight))
	for _, index := range pending {
		eg.Go(func() error {
			decided, usage, err := c.decideDocument(ctx, request, documents[index])
			if err != nil {
				return err
			}
			answers[index] = decided
			inputTokens.Add(usage.InputTokens)
			outputTokens.Add(usage.OutputTokens)
			requests.Add(1)
			return nil
		})
	}
	err = eg.Wait()
	usage := c.logger.WithField("action", "decisions_openai_decide").
		WithField("documents", len(documents)).
		WithField("questions", len(questions)).
		WithField("requests", requests.Load()).
		WithField("input_tokens", inputTokens.Load()).
		WithField("output_tokens", outputTokens.Load())
	if err != nil {
		// The answered documents were billed although the query failed.
		if requests.Load() > 0 {
			usage.Debug("openai decide failed")
		}
		return nil, err
	}
	usage.Debug("openai decide finished")

	out := make([][]ent.DecisionAnswer, len(documents))
	for i, document := range documents {
		if owner, ok := firstIndex[document]; ok {
			out[i] = answers[owner]
			continue
		}
		out[i] = refusedAnswers(questions)
	}
	return out, nil
}

func refusedAnswers(questions []ent.DecisionQuestion) []ent.DecisionAnswer {
	out := make([]ent.DecisionAnswer, len(questions))
	for i, question := range questions {
		out[i] = ent.DecisionAnswer{Name: question.Name, Kind: question.Kind, Refused: true}
	}
	return out
}

func (c *client) decideDocument(ctx context.Context, request decideRequest, document string,
) ([]ent.DecisionAnswer, decisionUsage, error) {
	body, err := json.Marshal(newDecideRequest(request, document))
	if err != nil {
		return nil, decisionUsage{}, errors.Wrap(err, "marshal body")
	}
	response, err := c.postWithRetry(ctx, request.url, request.apiKey, body)
	if err != nil {
		return nil, decisionUsage{}, err
	}
	answers, err := response.answers(request.questions)
	if err != nil {
		return nil, decisionUsage{}, err
	}
	return answers, response.Usage, nil
}

// newDecideRequest maps the questions onto the API's question types; the
// document is the input they share.
func newDecideRequest(request decideRequest, document string) decisionRequest {
	questions := make([]decisionQuestion, len(request.questions))
	for i, question := range request.questions {
		out := decisionQuestion{Type: predicateType, Name: question.Name, Instructions: question.Instructions}
		switch question.Kind {
		case ent.DecisionPredicate:
			// The default type.
		case ent.DecisionChoice:
			out.Type = choiceType
			out.Choices = make([]decisionChoice, len(question.Options))
			for j, option := range question.Options {
				out.Choices[j] = decisionChoice{Value: option.Value, Description: optional(option.Description)}
			}
		case ent.DecisionScore:
			out.Type = scoreType
			out.Levels = make([]decisionLevel, len(question.Levels))
			for j, level := range question.Levels {
				out.Levels[j] = decisionLevel{Label: level.Label, Description: optional(level.Description)}
			}
		}
		questions[i] = out
	}
	return decisionRequest{Model: request.model, Input: document, Questions: questions}
}

func optional(text string) *string {
	if text == "" {
		return nil
	}
	return &text
}

// answers returns one answer per question, in question order. The API
// answers in question order and echoes the names; both are checked.
func (r decisionResponse) answers(questions []ent.DecisionQuestion) ([]ent.DecisionAnswer, error) {
	if len(r.Answers) != len(questions) {
		return nil, errors.Errorf("expected %d answers in response, got %d", len(questions), len(r.Answers))
	}
	out := make([]ent.DecisionAnswer, len(questions))
	for i, question := range questions {
		answer := r.Answers[i]
		if answer.Name == nil || *answer.Name != question.Name {
			got := "no name"
			if answer.Name != nil {
				got = `"` + *answer.Name + `"`
			}
			return nil, errors.Errorf("answer %d in response is for %s, not for %q", i, got, question.Name)
		}
		decided, err := decideAnswer(question, answer)
		if err != nil {
			return nil, errors.Wrapf(err, "answer %q", question.Name)
		}
		out[i] = decided
	}
	return out, nil
}

// decideAnswer reads one answer and checks it against the question: the
// type, the ranges, a choice that names an option, a probability for every
// option or level.
func decideAnswer(question ent.DecisionQuestion, answer decisionAnswer) (ent.DecisionAnswer, error) {
	out := ent.DecisionAnswer{Name: question.Name, Kind: question.Kind}
	if answer.Type == refusalType {
		out.Refused = true
		return out, nil
	}
	switch question.Kind {
	case ent.DecisionPredicate:
		if answer.Type != predicateType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Probability == nil {
			return out, errors.New("no probability in response")
		}
		if p := *answer.Probability; p < 0 || p > 1 {
			return out, errors.Errorf("probability %v in response is outside [0, 1]", p)
		}
		out.Probability = *answer.Probability
	case ent.DecisionChoice:
		if answer.Type != choiceType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		choice, ok := answer.Choice.(string)
		if !ok {
			return out, errors.Errorf("choice %v in response is not one of the options", answer.Choice)
		}
		known := false
		for _, option := range question.Options {
			known = known || option.Value == choice
		}
		if !known {
			return out, errors.Errorf("choice %q in response is not one of the options", choice)
		}
		out.Choice = choice
		byValue := make(map[string]float64, len(answer.Probabilities))
		for _, p := range answer.Probabilities {
			if value, ok := p.Value.(string); ok {
				byValue[value] = p.Probability
			}
		}
		out.Probabilities = make([]ent.DecisionProbability, len(question.Options))
		for i, option := range question.Options {
			probability, err := namedProbability(byValue, option.Value, "option")
			if err != nil {
				return out, err
			}
			out.Probabilities[i] = ent.DecisionProbability{Value: option.Value, Probability: probability}
		}
	case ent.DecisionScore:
		if answer.Type != scoreType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Score == nil {
			return out, errors.New("no score in response")
		}
		if s := *answer.Score; s < 0 || s > float64(len(question.Levels)-1) {
			return out, errors.Errorf("score %v in response is out of range", s)
		}
		out.Score = *answer.Score
		// The API keys the probabilities of a score by level index.
		byIndex := make(map[int]float64, len(answer.Probabilities))
		for _, p := range answer.Probabilities {
			if index, ok := p.Value.(float64); ok {
				byIndex[int(index)] = p.Probability
			}
		}
		out.Probabilities = make([]ent.DecisionProbability, len(question.Levels))
		for i, level := range question.Levels {
			probability, ok := byIndex[i]
			if !ok {
				return out, errors.Errorf("no probability for level %q", level.Label)
			}
			if probability < 0 || probability > 1 {
				return out, errors.Errorf("probability %v of level %q is outside [0, 1]", probability, level.Label)
			}
			out.Probabilities[i] = ent.DecisionProbability{Value: level.Label, Probability: probability}
		}
	default:
		return out, errors.Errorf("unknown question kind %q", question.Kind)
	}
	if answer.Confidence != nil {
		if c := *answer.Confidence; c < 0 || c > 1 {
			return out, errors.Errorf("confidence %v in response is outside [0, 1]", c)
		}
		out.Confidence = *answer.Confidence
	}
	return out, nil
}

func namedProbability(probabilities map[string]float64, name, what string) (float64, error) {
	probability, ok := probabilities[name]
	if !ok {
		return 0, errors.Errorf("no probability for %s %q", what, name)
	}
	if probability < 0 || probability > 1 {
		return 0, errors.Errorf("probability %v of %s %q is outside [0, 1]", probability, what, name)
	}
	return probability, nil
}
