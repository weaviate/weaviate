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
	"strconv"
	"sync/atomic"

	"github.com/pkg/errors"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/decisions-typesafeai/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const choiceType = "choice"

// decisionRequest is what all batches of one Decide call have in common.
type decisionRequest struct {
	url       string
	apiKey    string
	model     string
	questions []ent.DecisionQuestion
	image     bool
}

func (r decisionRequest) batches(indexes []int, documents []string) [][]int {
	if r.image {
		return imageBatches(indexes, documents)
	}
	return batches(indexes, documents)
}

// Decide asks Jev every question about every document and returns the
// answers per document, in the order of the questions. One request carries
// all the questions about a document, and the documents are batched as in
// Rank, see MaxBatchDocuments. Identical documents are judged once. A
// document without text gets refused answers.
func (c *client) Decide(ctx context.Context, questions []ent.DecisionQuestion, documents []string,
	cfg moduletools.ClassConfig,
) ([][]ent.DecisionAnswer, error) {
	settings := config.NewClassSettings(cfg)
	if len(questions) == 0 {
		return nil, errors.New("no questions provided")
	}
	if maxDocuments := min(settings.MaxDocuments(), config.MaxDocumentsLimit); len(documents) > maxDocuments {
		return nil, errors.Errorf("%d documents exceed maxDocuments %d", len(documents), maxDocuments)
	}
	typesafeaiURL, apiKey, err := c.endpoint(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}
	// The questions of one call are about one property, see the provider;
	// a call about several would send the documents of one as the other.
	for _, question := range questions[1:] {
		if question.Property != questions[0].Property {
			return nil, errors.Errorf("questions about %q and %q in one call: a call judges one property", questions[0].Property, question.Property)
		}
	}
	image := isImageProperty(cfg, questions[0].Property)
	if image && !settings.Images() {
		return nil, errors.Errorf("property %q is a blob: images need the images setting of %s", questions[0].Property, config.ModuleName)
	}
	request := decisionRequest{url: typesafeaiURL, apiKey: apiKey, model: settings.Model(), questions: questions, image: image}

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
	if image {
		if err := checkImages(documents, pending); err != nil {
			return nil, err
		}
	}

	var inputTokens, requests atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.inFlight))
	for _, batch := range request.batches(pending, documents) {
		eg.Go(func() error {
			batchDocuments := make([]string, len(batch))
			for j, index := range batch {
				batchDocuments[j] = documents[index]
			}
			decided, usage, err := c.decideDocuments(ctx, request, batchDocuments)
			if err != nil {
				return err
			}
			for j, index := range batch {
				answers[index] = decided[j]
			}
			inputTokens.Add(usage.InputTokens)
			requests.Add(1)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	c.logger.WithField("action", "decisions_typesafeai_decide").
		WithField("documents", len(documents)).
		WithField("questions", len(questions)).
		WithField("judged", len(pending)).
		WithField("requests", requests.Load()).
		WithField("input_tokens", inputTokens.Load()).
		Debug("typesafeai decide finished")

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

// decideDocuments sends one batch. A batch the API rejects for its size is
// sent as two halves, down to single documents.
func (c *client) decideDocuments(ctx context.Context, request decisionRequest, documents []string,
) ([][]ent.DecisionAnswer, typesafeaiUsage, error) {
	decided, usage, result, err := c.decideBatch(ctx, request, documents)
	if err == nil || !result.tooLarge || len(documents) < 2 {
		return decided, usage, err
	}
	half := len(documents) / 2
	first, firstUsage, err := c.decideDocuments(ctx, request, documents[:half])
	if err != nil {
		return nil, typesafeaiUsage{}, err
	}
	second, secondUsage, err := c.decideDocuments(ctx, request, documents[half:])
	if err != nil {
		return nil, typesafeaiUsage{}, err
	}
	return append(first, second...), typesafeaiUsage{InputTokens: firstUsage.InputTokens + secondUsage.InputTokens}, nil
}

func (c *client) decideBatch(ctx context.Context, request decisionRequest, documents []string,
) ([][]ent.DecisionAnswer, typesafeaiUsage, sendResult, error) {
	typesafeaiReq, err := newDecisionRequest(request, documents)
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, err
	}
	body, err := json.Marshal(typesafeaiReq)
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, errors.Wrap(err, "marshal body")
	}
	response, result, err := c.postWithRetry(ctx, request.url, request.apiKey, body, request.image)
	if err != nil {
		return nil, typesafeaiUsage{}, result, err
	}
	decided, err := response.decisions(request.questions, len(documents))
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, err
	}
	return decided, response.Usage, result, nil
}

// newDecisionRequest builds the request for one batch. A single document is
// the state itself and the questions are keyed by position. Several
// documents become a map, with every question repeated per document under
// a key that names the document it is about. An image is an asset and the
// state holds its reference.
func newDecisionRequest(request decisionRequest, documents []string) (typesafeaiRequest, error) {
	values, assets, err := stateValues(request.image, documents)
	if err != nil {
		return typesafeaiRequest{}, err
	}
	if len(documents) == 1 {
		questions := make(map[string]typesafeaiQuestion, len(request.questions))
		for i, question := range request.questions {
			questions[questionID(i)] = typesafeaiDecisionQuestion(question, "")
		}
		return typesafeaiRequest{Model: request.model, State: values[0], Questions: questions, Assets: assets}, nil
	}
	state := make(map[string]string, len(documents))
	questions := make(map[string]typesafeaiQuestion, len(documents)*len(request.questions))
	for d := range documents {
		key := batchKey(d)
		state[key] = values[d]
		for i, question := range request.questions {
			questions[batchQuestionID(d, i)] = typesafeaiDecisionQuestion(question, "In "+key+": ")
		}
	}
	return typesafeaiRequest{Model: request.model, State: state, Questions: questions, Assets: assets}, nil
}

func questionID(i int) string {
	return "q" + strconv.Itoa(i)
}

func batchQuestionID(document, question int) string {
	return batchKey(document) + "_" + questionID(question)
}

// typesafeaiDecisionQuestion maps a question onto Jev's types: a predicate
// is a noul, a choice has its options as criteria with their descriptions,
// a score has its levels as criteria, lowest first.
func typesafeaiDecisionQuestion(question ent.DecisionQuestion, prefix string) typesafeaiQuestion {
	out := typesafeaiQuestion{Instructions: prefix + question.Instructions}
	switch question.Kind {
	case ent.DecisionChoice:
		out.Type = choiceType
		criteria := make(map[string]*string, len(question.Options))
		for _, option := range question.Options {
			var description *string
			if option.Description != "" {
				description = &option.Description
			}
			criteria[option.Value] = description
		}
		out.Criteria = criteria
	case ent.DecisionScore:
		out.Type = scoreType
		criteria := make([]string, len(question.Levels))
		for i, level := range question.Levels {
			criteria[i] = level.Label
			if level.Description != "" {
				criteria[i] = level.Label + ": " + level.Description
			}
		}
		out.Criteria = criteria
	default:
		out.Type = noulType
	}
	return out
}

// decisions returns the answers of a batch, per document in the order of
// the questions.
func (r typesafeaiResponse) decisions(questions []ent.DecisionQuestion, documents int) ([][]ent.DecisionAnswer, error) {
	out := make([][]ent.DecisionAnswer, documents)
	for d := range documents {
		out[d] = make([]ent.DecisionAnswer, len(questions))
		for i, question := range questions {
			key := questionID(i)
			if documents > 1 {
				key = batchQuestionID(d, i)
			}
			answer, ok := r.Answers[key]
			if !ok {
				return nil, errors.Errorf("no answer in response for %q", key)
			}
			decided, err := decisionAnswer(question, answer)
			if err != nil {
				return nil, errors.Wrapf(err, "answer %q", key)
			}
			out[d][i] = decided
		}
	}
	return out, nil
}

// decisionAnswer reads one answer and checks it against the question: the
// type, the ranges, and that a choice names an option and the probabilities
// cover every option or level.
func decisionAnswer(question ent.DecisionQuestion, answer typesafeaiAnswer) (ent.DecisionAnswer, error) {
	out := ent.DecisionAnswer{Name: question.Name, Kind: question.Kind}
	switch question.Kind {
	case ent.DecisionPredicate:
		if answer.Type != noulType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Noul == nil {
			return out, errors.New("no probability in response")
		}
		if !config.ValidProbability(*answer.Noul) {
			return out, errors.Errorf("probability %v out of range", *answer.Noul)
		}
		out.Probability = *answer.Noul
	case ent.DecisionChoice:
		if answer.Type != choiceType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Choice == nil {
			return out, errors.New("no choice in response")
		}
		known := false
		for _, option := range question.Options {
			known = known || option.Value == *answer.Choice
		}
		if !known {
			return out, errors.Errorf("choice %q is not an option", *answer.Choice)
		}
		out.Choice = *answer.Choice
		probabilities := make([]ent.DecisionProbability, len(question.Options))
		for i, option := range question.Options {
			probability, err := namedProbability(answer.Probabilities, option.Value, "option")
			if err != nil {
				return out, err
			}
			probabilities[i] = ent.DecisionProbability{Value: option.Value, Probability: probability}
		}
		out.Probabilities = probabilities
	case ent.DecisionScore:
		if answer.Type != scoreType {
			return out, errors.Errorf("unexpected answer type %q", answer.Type)
		}
		if answer.Score == nil {
			return out, errors.New("no score in response")
		}
		if s := *answer.Score; s < 0 || s > float64(len(question.Levels)-1) {
			return out, errors.Errorf("score %v out of range", s)
		}
		out.Score = *answer.Score
		// Jev keys the probabilities of a score by level index.
		probabilities := make([]ent.DecisionProbability, len(question.Levels))
		for i, level := range question.Levels {
			probability, err := namedProbability(answer.Probabilities, strconv.Itoa(i), "level")
			if err != nil {
				return out, err
			}
			probabilities[i] = ent.DecisionProbability{Value: level.Label, Probability: probability}
		}
		out.Probabilities = probabilities
	default:
		return out, errors.Errorf("unknown question kind %q", question.Kind)
	}
	if answer.Confidence != nil {
		if !config.ValidProbability(*answer.Confidence) {
			return out, errors.Errorf("confidence %v out of range", *answer.Confidence)
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
	if !config.ValidProbability(probability) {
		return 0, errors.Errorf("probability %v of %s %q out of range", probability, what, name)
	}
	return probability, nil
}
