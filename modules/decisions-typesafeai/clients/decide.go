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
	batchSize int
	questions []ent.DecisionQuestion
}

// cacheKeys covers everything that can change the answers or who may read
// them: the endpoint, the API key, the model, the batch size, every
// question and the document. See judgeRequest.cacheKeys for the two keys.
func (r decisionRequest) cacheKeys(document string) documentKeys {
	single := r.cacheKey(document, 1)
	if r.batchSize == 1 {
		return documentKeys{batched: single, single: single}
	}
	return documentKeys{batched: r.cacheKey(document, r.batchSize), single: single}
}

func (r decisionRequest) cacheKey(document string, batchSize int) judgmentKey {
	parts := []string{"decide", r.url, r.apiKey, r.model, strconv.Itoa(batchSize), strconv.Itoa(len(r.questions))}
	for _, question := range r.questions {
		parts = append(parts, question.Name, string(question.Kind), question.Instructions,
			strconv.Itoa(len(question.Options)), strconv.Itoa(len(question.Levels)))
		for _, option := range question.Options {
			parts = append(parts, option.Value, option.Description)
		}
		for _, level := range question.Levels {
			parts = append(parts, level.Label, level.Description)
		}
	}
	return newJudgmentKey(append(parts, document)...)
}

// Decide asks Jev every question about every document and returns the
// answers per document, in the order of the questions. One request carries
// all the questions about a document, and batchSize documents share a
// request, as in Rank. Identical documents and documents judged before
// with the same questions are not sent again. A document without text gets
// refused answers.
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
	batchSize := min(max(settings.BatchSize(), 1), config.MaxBatchSize)
	apiKey, err := c.getApiKey(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "TypeSafeAI API Key")
	}
	typesafeaiURL, err := c.getTypeSafeAIURL(ctx, settings.BaseURL())
	if err != nil {
		return nil, err
	}
	cache, err := cacheModeOf(ctx, settings.Cache())
	if err != nil {
		return nil, err
	}
	request := decisionRequest{
		url: typesafeaiURL, apiKey: apiKey, model: settings.Model(), batchSize: batchSize, questions: questions,
	}

	firstIndex := make(map[string]int, len(documents))
	answers := make([][]ent.DecisionAnswer, len(documents))
	keys := make(map[int]documentKeys)
	var pending []int
	for i, document := range documents {
		if document == "" {
			continue
		}
		if _, ok := firstIndex[document]; ok {
			continue
		}
		firstIndex[document] = i
		documentKeys := request.cacheKeys(document)
		if cache == cacheOn {
			if stored, ok := c.decisions.get(documentKeys.batched); ok {
				answers[i] = stored
				continue
			}
			if documentKeys.batched != documentKeys.single {
				if stored, ok := c.decisions.get(documentKeys.single); ok {
					answers[i] = stored
					continue
				}
			}
		}
		keys[i] = documentKeys
		pending = append(pending, i)
	}

	var inputTokens, requests atomic.Int64
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(cap(c.inFlight))
	for start := 0; start < len(pending); start += batchSize {
		batch := pending[start:min(start+batchSize, len(pending))]
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
				key := keys[index].batched
				if len(batch) == 1 {
					key = keys[index].single
				}
				switch cache {
				case cacheOff:
					answers[index] = decided[j]
				case cacheRefresh:
					c.decisions.put(key, decided[j])
					if other := keys[index].batched; other != key {
						c.decisions.remove(other)
					}
					answers[index] = decided[j]
				default:
					answers[index] = c.decisions.putIfAbsent(key, decided[j])
				}
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
	body, err := json.Marshal(newDecisionRequest(request, documents))
	if err != nil {
		return nil, typesafeaiUsage{}, sendResult{}, errors.Wrap(err, "marshal body")
	}
	response, result, err := c.postWithRetry(ctx, request.url, request.apiKey, body)
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
// a key that names the document it is about.
func newDecisionRequest(request decisionRequest, documents []string) typesafeaiRequest {
	if len(documents) == 1 {
		questions := make(map[string]typesafeaiQuestion, len(request.questions))
		for i, question := range request.questions {
			questions[questionID(i)] = typesafeaiDecisionQuestion(question, "")
		}
		return typesafeaiRequest{Model: request.model, State: documents[0], Questions: questions}
	}
	state := make(map[string]string, len(documents))
	questions := make(map[string]typesafeaiQuestion, len(documents)*len(request.questions))
	for d, document := range documents {
		key := batchKey(d)
		state[key] = document
		for i, question := range request.questions {
			questions[batchQuestionID(d, i)] = typesafeaiDecisionQuestion(question, "In "+key+": ")
		}
	}
	return typesafeaiRequest{Model: request.model, State: state, Questions: questions}
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
