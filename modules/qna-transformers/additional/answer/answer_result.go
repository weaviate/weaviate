//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package answer

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	qnamodels "github.com/weaviate/weaviate/modules/qna-transformers/additional/models"
	"github.com/weaviate/weaviate/modules/qna-transformers/ent"
)

func (p *AnswerProvider) findAnswer(ctx context.Context,
	in []search.Result, params *Params, limit *int,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	if len(in) > 0 {
		question := p.paramsHelper.GetQuestion(argumentModuleParams["ask"])
		if question == "" {
			return in, errors.New("empty question")
		}
		properties := p.paramsHelper.GetProperties(argumentModuleParams["ask"])

		for i := range in {
			textProperties := map[string]string{}
			schema := in[i].Object().Properties.(map[string]interface{})
			for property, value := range schema {
				if p.containsProperty(property, properties) {
					if valueString, ok := value.(string); ok && len(valueString) > 0 {
						textProperties[property] = valueString
					}
				}
			}

			texts := []string{}
			for _, value := range textProperties {
				texts = append(texts, value)
			}
			text := strings.Join(texts, " ")
			if len(text) == 0 {
				return in, errors.New("empty content")
			}

			answer, err := p.qna.Answer(ctx, text, question)
			if err != nil {
				return in, err
			}

			ap := in[i].AdditionalProperties
			if ap == nil {
				ap = models.AdditionalProperties{}
			}

			if answerMeetsSimilarityThreshold(argumentModuleParams["ask"], p.paramsHelper, answer) {
				propertyName, startPos, endPos := p.findProperty(answer.Answer, textProperties)
				ap["answer"] = &qnamodels.Answer{
					Result:        answer.Answer,
					Property:      propertyName,
					StartPosition: startPos,
					EndPosition:   endPos,
					Certainty:     answer.Certainty,
					Distance:      answer.Distance,
					HasAnswer:     answer.Answer != nil,
				}
			} else {
				ap["answer"] = &qnamodels.Answer{
					HasAnswer: false,
				}
			}

			in[i].AdditionalProperties = ap
		}
	}

	rerank := p.paramsHelper.GetRerank(argumentModuleParams["ask"])
	if rerank {
		return p.rerank(in), nil
	}
	return in, nil
}

func answerMeetsSimilarityThreshold(params interface{}, helper paramsHelper, ans *ent.AnswerResult) bool {
	certainty := helper.GetCertainty(params)
	if certainty > 0 && ans.Certainty != nil && *ans.Certainty < certainty {
		return false
	}

	distance := helper.GetDistance(params)
	if distance > 0 && ans.Distance != nil && *ans.Distance > distance {
		return false
	}

	return true
}

func (p *AnswerProvider) rerank(in []search.Result) []search.Result {
	if len(in) > 0 {
		sort.SliceStable(in, func(i, j int) bool {
			return p.getAnswerCertainty(in[i]) > p.getAnswerCertainty(in[j])
		})
	}
	return in
}

func (p *AnswerProvider) getAnswerCertainty(result search.Result) float64 {
	answerObj, ok := result.AdditionalProperties["answer"]
	if ok {
		answer, ok := answerObj.(*qnamodels.Answer)
		if ok {
			if answer.HasAnswer {
				return *answer.Certainty
			}
		}
	}
	return 0
}

func (p *AnswerProvider) containsProperty(property string, properties []string) bool {
	if len(properties) == 0 {
		return true
	}
	for i := range properties {
		if properties[i] == property {
			return true
		}
	}
	return false
}

func (p *AnswerProvider) findProperty(answer *string, textProperties map[string]string) (*string, int, int) {
	if answer == nil {
		return nil, 0, 0
	}
	lowercaseAnswer := strings.ToLower(*answer)
	if len(lowercaseAnswer) > 0 {
		for property, value := range textProperties {
			lowercaseValue := strings.ToLower(strings.ReplaceAll(value, "\n", " "))
			if strings.Contains(lowercaseValue, lowercaseAnswer) {
				startIndex := strings.Index(lowercaseValue, lowercaseAnswer)
				return &property, startIndex, startIndex + len(lowercaseAnswer)
			}
		}
	}
	propertyNotFound := ""
	return &propertyNotFound, 0, 0
}
