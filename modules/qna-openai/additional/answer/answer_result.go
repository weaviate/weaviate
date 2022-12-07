//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package answer

import (
	"context"
	"errors"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/search"
	qnamodels "github.com/semi-technologies/weaviate/modules/qna-openai/additional/models"
)

func (p *AnswerProvider) findAnswer(ctx context.Context, in []search.Result, params *Params, limit *int, argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
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

		var texts []string
		for _, value := range textProperties {
			texts = append(texts, value)
		}
		text := strings.Join(texts, " ")
		if len(text) == 0 {
			return in, errors.New("empty content")
		}

		answer, err := p.qna.Answer(ctx, text, question, cfg)
		if err != nil {
			return in, err
		}

		ap := in[i].AdditionalProperties
		if ap == nil {
			ap = models.AdditionalProperties{}
		}
		propertyName, startPos, endPos := p.findProperty(answer.Answer, textProperties)
		ap["answer"] = &qnamodels.Answer{
			Result:        answer.Answer,
			Property:      propertyName,
			StartPosition: startPos,
			EndPosition:   endPos,
			HasAnswer:     answer.Answer != nil,
		}

		in[i].AdditionalProperties = ap
	}

	return in, nil
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
