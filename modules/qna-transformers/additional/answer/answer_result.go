//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package answer

import (
	"context"
	"errors"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	qnamodels "github.com/semi-technologies/weaviate/modules/qna-transformers/additional/models"
)

func (p *AnswerProvider) findAnswer(ctx context.Context,
	in []search.Result, params *Params, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	if len(in) > 0 {
		textFields := []string{}
		schema := in[0].Object().Properties.(map[string]interface{})
		for _, value := range schema {
			if valueString, ok := value.(string); ok && len(valueString) > 0 {
				textFields = append(textFields, valueString)
			}
		}

		text := strings.Join(textFields, " ")
		if len(text) == 0 {
			return in, errors.New("empty content")
		}
		question := p.paramsHelper.GetQuestion(argumentModuleParams["ask"])
		if question == "" {
			return in, errors.New("empty question")
		}

		answer, err := p.qna.Answer(ctx, text, question)
		if err != nil {
			return in, err
		}

		ap := in[0].AdditionalProperties
		if ap == nil {
			ap = models.AdditionalProperties{}
		}

		ap["answer"] = &qnamodels.Answer{
			Result:        answer.Answer,
			StartPosition: answer.StartPosition,
			EndPosition:   answer.EndPosition,
		}

		in[0].AdditionalProperties = ap
	}

	return in, nil
}
