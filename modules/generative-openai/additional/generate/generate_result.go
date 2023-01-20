//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package generate

import (
	"context"
	"errors"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativemodels "github.com/weaviate/weaviate/modules/generative-openai/additional/models"
)

func (p *GenerateProvider) findResults(ctx context.Context, in []search.Result, params *Params, limit *int, argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
	//nolint:gofumpt    //todo switch here depending on input
	task := params.Task
	onSet := params.OnSet
	language := params.ResultLanguage
	properties := params.Properties

	if onSet == "individualResults" {
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

			generateResult, err := p.client.Generate(ctx, text, task, language, cfg)
			if err != nil {
				return in, err
			}

			ap := in[i].AdditionalProperties
			if ap == nil {
				ap = models.AdditionalProperties{}
			}

			ap["generate"] = &generativemodels.GenerateResult{
				Result: generateResult.Result,
			}

			in[i].AdditionalProperties = ap
		}
	}

	return in, nil
}

func (p *GenerateProvider) containsProperty(property string, properties []string) bool {
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
