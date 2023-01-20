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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativemodels "github.com/weaviate/weaviate/modules/generative-openai/additional/models"
	"github.com/weaviate/weaviate/modules/generative-openai/ent"
)

func (p *GenerateProvider) generateResult(ctx context.Context, in []search.Result, params *Params, limit *int, argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
	task := params.Task
	onSet := params.OnSet
	language := params.ResultLanguage
	properties := params.Properties

	var propertieForAllDocs []map[string]string
	for i := range in {
		textProperties := p.getTextProperties(in, i, properties)
		i2 := []map[string]string{textProperties}
		if onSet == "individualResults" {
			generateResult, err := p.client.Generate(ctx, i2, task, language, cfg)
			if err != nil {
				return in, err
			}

			p.setResult(in, 0, generateResult)
		} else {
			propertieForAllDocs = append(propertieForAllDocs, p.getTextProperties(in, i, properties))
		}

	}

	if len(propertieForAllDocs) > 0 {
		generateResult, err := p.client.Generate(ctx, propertieForAllDocs, task, language, cfg)
		if err != nil {
			return in, err
		}

		p.setResult(in, 0, generateResult)
	}
	return in, nil
}

func (p *GenerateProvider) setResult(in []search.Result, i int, generateResult *ent.GenerateResult) {
	ap := in[i].AdditionalProperties
	if ap == nil {
		ap = models.AdditionalProperties{}
	}

	ap["generate"] = &generativemodels.GenerateResult{
		Result: generateResult.Result,
	}

	in[i].AdditionalProperties = ap
}

func (p *GenerateProvider) getTextProperties(in []search.Result, i int, properties []string) map[string]string {
	textProperties := map[string]string{}
	schema := in[i].Object().Properties.(map[string]interface{})
	for property, value := range schema {
		if p.containsProperty(property, properties) {
			if valueString, ok := value.(string); ok && len(valueString) > 0 {
				textProperties[property] = valueString
			}
		}
	}
	return textProperties
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
