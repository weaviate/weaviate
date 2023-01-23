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
	"sync"

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

	switch onSet {
	case "individualResults":
		return p.generatePerSearchResult(ctx, in, properties, task, language, cfg)
	case "allResults":
		return p.generateForAllSearchResults(ctx, in, properties, task, language, cfg)
	default:
		return in, nil
	}
}

func (p *GenerateProvider) generatePerSearchResult(ctx context.Context, in []search.Result, properties []string, task string, language string, cfg moduletools.ClassConfig) ([]search.Result, error) {
	//nolint:gofumpt    //todo How to limit the number of go-routines??
	var wg sync.WaitGroup
	for i, result := range in {
		wg.Add(1)
		textProperties := p.getTextProperties(result, properties)
		go func(result search.Result, textProperties map[string]string, i int) {
			defer wg.Done()
			generateResult, err := p.client.Generate(ctx, []map[string]string{textProperties}, task, language, cfg)
			if err != nil {
				return
			}
			p.setResult(in, i, generateResult)
		}(result, textProperties, i)
	}
	wg.Wait()
	return in, nil
}

func (p *GenerateProvider) generateForAllSearchResults(ctx context.Context, in []search.Result, properties []string, task string, language string, cfg moduletools.ClassConfig) ([]search.Result, error) {
	var propertiesForAllDocs []map[string]string
	for _, res := range in {
		propertiesForAllDocs = append(propertiesForAllDocs, p.getTextProperties(res, properties))
	}
	generateResult, err := p.client.Generate(ctx, propertiesForAllDocs, task, language, cfg)
	if err != nil {
		return in, err
	}

	p.setResult(in, 0, generateResult)
	return in, nil
}

func (p *GenerateProvider) getTextProperties(result search.Result, properties []string) map[string]string {
	textProperties := map[string]string{}
	schema := result.Object().Properties.(map[string]interface{})
	for property, value := range schema {
		if p.containsProperty(property, properties) {
			if valueString, ok := value.(string); ok {
				textProperties[property] = valueString
			}
		}
	}
	return textProperties
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
