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

package generate

import (
	"context"
	"regexp"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

func (p *GenerateProvider) generateResult(ctx context.Context, in []search.Result, params *Params, limit *int, argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
	prompt := params.Prompt
	task := params.Task
	properties := params.Properties
	var err error

	if task != nil {
		_, err = p.generateForAllSearchResults(ctx, in, *task, properties, cfg)
	}
	if prompt != nil {
		prompt, err = validatePrompt(prompt)
		if err != nil {
			return nil, err
		}
		_, err = p.generatePerSearchResult(ctx, in, *prompt, cfg)
	}

	return in, err
}

func validatePrompt(prompt *string) (*string, error) {
	matched, err := regexp.MatchString("{([\\s\\w]*)}", *prompt)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, errors.Errorf("Prompt does not contain any properties. Use {PROPERTY_NAME} in the prompt to instuct Weaviate which data to use")
	}

	return prompt, err
}

func (p *GenerateProvider) generatePerSearchResult(ctx context.Context, in []search.Result, prompt string, cfg moduletools.ClassConfig) ([]search.Result, error) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.maximumNumberOfGoroutines)
	for i, result := range in {
		wg.Add(1)
		textProperties := p.getTextProperties(result, nil)
		go func(result search.Result, textProperties map[string]string, i int) {
			sem <- struct{}{}
			defer wg.Done()
			defer func() { <-sem }()
			generateResult, err := p.client.GenerateSingleResult(ctx, textProperties, prompt, cfg)
			p.setIndividualResult(in, i, generateResult, err)
		}(result, textProperties, i)
	}
	wg.Wait()
	return in, nil
}

func (p *GenerateProvider) generateForAllSearchResults(ctx context.Context, in []search.Result, task string, properties []string, cfg moduletools.ClassConfig) ([]search.Result, error) {
	var propertiesForAllDocs []map[string]string
	for _, res := range in {
		propertiesForAllDocs = append(propertiesForAllDocs, p.getTextProperties(res, properties))
	}
	generateResult, err := p.client.GenerateAllResults(ctx, propertiesForAllDocs, task, cfg)
	p.setCombinedResult(in, 0, generateResult, err)
	return in, nil
}

func (p *GenerateProvider) getTextProperties(result search.Result, properties []string) map[string]string {
	textProperties := map[string]string{}
	schema := result.Object().Properties.(map[string]interface{})
	for property, value := range schema {
		if len(properties) > 0 {
			if p.containsProperty(property, properties) {
				if valueString, ok := value.(string); ok {
					textProperties[property] = valueString
				}
			}
		} else {
			if valueString, ok := value.(string); ok {
				textProperties[property] = valueString
			}
		}
	}
	return textProperties
}

func (p *GenerateProvider) setCombinedResult(in []search.Result, i int, generateResult *generativemodels.GenerateResponse, err error) {
	ap := in[i].AdditionalProperties
	if ap == nil {
		ap = models.AdditionalProperties{}
	}

	var result *string
	if generateResult != nil {
		result = generateResult.Result
	}

	ap["generate"] = &generativemodels.GenerateResult{
		GroupedResult: result,
		Error:         err,
	}

	in[i].AdditionalProperties = ap
}

func (p *GenerateProvider) setIndividualResult(in []search.Result, i int, generateResult *generativemodels.GenerateResponse, err error) {
	var result *string
	if generateResult != nil {
		result = generateResult.Result
	}

	ap := in[i].AdditionalProperties
	if ap == nil {
		ap = models.AdditionalProperties{}
	}

	if ap["generate"] != nil {
		ap["generate"] = &generativemodels.GenerateResult{
			GroupedResult: ap["generate"].(*generativemodels.GenerateResult).GroupedResult,
			SingleResult:  result,
			Error:         err,
		}
	} else {
		ap["generate"] = &generativemodels.GenerateResult{
			SingleResult: result,
			Error:        err,
		}
	}

	in[i].AdditionalProperties = ap
}

func (p *GenerateProvider) containsProperty(property string, properties []string) bool {
	for i := range properties {
		if properties[i] == property {
			return true
		}
	}
	return false
}
