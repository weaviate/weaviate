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
	"fmt"
	"regexp"
	"strings"
	"sync"

	enterrors "github.com/liutizhong/weaviate/entities/errors"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"

	"github.com/pkg/errors"
	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/search"
)

func (p *GenerateProvider) generateResult(ctx context.Context,
	in []search.Result,
	params *Params,
	limit *int,
	argumentModuleParams map[string]interface{},
	cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if len(in) == 0 {
		return in, nil
	}
	prompt := params.Prompt
	task := params.Task
	properties := params.Properties
	debug := params.Debug
	provider, settings := p.getProviderSettings(params)
	client, err := p.getClient(provider)
	if err != nil {
		return nil, err
	}

	if task != nil {
		_, err = p.generateForAllSearchResults(ctx, in, *task, properties, client, settings, debug, cfg)
	}
	if prompt != nil {
		prompt, err = validatePrompt(prompt)
		if err != nil {
			return nil, err
		}
		_, err = p.generatePerSearchResult(ctx, in, *prompt, client, settings, debug, cfg)
	}

	return in, err
}

func (p *GenerateProvider) getProviderSettings(params *Params) (string, interface{}) {
	for name, settings := range params.Options {
		return name, settings
	}
	return p.defaultProviderName, nil
}

func (p *GenerateProvider) getClient(provider string) (modulecapabilities.GenerativeClient, error) {
	if len(p.additionalGenerativeParameters) > 0 {
		if generativeParams, ok := p.additionalGenerativeParameters[provider]; ok && generativeParams.Client != nil {
			return generativeParams.Client, nil
		}
	}
	if provider == "" {
		if len(p.additionalGenerativeParameters) == 1 {
			for _, params := range p.additionalGenerativeParameters {
				return params.Client, nil
			}
		}
		return nil, fmt.Errorf("client not found, empty provider")
	}
	return nil, fmt.Errorf("client not found for provider: %s", provider)
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

func (p *GenerateProvider) generatePerSearchResult(ctx context.Context,
	in []search.Result,
	prompt string,
	client modulecapabilities.GenerativeClient,
	settings interface{},
	debug bool,
	cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.maximumNumberOfGoroutines)
	for i, result := range in {
		wg.Add(1)
		i := i
		textProperties := p.getTextProperties(result, nil)
		enterrors.GoWrapper(func() {
			sem <- struct{}{}
			defer wg.Done()
			defer func() { <-sem }()
			generateResult, err := client.GenerateSingleResult(ctx, textProperties, prompt, settings, debug, cfg)
			p.setIndividualResult(in, i, generateResult, err)
		}, p.logger)
	}
	wg.Wait()
	return in, nil
}

func (p *GenerateProvider) generateForAllSearchResults(ctx context.Context,
	in []search.Result,
	task string,
	properties []string,
	client modulecapabilities.GenerativeClient,
	settings interface{},
	debug bool,
	cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	var propertiesForAllDocs []map[string]string
	for _, res := range in {
		propertiesForAllDocs = append(propertiesForAllDocs, p.getTextProperties(res, properties))
	}
	generateResult, err := client.GenerateAllResults(ctx, propertiesForAllDocs, task, settings, debug, cfg)
	p.setCombinedResult(in, 0, generateResult, err)
	return in, nil
}

func (p *GenerateProvider) getTextProperties(result search.Result,
	properties []string,
) map[string]string {
	textProperties := map[string]string{}
	schema := result.Object().Properties.(map[string]interface{})
	for property, value := range schema {
		if len(properties) > 0 {
			if p.containsProperty(property, properties) {
				if valueString, ok := value.(string); ok {
					textProperties[property] = valueString
				} else if valueArray, ok := value.([]string); ok {
					textProperties[property] = strings.Join(valueArray, ",")
				}
			}
		} else {
			if valueString, ok := value.(string); ok {
				textProperties[property] = valueString
			} else if valueArray, ok := value.([]string); ok {
				textProperties[property] = strings.Join(valueArray, ",")
			}
		}
	}
	return textProperties
}

func (p *GenerateProvider) setCombinedResult(in []search.Result, i int,
	generateResult *modulecapabilities.GenerateResponse, err error,
) {
	ap := in[i].AdditionalProperties
	if ap == nil {
		ap = models.AdditionalProperties{}
	}

	var result *string
	var params map[string]interface{}
	if generateResult != nil {
		result = generateResult.Result
		params = generateResult.Params
	}

	generate := map[string]interface{}{
		"groupedResult": result,
		"error":         err,
	}

	for k, v := range params {
		generate[k] = v
	}

	ap["generate"] = generate

	in[i].AdditionalProperties = ap
}

func (p *GenerateProvider) setIndividualResult(in []search.Result, i int,
	generateResult *modulecapabilities.GenerateResponse, err error,
) {
	var result *string
	var params map[string]interface{}
	var debug *modulecapabilities.GenerateDebugInformation
	if generateResult != nil {
		result = generateResult.Result
		params = generateResult.Params
		debug = generateResult.Debug
	}

	ap := in[i].AdditionalProperties
	if ap == nil {
		ap = models.AdditionalProperties{}
	}

	generate := map[string]interface{}{
		"singleResult": result,
		"error":        err,
		"debug":        debug,
	}

	for k, v := range params {
		generate[k] = v
	}

	if ap["generate"] != nil {
		generate["groupedResult"] = ap["generate"].(map[string]interface{})["groupedResult"]
	}

	ap["generate"] = generate

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
