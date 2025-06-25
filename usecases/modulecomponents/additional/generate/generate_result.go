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
	"strings"
	"sync"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
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

	var propertyDataTypes map[string]schema.DataType
	if cfg != nil {
		propertyDataTypes = cfg.PropertiesDataTypes() // do once for all results to avoid loops over the schema
	}
	if task != nil {
		_, err = p.generateForAllSearchResults(ctx, in, *task, properties, client, settings, debug, cfg, propertyDataTypes)
	}
	if prompt != nil {
		_, err = p.generatePerSearchResult(ctx, in, *prompt, client, settings, debug, cfg, propertyDataTypes)
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

func (p *GenerateProvider) generatePerSearchResult(ctx context.Context,
	in []search.Result,
	prompt string,
	client modulecapabilities.GenerativeClient,
	settings interface{},
	debug bool,
	cfg moduletools.ClassConfig,
	propertyDataTypes map[string]schema.DataType,
) ([]search.Result, error) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.maximumNumberOfGoroutines)
	for i := range in {
		wg.Add(1)
		i := i
		enterrors.GoWrapper(func() {
			sem <- struct{}{}
			defer wg.Done()
			defer func() { <-sem }()
			var props *modulecapabilities.GenerateProperties
			if propertyDataTypes != nil {
				props = p.getProperties(in[i], nil, propertyDataTypes)
			}
			generateResult, err := client.GenerateSingleResult(ctx, props, prompt, settings, debug, cfg)
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
	propertyDataTypes map[string]schema.DataType,
) ([]search.Result, error) {
	var propertiesForAllDocs []*modulecapabilities.GenerateProperties
	if propertyDataTypes != nil {
		for _, res := range in {
			propertiesForAllDocs = append(propertiesForAllDocs, p.getProperties(res, properties, propertyDataTypes))
		}
	}
	generateResult, err := client.GenerateAllResults(ctx, propertiesForAllDocs, task, settings, debug, cfg)
	p.setCombinedResult(in, 0, generateResult, err)
	return in, nil
}

func (p *GenerateProvider) getProperties(result search.Result,
	properties []string, propertyDataTypes map[string]schema.DataType,
) *modulecapabilities.GenerateProperties {
	textProperties := map[string]string{}
	blobProperties := map[string]*string{}
	allProperties := result.Object().Properties.(map[string]interface{})
	for property, value := range allProperties {
		if len(properties) > 0 && !p.containsProperty(property, properties) {
			continue
		}

		// Nil property is not useful as an input to a generative model.
		if value == nil {
			continue
		}

		if dt, ok := propertyDataTypes[property]; ok {
			switch dt {
			// todo: add rest of types
			case schema.DataTypeText:
				textProperties[property] = value.(string)
			case schema.DataTypeTextArray:
				textProperties[property] = strings.Join(value.([]string), ",")
			case schema.DataTypeBlob:
				v := value.(string)
				blobProperties[property] = &v
			default:
			}
		}
	}
	return &modulecapabilities.GenerateProperties{
		Text: textProperties,
		Blob: blobProperties,
	}
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

	// pulls out the error from the task rather than cloberring it below
	if g := ap["generate"]; g != nil {
		if e := g.(map[string]interface{})["error"]; e != nil {
			err = e.(error)
		}
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
