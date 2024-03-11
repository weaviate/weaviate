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

package config

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	baseURLProperty     = "baseURL"
	modelProperty       = "model"
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"
)

var availableMistralModels = []string{
	"open-mistral-7b", "mistral-tiny-2312", "mistral-tiny", "open-mixtral-8x7b",
	"mistral-small-2312", "mistral-small", "mistral-small-2402", "mistral-small-latest",
	"mistral-medium-latest", "mistral-medium-2312", "mistral-medium", "mistral-large-latest",
	"mistral-large-2402",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL            = "https://api.mistral.ai"
	DefaultMistralModel       = "open-mistral-7b"
	DefaultMistralTemperature = 0
	DefaultMistralMaxTokens   = 2048
)

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultMistralModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Mistral model name, available model names are: %v", availableMistralModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-mistral")[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return &asString
		}
		var empty string
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-mistral")[name]
	if ok {
		asInt, ok := val.(int)
		if ok {
			return &asInt
		}
		asFloat, ok := val.(float64)
		if ok {
			asInt := int(asFloat)
			return &asInt
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			asInt := int(asFloat)
			return &asInt
		}
		var wrongVal int = -1
		return &wrongVal
	}

	if defaultValue != nil {
		return defaultValue
	}
	return nil
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultMistralMaxTokens
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableMistralModels, model)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultMistralModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultMistralMaxTokens)
}

func (ic *classSettings) Temperature() int {
	return *ic.getIntProperty(temperatureProperty, &DefaultMistralTemperature)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
