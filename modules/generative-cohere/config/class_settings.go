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
	baseURLProperty           = "baseURL"
	modelProperty             = "model"
	temperatureProperty       = "temperature"
	maxTokensProperty         = "maxTokens"
	kProperty                 = "k"
	stopSequencesProperty     = "stopSequences"
	returnLikelihoodsProperty = "returnLikelihoods"
)

var availableCohereModels = []string{
	"command-xlarge-beta",
	"command-xlarge", "command-medium", "command-xlarge-nightly", "command-medium-nightly", "xlarge", "medium",
	"command", "command-light", "command-nightly", "command-light-nightly", "base", "base-light",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                 = "https://api.cohere.ai"
	DefaultCohereModel             = "command-nightly"
	DefaultCohereTemperature       = 0
	DefaultCohereMaxTokens         = 2048
	DefaultCohereK                 = 0
	DefaultCohereStopSequences     = []string{}
	DefaultCohereReturnLikelihoods = "NONE"
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
	model := ic.getStringProperty(modelProperty, DefaultCohereModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Cohere model name, available model names are: %v", availableCohereModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
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

	val, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
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

func (ic *classSettings) getListOfStringsProperty(name string, defaultValue []string) *[]string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-cohere")[name]
	if ok {
		asStringList, ok := model.([]string)
		if ok {
			return &asStringList
		}
		var empty []string
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultCohereMaxTokens
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableCohereModels, model)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultCohereModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultCohereMaxTokens)
}

func (ic *classSettings) Temperature() int {
	return *ic.getIntProperty(temperatureProperty, &DefaultCohereTemperature)
}

func (ic *classSettings) K() int {
	return *ic.getIntProperty(kProperty, &DefaultCohereK)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultCohereStopSequences)
}

func (ic *classSettings) ReturnLikelihoods() string {
	return *ic.getStringProperty(returnLikelihoodsProperty, DefaultCohereReturnLikelihoods)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
