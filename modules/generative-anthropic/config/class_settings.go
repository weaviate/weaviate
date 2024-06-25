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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	baseURLProperty       = "baseURL"
	modelProperty         = "model"
	maxTokensProperty     = "maxTokens"
	stopSequencesProperty = "stopSequences"
	temperatureProperty   = "temperature"
	topKProperty          = "k"
	topPProperty          = "p"
)

var availableAnthropicModels = []string{
	"claude-3-5-sonnet-20240620",
	"claude-3-opus-20240229",
	"claude-3-sonnet-20240229",
	"claude-3-haiku-20240307",
}

// todo: anthropic make a distinction between input and output tokens
// so have a context window and a max ouput tokens, for now
// we will use the value for context window as max tokens
var defaultMaxTokens = map[string]int{
	"claude-3-5-sonnet-20240620": 200000,
	"claude-3-opus-20240229":     200000,
	"claude-3-sonnet-20240229":   200000,
	"claude-3-haiku-20240307":    200000,
}

var (
	DefaultBaseURL                = "https://api.anthropic.com"
	DefaultAnthropicModel         = "claude-3-5-sonnet-20240620"
	DefaultAnthropicTemperature   = 1.0
	DefaultAnthropicMaxTokens     = 200000
	DefaultAnthropicK             = 0
	DefaultAnthropicP             = 0.0
	DefaultAnthropicStopSequences = []string{}
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

// NewClassSettings creates a new classSettings instance
func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-anthropic")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultAnthropicModel)

	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Anthropic model name, available model names are: %v", availableAnthropicModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(property string, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, property, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	var wrongVal int = -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	var wrongVal float64 = -1.0
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getListOfStringsProperty(name string, defaultValue []string) *[]string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-anthropic")[name]
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
	return defaultMaxTokens[model]
}

// possibly we just validate the model name is not empty, to allow
// for future models to be added without changing the code
func (ic *classSettings) validateModel(model string) bool {
	return contains(availableAnthropicModels, model)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultAnthropicModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultAnthropicMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultAnthropicTemperature)
}

func (ic *classSettings) K() int {
	return *ic.getIntProperty(topKProperty, &DefaultAnthropicK)
}

func (ic *classSettings) P() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultAnthropicP)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultAnthropicStopSequences)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
