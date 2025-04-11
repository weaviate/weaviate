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
	topKProperty          = "topK"
	topPProperty          = "topP"
)

// todo: anthropic make a distinction between input and output tokens
// so have a context window and a max output tokens, while the max
// input tokens for all models is 200000, the max output tokens is shorter at
// 4096
var defaultMaxTokens = map[string]int{
	"claude-3-5-sonnet-20240620": 4096,
	"claude-3-opus-20240229":     4096,
	"claude-3-sonnet-20240229":   4096,
	"claude-3-haiku-20240307":    4096,
	"claude-3-5-sonnet-20241022": 8192,
	"claude-3-5-haiku-20241022":  8192,
}

var (
	DefaultBaseURL              = "https://api.anthropic.com"
	DefaultAnthropicModel       = "claude-3-5-sonnet-20240620"
	DefaultAnthropicTemperature = 1.0
	// DefaultAnthropicMaxTokens - 4096 is the max output tokens, input tokens are typically much larger
	DefaultAnthropicMaxTokens     = 4096
	DefaultAnthropicTopK          = 0
	DefaultAnthropicTopP          = 0.0
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
	return nil
}

func (ic *classSettings) getStringProperty(property string, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, property, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-1.0)
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

func (ic *classSettings) GetMaxTokensForModel(model string) *int {
	if maxTokens, ok := defaultMaxTokens[model]; ok {
		return &maxTokens
	}
	return &DefaultAnthropicMaxTokens
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultAnthropicModel)
}

func (ic *classSettings) MaxTokens() *int {
	return ic.getIntProperty(maxTokensProperty, nil)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultAnthropicTemperature)
}

func (ic *classSettings) TopK() int {
	return *ic.getIntProperty(topKProperty, &DefaultAnthropicTopK)
}

func (ic *classSettings) TopP() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultAnthropicTopP)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultAnthropicStopSequences)
}
