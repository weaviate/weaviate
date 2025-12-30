//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	modelProperty            = "model"
	temperatureProperty      = "temperature"
	maxTokensProperty        = "maxTokens"
	frequencyPenaltyProperty = "frequencyPenalty"
	presencePenaltyProperty  = "presencePenalty"
	topPProperty             = "topP"
	baseURLProperty          = "baseURL"
)

var (
	DefaultDeepSeekModel            = "deepseek-chat"
	DefaultDeepSeekBaseURL          = "https://api.deepseek.com"
	DefaultDeepSeekFrequencyPenalty = 0.0
	DefaultDeepSeekPresencePenalty  = 0.0
	DefaultDeepSeekTopP             = 1.0
)

var defaultMaxTokens = map[string]float64{
	"deepseek-chat":     64000,
	"deepseek-reasoner": 64000,
}

func GetMaxTokensForModel(model string) *float64 {
	if maxTokens, ok := defaultMaxTokens[model]; ok {
		return &maxTokens
	}
	return nil
}

type ClassSettings interface {
	Model() string
	MaxTokens() *float64
	Temperature() *float64
	FrequencyPenalty() float64
	PresencePenalty() float64
	TopP() float64
	BaseURL() string
	Validate(class *models.Class) error
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) ClassSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-deepseek")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	temperature := ic.Temperature()
	if temperature != nil && (*temperature < 0 || *temperature > 2) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 2.0")
	}

	model := ic.getStringProperty(modelProperty, DefaultDeepSeekModel)
	maxTokens := ic.MaxTokens()
	maxTokensForModel := GetMaxTokensForModel(*model)
	if maxTokens != nil && (*maxTokens < 0 || (maxTokensForModel != nil && *maxTokens > *maxTokensForModel)) {
		return errors.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used")
	}

	frequencyPenalty := ic.getFloatProperty(frequencyPenaltyProperty, &DefaultDeepSeekFrequencyPenalty)
	if frequencyPenalty == nil || (*frequencyPenalty < -2 || *frequencyPenalty > 2) {
		return errors.Errorf("Wrong frequencyPenalty configuration, values are between -2.0 and 2.0")
	}

	presencePenalty := ic.getFloatProperty(presencePenaltyProperty, &DefaultDeepSeekPresencePenalty)
	if presencePenalty == nil || (*presencePenalty < -2 || *presencePenalty > 2) {
		return errors.Errorf("Wrong presencePenalty configuration, values are between -2.0 and 2.0")
	}

	topP := ic.getFloatProperty(topPProperty, &DefaultDeepSeekTopP)
	if topP == nil || (*topP < 0 || *topP > 1) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 0 and max of 1")
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

// FIX: Changed wrongVal from -1.0 to -100.0 so it triggers validation error
// even when the valid range allows negative numbers (like -2.0).
func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-100.0)
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultDeepSeekModel)
}

func (ic *classSettings) MaxTokens() *float64 {
	return ic.getFloatProperty(maxTokensProperty, nil)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultDeepSeekBaseURL)
}

func (ic *classSettings) Temperature() *float64 {
	return ic.getFloatProperty(temperatureProperty, nil)
}

func (ic *classSettings) FrequencyPenalty() float64 {
	return *ic.getFloatProperty(frequencyPenaltyProperty, &DefaultDeepSeekFrequencyPenalty)
}

func (ic *classSettings) PresencePenalty() float64 {
	return *ic.getFloatProperty(presencePenaltyProperty, &DefaultDeepSeekPresencePenalty)
}

func (ic *classSettings) TopP() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultDeepSeekTopP)
}
