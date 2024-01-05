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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
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
	DefaultOpenAIModel                    = "text-ada-001"
	DefaultOpenAITemperature      float64 = 0.0
	DefaultOpenAIMaxTokens        float64 = 16
	DefaultOpenAIFrequencyPenalty float64 = 0.0
	DefaultOpenAIPresencePenalty  float64 = 0.0
	DefaultOpenAITopP             float64 = 1.0
	DefaultOpenAIBaseURL                  = "https://api.openai.com"
)

var maxTokensForModel = map[string]float64{
	"text-ada-001":           2048,
	"text-babbage-001":       2048,
	"text-curie-001":         2048,
	"text-davinci-002":       4000,
	"text-davinci-003":       4000,
	"gpt-3.5-turbo-instruct": 4000,
}

var availableOpenAIModels = []string{
	"text-ada-001",
	"text-babbage-001",
	"text-curie-001",
	"text-davinci-002",
	"text-davinci-003",
	"gpt-3.5-turbo-instruct",
}

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

	model := ic.getStringProperty(modelProperty, DefaultOpenAIModel)
	if model == nil || !ic.validateOpenAISetting(*model, availableOpenAIModels) {
		return errors.Errorf("wrong OpenAI model name, available model names are: %v", availableOpenAIModels)
	}

	temperature := ic.getFloatProperty(temperatureProperty, &DefaultOpenAITemperature)
	if temperature == nil || (*temperature < 0 || *temperature > 1) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0")
	}

	maxTokens := ic.getFloatProperty(maxTokensProperty, &DefaultOpenAIMaxTokens)
	if maxTokens == nil || (*maxTokens < 0 || *maxTokens > getMaxTokensForModel(*model)) {
		return errors.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used")
	}

	frequencyPenalty := ic.getFloatProperty(frequencyPenaltyProperty, &DefaultOpenAIFrequencyPenalty)
	if frequencyPenalty == nil || (*frequencyPenalty < 0 || *frequencyPenalty > 1) {
		return errors.Errorf("Wrong frequencyPenalty configuration, values are between 0.0 and 1.0")
	}

	presencePenalty := ic.getFloatProperty(presencePenaltyProperty, &DefaultOpenAIPresencePenalty)
	if presencePenalty == nil || (*presencePenalty < 0 || *presencePenalty > 1) {
		return errors.Errorf("Wrong presencePenalty configuration, values are between 0.0 and 1.0")
	}

	topP := ic.getFloatProperty(topPProperty, &DefaultOpenAITopP)
	if topP == nil || (*topP < 0 || *topP > 5) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5")
	}

	err := ic.validateAzureConfig(ic.ResourceName(), ic.DeploymentID())
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("qna-openai")[name]
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

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("qna-openai")[name]
	if ok {
		asFloat, ok := val.(float64)
		if ok {
			return &asFloat
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			return &asFloat
		}
		asInt, ok := val.(int)
		if ok {
			asFloat := float64(asInt)
			return &asFloat
		}
		var wrongVal float64 = -1.0
		return &wrongVal
	}

	if defaultValue != nil {
		return defaultValue
	}
	return nil
}

func getMaxTokensForModel(model string) float64 {
	return maxTokensForModel[model]
}

func (ic *classSettings) validateOpenAISetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultOpenAIModel)
}

func (ic *classSettings) MaxTokens() float64 {
	return *ic.getFloatProperty(maxTokensProperty, &DefaultOpenAIMaxTokens)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultOpenAIBaseURL)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultOpenAITemperature)
}

func (ic *classSettings) FrequencyPenalty() float64 {
	return *ic.getFloatProperty(frequencyPenaltyProperty, &DefaultOpenAIFrequencyPenalty)
}

func (ic *classSettings) PresencePenalty() float64 {
	return *ic.getFloatProperty(presencePenaltyProperty, &DefaultOpenAIPresencePenalty)
}

func (ic *classSettings) TopP() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultOpenAITopP)
}

func (ic *classSettings) ResourceName() string {
	return *ic.getStringProperty("resourceName", "")
}

func (ic *classSettings) DeploymentID() string {
	return *ic.getStringProperty("deploymentId", "")
}

func (ic *classSettings) IsAzure() bool {
	return ic.ResourceName() != "" && ic.DeploymentID() != ""
}

func (ic *classSettings) validateAzureConfig(resourceName string, deploymentId string) error {
	if (resourceName == "" && deploymentId != "") || (resourceName != "" && deploymentId == "") {
		return fmt.Errorf("both resourceName and deploymentId must be provided")
	}
	return nil
}
