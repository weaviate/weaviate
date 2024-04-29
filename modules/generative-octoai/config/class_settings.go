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
	baseURLProperty     = "baseURL"
	modelProperty       = "model"
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"
)

var availableOctoAIModels = []string{
	"qwen1.5-32b-chat",
	"meta-llama-3-8b-instruct",
	"meta-llama-3-70b-instruct",
	"mixtral-8x22b-instruct",
	"nous-hermes-2-mixtral-8x7b-dpo",
	"mixtral-8x7b-instruct",
	"mixtral-8x22b-finetuned",
	"hermes-2-pro-mistral-7b",
	"mistral-7b-instruct",
	"codellama-7b-instruct",
	"codellama-13b-instruct",
	"codellama-34b-instruct",
	"llama-2-13b-chat",
	"llama-2-70b-chat",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL           = "https://text.octoai.run"
	DefaultOctoAIModel       = "mistral-7b-instruct"
	DefaultOctoAITemperature = 0
	DefaultOctoAIMaxTokens   = 2048
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-octoai")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultOctoAIModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong OctoAI model name, available model names are: %v", availableOctoAIModels)
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	var wrongVal int = -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultOctoAIMaxTokens
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableOctoAIModels, model)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultOctoAIModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultOctoAIMaxTokens)
}

func (ic *classSettings) Temperature() int {
	return *ic.getIntProperty(temperatureProperty, &DefaultOctoAITemperature)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
