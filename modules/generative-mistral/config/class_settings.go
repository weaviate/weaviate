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

var availableMistralModels = []string{
	"open-mistral-7b", "mistral-tiny-2312", "mistral-tiny", "open-mixtral-8x7b",
	"mistral-small-2312", "mistral-small", "mistral-small-2402", "mistral-small-latest",
	"mistral-medium-latest", "mistral-medium-2312", "mistral-medium", "mistral-large-latest",
	"mistral-large-2402",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                    = "https://api.mistral.ai"
	DefaultMistralModel               = "open-mistral-7b"
	DefaultMistralTemperature float64 = 0
	DefaultMistralMaxTokens           = 2048
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-mistral")}
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
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloat64Property(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-1)
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultMistralMaxTokens
}

func (ic *classSettings) validateModel(model string) bool {
	return basesettings.ValidateSetting(model, availableMistralModels)
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

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloat64Property(temperatureProperty, &DefaultMistralTemperature)
}
