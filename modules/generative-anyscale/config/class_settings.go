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
)

var availableAnyscaleModels = []string{
	"meta-llama/Llama-2-70b-chat-hf",
	"meta-llama/Llama-2-13b-chat-hf",
	"meta-llama/Llama-2-7b-chat-hf",
	"codellama/CodeLlama-34b-Instruct-hf",
	"mistralai/Mistral-7B-Instruct-v0.1",
	"mistralai/Mixtral-8x7B-Instruct-v0.1",
}

// note we might want to separate the baseURL and completions URL in the future. Fine-tuned models also use this URL. 12/3/23
var (
	DefaultBaseURL             = "https://api.endpoints.anyscale.com"
	DefaultAnyscaleModel       = "meta-llama/Llama-2-70b-chat-hf"
	DefaultAnyscaleTemperature = 0
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-anyscale")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	model := ic.getStringProperty(modelProperty, DefaultAnyscaleModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Anyscale model name, available model names are: %v", availableAnyscaleModels)
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

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableAnyscaleModels, model)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultAnyscaleModel)
}

func (ic *classSettings) Temperature() int {
	return *ic.getIntProperty(temperatureProperty, &DefaultAnyscaleTemperature)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
