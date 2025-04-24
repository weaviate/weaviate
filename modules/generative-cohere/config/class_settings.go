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
	temperatureProperty   = "temperature"
	maxTokensProperty     = "maxTokens"
	kProperty             = "k"
	stopSequencesProperty = "stopSequences"
)

var availableCohereModels = []string{
	"command-r-plus", "command-r", "command-xlarge-beta",
	"command-xlarge", "command-medium", "command-xlarge-nightly", "command-medium-nightly", "xlarge", "medium",
	"command", "command-light", "command-nightly", "command-light-nightly", "base", "base-light",
}

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                     = "https://api.cohere.ai"
	DefaultCohereModel                 = "command-r"
	DefaultCohereTemperature   float64 = 0
	DefaultCohereMaxTokens             = 2048
	DefaultCohereK                     = 0
	DefaultCohereStopSequences         = []string{}
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-cohere")}
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
	return basesettings.ValidateSetting(model, availableCohereModels)
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

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloat64Property(temperatureProperty, &DefaultCohereTemperature)
}

func (ic *classSettings) K() int {
	return *ic.getIntProperty(kProperty, &DefaultCohereK)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultCohereStopSequences)
}
