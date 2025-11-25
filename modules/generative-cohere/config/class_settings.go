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
	baseURLProperty       = "baseURL"
	modelProperty         = "model"
	temperatureProperty   = "temperature"
	maxTokensProperty     = "maxTokens"
	kProperty             = "k"
	stopSequencesProperty = "stopSequences"
)

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                     = "https://api.cohere.ai"
	DefaultCohereModel                 = "command-a-03-2025"
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
	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getIntPropertyWithLegacy(name string, defaultValue *int) *int {
	if ic.cfg == nil {
		return defaultValue
	}

	// Check for correct property name
	config := ic.cfg.ClassByModuleName("generative-cohere")
	if _, ok := config[name]; ok {
		return ic.getIntProperty(name, defaultValue)
	}

	// Fallback for legacy property names
	legacyName := name + "Property"
	return ic.getIntProperty(legacyName, defaultValue)
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloat64PropertyWithLegacy(name string, defaultValue *float64) *float64 {
	if ic.cfg == nil {
		return defaultValue
	}

	// Check for correct property name
	config := ic.cfg.ClassByModuleName("generative-cohere")
	if _, ok := config[name]; ok {
		return ic.getFloat64Property(name, defaultValue)
	}

	// Fallback for legacy property names
	legacyName := name + "Property"
	return ic.getFloat64Property(legacyName, defaultValue)
}

func (ic *classSettings) getFloat64Property(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-1)
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getListOfStringsPropertyWithLegacy(name string, defaultValue []string) *[]string {
	if ic.cfg == nil {
		return &defaultValue
	}

	// Check for correct property name
	config := ic.cfg.ClassByModuleName("generative-cohere")
	if _, ok := config[name]; ok {
		return ic.getListOfStringsProperty(name, defaultValue)
	}

	// Fallback for legacy property names
	legacyName := name + "Property"
	return ic.getListOfStringsProperty(legacyName, defaultValue)
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

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultCohereModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntPropertyWithLegacy(maxTokensProperty, &DefaultCohereMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloat64PropertyWithLegacy(temperatureProperty, &DefaultCohereTemperature)
}

func (ic *classSettings) K() int {
	return *ic.getIntPropertyWithLegacy(kProperty, &DefaultCohereK)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsPropertyWithLegacy(stopSequencesProperty, DefaultCohereStopSequences)
}
