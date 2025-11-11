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
	modelProperty           = "model"
	temperatureProperty     = "temperature"
	topPProperty            = "topP"
	maxNewTokensProperty    = "maxNewTokens"
	systemPromptProperty    = "systemPrompt"
	avoidCommentaryProperty = "avoidCommentary"
)

var (
	DefaultContextualAIModel           = "v2"
	DefaultContextualAITemperature     = 0.0
	DefaultContextualAITopP            = 0.9
	DefaultContextualAIMaxNewTokens    = 1024
	DefaultContextualAISystemPrompt    = ""
	DefaultContextualAIAvoidCommentary = false
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

// NewClassSettings creates a new classSettings instance
func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-contextualai")}
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

func (ic *classSettings) getBoolProperty(name string, defaultValue bool) *bool {
	wrongVal := !defaultValue
	result := ic.propertyValuesHelper.GetPropertyAsBoolWithNotExists(ic.cfg, name, wrongVal, defaultValue)
	return &result
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultContextualAIModel)
}

func (ic *classSettings) Temperature() *float64 {
	return ic.getFloatProperty(temperatureProperty, &DefaultContextualAITemperature)
}

func (ic *classSettings) TopP() *float64 {
	return ic.getFloatProperty(topPProperty, &DefaultContextualAITopP)
}

func (ic *classSettings) MaxNewTokens() *int {
	return ic.getIntProperty(maxNewTokensProperty, &DefaultContextualAIMaxNewTokens)
}

func (ic *classSettings) SystemPrompt() string {
	return *ic.getStringProperty(systemPromptProperty, DefaultContextualAISystemPrompt)
}

func (ic *classSettings) AvoidCommentary() *bool {
	return ic.getBoolProperty(avoidCommentaryProperty, DefaultContextualAIAvoidCommentary)
}
