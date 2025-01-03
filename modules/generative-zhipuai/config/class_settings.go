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
	modelProperty            = "model"
	temperatureProperty      = "temperature"
	maxTokensProperty        = "maxTokens" 
	topPProperty             = "topP"
	baseURLProperty          = "baseURL" 
)


var availableZhipuAIModels = []string{
	"glm-4",
	"glm-4-plus",
	"glm-4-air", 
	"glm-4-airx", 
	"glm-4-0520", 
	"glm-4-long",
	"glm-4-flashx",
	"glm-4-flash",
}

var (
	DefaultZhipuAIModel            = "glm-4"
	DefaultZhipuAITemperature      = 0.0
	DefaultZhipuAIMaxTokens        = defaultMaxTokens[DefaultZhipuAIModel] 
	DefaultZhipuAITopP             = 1.0
	DefaultZhipuAIBaseURL          = "https://open.bigmodel.cn/api/paas/v4" 
)

// todo Need to parse the tokenLimits in a smarter way, as the prompt defines the max length
var defaultMaxTokens = map[string]float64{ 

	"glm-4": 4095,
	"glm-4-plus": 4095,
	"glm-4-air": 4095,
	"glm-4-airx": 4095,
	"glm-4-0520": 4095,
	"glm-4-long": 4095,
	"glm-4-flashx": 4095,
	"glm-4-flash": 4095,
}
 

func GetMaxTokensForModel(model string) float64 {
	return defaultMaxTokens[model]
}
 
  

type ClassSettings interface {
	Model() string
	MaxTokens() float64
	Temperature() float64 
	TopP() float64  
	Validate(class *models.Class) error
	BaseURL() string
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) ClassSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-zhipuai")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	model := ic.getStringProperty(modelProperty, DefaultZhipuAIModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong ZhipuAI model name, available model names are: %v", availableZhipuAIModels)
	}

	temperature := ic.getFloatProperty(temperatureProperty, &DefaultZhipuAITemperature)
	if temperature == nil || (*temperature < 0 || *temperature > 1) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0")
	}

	maxTokens := ic.getFloatProperty(maxTokensProperty, &DefaultZhipuAIMaxTokens)
	if maxTokens == nil || (*maxTokens < 0 || *maxTokens > GetMaxTokensForModel(DefaultZhipuAIModel)) {
		return errors.Errorf("Wrong maxTokens configuration, values are should have a minimal value of 1 and max is dependant on the model used")
	}
 
	topP := ic.getFloatProperty(topPProperty, &DefaultZhipuAITopP)
	if topP == nil || (*topP < 0 || *topP > 5) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5")
	}


	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getBoolProperty(name string, defaultValue bool) *bool {
	asBool := ic.propertyValuesHelper.GetPropertyAsBool(ic.cfg, name, false)
	return &asBool
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	var wrongVal float64 = -1.0
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableZhipuAIModels, model)
}
 
func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultZhipuAIModel)
}

func (ic *classSettings) MaxTokens() float64 {
	return *ic.getFloatProperty(maxTokensProperty, &DefaultZhipuAIMaxTokens)
}

func (ic *classSettings) BaseURL() string {
	return *ic.getStringProperty(baseURLProperty, DefaultZhipuAIBaseURL)
}
 
func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultZhipuAITemperature)
}
 

func (ic *classSettings) TopP() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultZhipuAITopP)
} 
 
func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
