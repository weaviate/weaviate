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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"

	topPProperty = "topP"
)

var availableOpenAILegacyModels = []string{
	"text-davinci-002",
	"text-davinci-003",
}

var availableOpenAIModels = []string{
	"gpt-3.5-turbo",
	"gpt-3.5-turbo-16k",
	"gpt-3.5-turbo-1106",
	"gpt-4",
	"gpt-4-32k",
	"gpt-4-1106-preview",
}

var (
	DefaultOpenAIModel            = "gpt-3.5-turbo"
	DefaultOpenAITemperature      = 0.0
	DefaultOpenAIMaxTokens        = defaultMaxTokens[DefaultOpenAIModel]
	DefaultOpenAIFrequencyPenalty = 0.0
	DefaultOpenAIPresencePenalty  = 0.0
	DefaultOpenAITopP             = 1.0
	DefaultOpenAIBaseURL          = "https://api.openai.com"
	DefaultApiVersion             = "2024-02-01"
)

// todo Need to parse the tokenLimits in a smarter way, as the prompt defines the max length
var defaultMaxTokens = map[string]float64{
	"text-davinci-002":   4097,
	"text-davinci-003":   4097,
	"gpt-3.5-turbo":      4097,
	"gpt-3.5-turbo-16k":  16384,
	"gpt-3.5-turbo-1106": 16385,
	"gpt-4":              8192,
	"gpt-4-32k":          32768,
	"gpt-4-1106-preview": 128000,
}

var availableApiVersions = []string{
	"2022-12-01",
	"2023-03-15-preview",
	"2023-05-15",
	"2023-06-01-preview",
	"2023-07-01-preview",
	"2023-08-01-preview",
	"2023-09-01-preview",
	"2023-12-01-preview",
	"2024-02-15-preview",
	"2024-03-01-preview",
	"2024-02-01",
}

type ClassSettings interface {
	MaxTokens() float64
	Temperature() float64
	TopP() float64
	ResourceName() string
	DeploymentID() string
	IsAzure() bool
	GetMaxTokensForModel(model string) float64
	Validate(class *models.Class) error
	ServingURL() string
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) ClassSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-databricks")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	temperature := ic.getFloatProperty(temperatureProperty, &DefaultOpenAITemperature)
	if temperature == nil || (*temperature < 0 || *temperature > 1) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0")
	}

	maxTokens := ic.getFloatProperty(maxTokensProperty, &DefaultOpenAIMaxTokens)
	if maxTokens != nil && *maxTokens <= 0 {
		return errors.Errorf("Wrong maxTokens configuration, values should be greater than zero or nil")
	}

	topP := ic.getFloatProperty(topPProperty, &DefaultOpenAITopP)
	if topP == nil || (*topP < 0 || *topP > 5) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5")
	}

	servingURL := ic.ServingURL()
	err := ic.ValidateServingURL(servingURL)
	if err != nil {
		return err
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	var wrongVal float64 = -1.0
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) GetMaxTokensForModel(model string) float64 {
	return defaultMaxTokens[model]
}

func (ic *classSettings) validateModel(model string) bool {
	return contains(availableOpenAIModels, model) || contains(availableOpenAILegacyModels, model)
}

func (ic *classSettings) validateApiVersion(apiVersion string) bool {
	return contains(availableApiVersions, apiVersion)
}

func (ic *classSettings) MaxTokens() float64 {
	return *ic.getFloatProperty(maxTokensProperty, &DefaultOpenAIMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultOpenAITemperature)
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

func (ic *classSettings) ServingURL() string {
	return *ic.getStringProperty("servingUrl", "")
}

func (ic *classSettings) validateAzureConfig(resourceName string, deploymentId string) error {
	if (resourceName == "" && deploymentId != "") || (resourceName != "" && deploymentId == "") {
		return fmt.Errorf("both resourceName and deploymentId must be provided")
	}
	return nil
}

func (cs *classSettings) ValidateServingURL(servingUrl string) error {
	if servingUrl == "" {
		return errors.New("servingUrl cannot be empty")
	}
	return nil
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
