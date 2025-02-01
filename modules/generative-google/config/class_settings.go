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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	apiEndpointProperty = "apiEndpoint"
	projectIDProperty   = "projectId"
	endpointIDProperty  = "endpointId"
	regionProperty      = "region"
	modelIDProperty     = "modelId"
	modelProperty       = "model"
	temperatureProperty = "temperature"
	tokenLimitProperty  = "tokenLimit"
	topPProperty        = "topP"
	topKProperty        = "topK"
)

var (
	DefaultGoogleApiEndpoint          = "us-central1-aiplatform.googleapis.com"
	DefaultGoogleModel                = "chat-bison"
	DefaultGoogleRegion               = "us-central1"
	DefaultGoogleTemperature          = 1.0
	DefaultTokenLimit                 = 1024
	DefaultTokenLimitGemini1_0        = 2048
	DefaultTokenLimitGemini1_0_Vision = 2048
	DefaultTokenLimitGemini1_5        = 8192
	DefaultGoogleTopP                 = 0.95
	DefaultGoogleTopK                 = 40
	DefaulGenerativeAIApiEndpoint     = "generativelanguage.googleapis.com"
	DefaulGenerativeAIModelID         = "chat-bison-001"
)

var supportedVertexAIModels = []string{
	DefaultGoogleModel,
	"chat-bison-32k",
	"chat-bison@002",
	"chat-bison-32k@002",
	"chat-bison@001",
	"gemini-1.5-pro-preview-0514",
	"gemini-1.5-pro-preview-0409",
	"gemini-1.5-flash-preview-0514",
	"gemini-1.0-pro-002",
	"gemini-1.0-pro-001",
	"gemini-1.0-pro",
}

var supportedGenerativeAIModels = []string{
	// chat-bison-001
	DefaulGenerativeAIModelID,
	"gemini-pro",
	"gemini-ultra",
	"gemini-1.5-flash-latest",
	"gemini-1.5-pro-latest",
}

type ClassSettings interface {
	Validate(class *models.Class) error
	// Module settings
	ApiEndpoint() string
	ProjectID() string
	EndpointID() string
	Region() string

	ModelID() string
	Model() string
	// parameters
	// 0.0 - 1.0
	Temperature() float64
	// 1 - 1024 / 2048 Gemini 1.0 / 8192 Gemini 1.5
	TokenLimit() int
	// 1 -
	TopK() int
	// 0.0 - 1.0
	TopP() float64
}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) ClassSettings {
	return &classSettings{
		cfg:                  cfg,
		propertyValuesHelper: basesettings.NewPropertyValuesHelperWithAltNames("generative-google", []string{"generative-palm"}),
	}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	var errorMessages []string

	apiEndpoint := ic.ApiEndpoint()
	projectID := ic.ProjectID()
	if apiEndpoint != DefaulGenerativeAIApiEndpoint && projectID == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", projectIDProperty))
	}
	temperature := ic.Temperature()
	if temperature < 0 || temperature > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", temperatureProperty))
	}
	tokenLimit := ic.TokenLimit()
	if tokenLimit < 1 || tokenLimit > ic.getDefaultTokenLimit(ic.ModelID()) {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and %v", tokenLimitProperty, ic.getDefaultTokenLimit(ic.ModelID())))
	}
	topK := ic.TopK()
	if topK < 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value above or equal 1", topKProperty))
	}
	topP := ic.TopP()
	if topP < 0 || topP > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", topPProperty))
	}
	// Google MakerSuite
	availableModels := append(supportedGenerativeAIModels, supportedVertexAIModels...)
	model := ic.ModelID()
	if apiEndpoint == DefaulGenerativeAIApiEndpoint && !contains(availableModels, model) {
		errorMessages = append(errorMessages, fmt.Sprintf("%s is not supported available models are: %+v", model, availableModels))
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, name, defaultValue)
}

func (ic *classSettings) getFloatProperty(name string, defaultValue float64) float64 {
	asFloat64 := ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, name, &defaultValue)
	return *asFloat64
}

func (ic *classSettings) getIntProperty(name string, defaultValue int) int {
	asInt := ic.propertyValuesHelper.GetPropertyAsInt(ic.cfg, name, &defaultValue)
	return *asInt
}

func (ic *classSettings) getApiEndpoint(projectID string) string {
	if projectID == "" {
		return DefaulGenerativeAIApiEndpoint
	}
	return DefaultGoogleApiEndpoint
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		return DefaulGenerativeAIModelID
	}
	return DefaultGoogleModel
}

func (ic *classSettings) getDefaultTokenLimit(model string) int {
	if strings.HasPrefix(model, "gemini-1.5") {
		return DefaultTokenLimitGemini1_5
	}
	if strings.HasPrefix(model, "gemini-1.0") || strings.HasPrefix(model, "gemini-pro") {
		if strings.Contains(model, "vision") {
			return DefaultTokenLimitGemini1_0_Vision
		}
		return DefaultTokenLimitGemini1_0
	}
	return DefaultTokenLimit
}

// Google params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, ic.getApiEndpoint(ic.ProjectID()))
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(projectIDProperty, "")
}

func (ic *classSettings) EndpointID() string {
	return ic.getStringProperty(endpointIDProperty, "")
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, ic.getDefaultModel(ic.ApiEndpoint()))
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, "")
}

func (ic *classSettings) Region() string {
	return ic.getStringProperty(regionProperty, DefaultGoogleRegion)
}

// parameters

// 0.0 - 1.0
func (ic *classSettings) Temperature() float64 {
	return ic.getFloatProperty(temperatureProperty, DefaultGoogleTemperature)
}

// 1 - 1024
func (ic *classSettings) TokenLimit() int {
	return ic.getIntProperty(tokenLimitProperty, ic.getDefaultTokenLimit(ic.ModelID()))
}

// 1 - 40
func (ic *classSettings) TopK() int {
	return ic.getIntProperty(topKProperty, DefaultGoogleTopK)
}

// 0.0 - 1.0
func (ic *classSettings) TopP() float64 {
	return ic.getFloatProperty(topPProperty, DefaultGoogleTopP)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
