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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

const (
	apiEndpointProperty = "apiEndpoint"
	projectIDProperty   = "projectId"
	endpointIDProperty  = "endpointId"
	modelIDProperty     = "modelId"
	temperatureProperty = "temperature"
	tokenLimitProperty  = "tokenLimit"
	topPProperty        = "topP"
	topKProperty        = "topK"
)

var (
	DefaultPaLMApiEndpoint        = "us-central1-aiplatform.googleapis.com"
	DefaultPaLMModel              = "chat-bison"
	DefaultPaLMTemperature        = 0.2
	DefaultTokenLimit             = 256
	DefaultPaLMTopP               = 0.95
	DefaultPaLMTopK               = 40
	DefaulGenerativeAIApiEndpoint = "generativelanguage.googleapis.com"
	DefaulGenerativeAIModelID     = "chat-bison-001"
)

var supportedGenerativeAIModels = []string{
	DefaulGenerativeAIModelID,
	"gemini-pro",
}

type ClassSettings interface {
	Validate(class *models.Class) error
	// Module settings
	ApiEndpoint() string
	ProjectID() string
	EndpointID() string
	ModelID() string

	// parameters
	// 0.0 - 1.0
	Temperature() float64
	// 1 - 1024
	TokenLimit() int
	// 1 - 40
	TopK() int
	// 0.0 - 1.0
	TopP() float64
}

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) ClassSettings {
	return &classSettings{cfg: cfg}
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
	if tokenLimit < 1 || tokenLimit > 1024 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and 1024", tokenLimitProperty))
	}
	topK := ic.TopK()
	if topK < 1 || topK > 40 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and 40", topKProperty))
	}
	topP := ic.TopP()
	if topP < 0 || topP > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", topPProperty))
	}
	// Google MakerSuite
	model := ic.ModelID()
	if apiEndpoint == DefaulGenerativeAIApiEndpoint && !contains[string](supportedGenerativeAIModels, model) {
		errorMessages = append(errorMessages, fmt.Sprintf("%s is not supported available models are: %+v", model, supportedGenerativeAIModels))
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("generative-palm")[name]
	if ok {
		asString, ok := value.(string)
		if ok {
			return asString
		}
	}
	return defaultValue
}

func (ic *classSettings) getFloatProperty(name string, defaultValue float64) float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-palm")[name]
	if ok {
		asFloat, ok := val.(float64)
		if ok {
			return asFloat
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asFloat, _ := asNumber.Float64()
			return asFloat
		}
		asInt, ok := val.(int)
		if ok {
			asFloat := float64(asInt)
			return asFloat
		}
	}

	return defaultValue
}

func (ic *classSettings) getIntProperty(name string, defaultValue int) int {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	val, ok := ic.cfg.ClassByModuleName("generative-palm")[name]
	if ok {
		asFloat, ok := val.(float64)
		if ok {
			return int(asFloat)
		}
		asNumber, ok := val.(json.Number)
		if ok {
			asInt64, _ := asNumber.Int64()
			return int(asInt64)
		}
		asInt, ok := val.(int)
		if ok {
			return asInt
		}
	}

	return defaultValue
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		return DefaulGenerativeAIModelID
	}
	return DefaultPaLMModel
}

// PaLM params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultPaLMApiEndpoint)
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

// parameters

// 0.0 - 1.0
func (ic *classSettings) Temperature() float64 {
	return ic.getFloatProperty(temperatureProperty, DefaultPaLMTemperature)
}

// 1 - 1024
func (ic *classSettings) TokenLimit() int {
	return ic.getIntProperty(tokenLimitProperty, DefaultTokenLimit)
}

// 1 - 40
func (ic *classSettings) TopK() int {
	return ic.getIntProperty(topKProperty, DefaultPaLMTopK)
}

// 0.0 - 1.0
func (ic *classSettings) TopP() float64 {
	return ic.getFloatProperty(topPProperty, DefaultPaLMTopP)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
