//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
	serviceProperty       = "service"
	regionProperty        = "region"
	modelProperty         = "model"
	maxTokenCountProperty = "maxTokenCount"
	stopSequencesProperty = "stopSequences"
	temperatureProperty   = "temperature"
	topPProperty          = "topP"
)

var (
	DefaultTitanMaxTokens     = 8192
	DefaultTitanStopSequences = []string{}
	DefaultTitanTemperature   = 0.0
	DefaultTitanTopK          = 1
)

var availableAWSModels = []string{
	"amazon.titan-tg1-large",
}

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	var errorMessages []string

	service := ic.Service()
	if service == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", serviceProperty))
	}
	region := ic.Region()
	if region == "" {
		errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", regionProperty))
	}
	model := ic.Model()
	if model == "" && !ic.validateAWSSetting(model, availableAWSModels) {
		errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available model names are: %v", modelProperty, availableAWSModels))
	}

	maxTokenCount := ic.MaxTokenCount()
	if maxTokenCount < 1 || maxTokenCount > 8192 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 1 and 8096", maxTokenCountProperty))
	}
	temperature := ic.Temperature()
	if temperature < 0 || temperature > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be float value between 0 and 1", temperatureProperty))
	}
	topP := ic.TopP()
	if topP < 0 || topP > 1 {
		errorMessages = append(errorMessages, fmt.Sprintf("%s has to be an integer value between 0 and 1", topPProperty))
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validateAWSSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
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

	val, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
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

	val, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
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

func (ic *classSettings) getListOfStringsProperty(name string, defaultValue []string) *[]string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-aws")[name]
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

// AWS params
func (ic *classSettings) Service() string {
	return ic.getStringProperty(serviceProperty, "")
}

func (ic *classSettings) Region() string {
	return ic.getStringProperty(regionProperty, "")
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, "")
}

func (ic *classSettings) MaxTokenCount() int {
	return ic.getIntProperty(maxTokenCountProperty, DefaultTitanMaxTokens)
}

func (ic *classSettings) StopSequences() []string {
	return *ic.getListOfStringsProperty(stopSequencesProperty, DefaultTitanStopSequences)
}

func (ic *classSettings) Temperature() float64 {
	return ic.getFloatProperty(temperatureProperty, DefaultTitanTemperature)
}

func (ic *classSettings) TopP() int {
	return ic.getIntProperty(topPProperty, DefaultTitanTopK)
}
