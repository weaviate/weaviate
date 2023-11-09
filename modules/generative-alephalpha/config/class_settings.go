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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"strconv"
)

var (
	DefaultAlephAlphaModel = "luminous-base-control"
	DefaultMaximumTokens   = 64
	DefaultTemperature     = 0.0
)

const (
	MaximumTokensKey = "maximum_tokens"
	ModelKey		 = "model"
)

var availableAlephAlphaModels = []string{
	"luminous-supreme-control",
	"luminous-extended",
	"luminous-base",
	"luminous-base-control",
	"luminous-extended-control",
	"luminous-supreme",
}

type ClassSettings interface {
	Model() string
	MaximumTokens() int
	Temperature() float64
	Validate(class *models.Class) error
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

	model := ic.getStringProperty(ModelKey, DefaultAlephAlphaModel)
	if model == nil || !ic.validateModel(*model) {
		return errors.Errorf("wrong Aleph Alpha model name, available model names are: %v", availableAlephAlphaModels)
	}

	maximumTokens := ic.getIntProperty(MaximumTokensKey, DefaultMaximumTokens)
	if maximumTokens == nil || !ic.validateMaximumTokens(*maximumTokens) {
		return errors.Errorf("wrong maximum tokens configuration, value is required and should be greater than 0")
	}

	temperature := ic.getFloatProperty("temperature", DefaultTemperature)
	if temperature == nil || !ic.validateTemperature(*temperature) {
		return errors.Errorf("wrong temperature configuration, value should be between 0.0 and 1.0")
	}

	fmt.Printf("validation is successful!")
	return nil
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(ModelKey, DefaultAlephAlphaModel)
}

func (ic *classSettings) MaximumTokens() int {
	return *ic.getIntProperty(MaximumTokensKey, DefaultMaximumTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty("temperature", DefaultTemperature)
}

func (ic *classSettings) validateModel(model string) bool {
	for _, availableModel := range availableAlephAlphaModels {
		if availableModel == model {
			return true
		}
	}
	return false
}

func (ic *classSettings) validateMaximumTokens(maximumTokens int) bool {

	return maximumTokens > 0

}

func (ic *classSettings) validateTemperature(temperature float64) bool {
	return temperature >= 0.0 && temperature <= 1.0
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	model, ok := ic.cfg.ClassByModuleName("generative-alephalpha")[name]
	if ok {
		asString, ok := model.(string)
		if ok {
			return &asString
		}
		var empty string
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) getIntProperty(name string, defaultValue int) *int {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("generative-alephalpha")[name]
	if ok {
		asInt, ok := value.(json.Number)
		if ok {
			casted, _ := strconv.Atoi(string(asInt))
			return &casted
		}
		var empty int
		return &empty
	}
	return &defaultValue
}

func (ic *classSettings) getFloatProperty(name string, defaultValue float64) *float64 {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return &defaultValue
	}

	value, ok := ic.cfg.ClassByModuleName("generative-alephalpha")[name]
	if ok {
		asFloat, ok := value.(json.Number)
		if ok {
			casted, _ := asFloat.Float64()
			return &casted
		}
		var empty float64
		return &empty
	}
	return &defaultValue
}
