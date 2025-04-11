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
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"

	topPProperty = "topP"
	topKProperty = "topK"
)

var (
	DefaultDatabricksTemperature = 1.0
	DefaultDatabricksTopP        = 1.0
	DefaultDatabricksMaxTokens   = 8192.0
	DefaultDatabricksTopK        = math.MaxInt64
)

type ClassSettings interface {
	MaxTokens() *int
	Temperature() float64
	TopP() float64
	TopK() int
	Validate(class *models.Class) error
	Endpoint() string
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

	temperature := ic.getFloatProperty(temperatureProperty, &DefaultDatabricksTemperature)
	if temperature == nil || (*temperature < 0 || *temperature > 1) {
		return errors.Errorf("Wrong temperature configuration, values are between 0.0 and 1.0")
	}

	maxTokens := ic.getIntProperty(maxTokensProperty, nil)
	if maxTokens != nil && *maxTokens <= 0 {
		return errors.Errorf("Wrong maxTokens configuration, values should be greater than zero or nil")
	}

	topP := ic.getFloatProperty(topPProperty, &DefaultDatabricksTopP)
	if topP == nil || (*topP < 0 || *topP > 5) {
		return errors.Errorf("Wrong topP configuration, values are should have a minimal value of 1 and max of 5")
	}

	topK := ic.getIntProperty(topKProperty, &DefaultDatabricksTopK)
	if topK != nil && (*topK <= 0) {
		return errors.Errorf("Wrong topK configuration, values should be greater than zero or nil")
	}

	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) *string {
	asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, "", defaultValue)
	return &asString
}

func (ic *classSettings) getFloatProperty(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-1.0)
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) MaxTokens() *int {
	return ic.getIntProperty(maxTokensProperty, nil)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloatProperty(temperatureProperty, &DefaultDatabricksTemperature)
}

func (ic *classSettings) TopP() float64 {
	return *ic.getFloatProperty(topPProperty, &DefaultDatabricksTopP)
}

func (ic *classSettings) TopK() int {
	return *ic.getIntProperty(topKProperty, &DefaultDatabricksTopK)
}

func (ic *classSettings) Endpoint() string {
	return *ic.getStringProperty("endpoint", "")
}
