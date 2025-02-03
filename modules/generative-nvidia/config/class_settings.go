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
	baseURLProperty     = "baseURL"
	headerURLProperty   = "X-Nvidia-Baseurl"
	modelProperty       = "model"
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"
)

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                   = "https://integrate.api.nvidia.com/v1"
	DefaultNvidiaModel               = "meta/llama-3.2-3b-instruct"
	DefaultNvidiaTemperature float64 = 0
	DefaultNvidiaMaxTokens           = 2048
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-nvidia")}
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

func (ic *classSettings) getIntProperty(name string, defaultValue *int) *int {
	var wrongVal int = -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloat64Property(name string, defaultValue *float64) *float64 {
	var wrongVal float64 = -1
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultNvidiaMaxTokens
}

func (ic *classSettings) BaseURL() string {
	baseURL := *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
	if headerURL := *ic.getStringProperty(headerURLProperty, ""); headerURL != "" {
		baseURL = headerURL
	}
	return baseURL
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultNvidiaModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultNvidiaMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloat64Property(temperatureProperty, &DefaultNvidiaTemperature)
}
