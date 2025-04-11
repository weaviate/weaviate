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
	headerURLProperty   = "X-Friendli-Baseurl"
	modelProperty       = "model"
	temperatureProperty = "temperature"
	maxTokensProperty   = "maxTokens"
)

// note it might not like this -- might want int values for e.g. MaxTokens
var (
	DefaultBaseURL                     = "https://inference.friendli.ai"
	DefaultFriendliModel               = "meta-llama-3.1-70b-instruct"
	DefaultFriendliTemperature float64 = 0
	DefaultFriendliMaxTokens           = 2048
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-friendliai")}
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
	wrongVal := -1
	return ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) getFloat64Property(name string, defaultValue *float64) *float64 {
	wrongVal := float64(-1)
	return ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, name, &wrongVal, defaultValue)
}

func (ic *classSettings) GetMaxTokensForModel(model string) int {
	return DefaultFriendliMaxTokens
}

func (ic *classSettings) BaseURL() string {
	baseURL := *ic.getStringProperty(baseURLProperty, DefaultBaseURL)
	if headerURL := *ic.getStringProperty(headerURLProperty, ""); headerURL != "" {
		baseURL = headerURL
	}
	return baseURL
}

func (ic *classSettings) Model() string {
	return *ic.getStringProperty(modelProperty, DefaultFriendliModel)
}

func (ic *classSettings) MaxTokens() int {
	return *ic.getIntProperty(maxTokensProperty, &DefaultFriendliMaxTokens)
}

func (ic *classSettings) Temperature() float64 {
	return *ic.getFloat64Property(temperatureProperty, &DefaultFriendliTemperature)
}
