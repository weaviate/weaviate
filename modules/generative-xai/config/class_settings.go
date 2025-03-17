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
	modelProperty       = "model"
	temperatureProperty = "temperature"
	topPProperty        = "topP"
	maxTokensProperty   = "maxTokens"
)

var (
	DefaultBaseURL  = "https://api.x.ai"
	DefaultXaiModel = "grok-2-latest"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-xai")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	return nil
}

func (ic *classSettings) BaseURL() string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, modelProperty, DefaultXaiModel)
}

func (ic *classSettings) Temperature() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, temperatureProperty, nil)
}

func (ic *classSettings) TopP() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, topPProperty, nil)
}

func (ic *classSettings) MaxTokens() *int {
	return ic.propertyValuesHelper.GetPropertyAsInt(ic.cfg, maxTokensProperty, nil)
}
