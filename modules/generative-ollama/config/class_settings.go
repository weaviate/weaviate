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
	apiEndpointProperty = "apiEndpoint"
	modelProperty       = "model"
)

const (
	DefaultApiEndpoint = "http://localhost:11434"
	DefaultModel       = "llama3"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-ollama")}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	if ic.ApiEndpoint() == "" {
		return errors.New("apiEndpoint cannot be empty")
	}
	model := ic.Model()
	if model == "" {
		return errors.New("model cannot be empty")
	}
	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, name, defaultValue)
}

func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, DefaultModel)
}
