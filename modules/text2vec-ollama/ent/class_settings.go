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

package ent

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
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultApiEndpoint           = "http://localhost:11434"
	DefaultModel                 = "nomic-embed-text"
	LowerCaseInput               = false
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, LowerCaseInput)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.BaseClassSettings.Validate(class); err != nil {
		return err
	}
	if ic.ApiEndpoint() == "" {
		return errors.New("apiEndpoint cannot be empty")
	}
	if ic.Model() == "" {
		return errors.New("model cannot be empty")
	}
	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty(modelProperty, DefaultModel)
}
