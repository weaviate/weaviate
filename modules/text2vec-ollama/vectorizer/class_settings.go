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

package vectorizer

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	apiEndpointProperty = "apiEndpoint"
	modelIDProperty     = "modelId"
)

const (
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultApiEndpoint           = "http://localhost:11434"
	DefaultModelID               = "nomic-embed-text"
)

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.BaseClassSettings.Validate(); err != nil {
		return err
	}
	if ic.ApiEndpoint() == "" {
		return errors.New("apiEndpoint cannot be empty")
	}
	if ic.ModelID() == "" {
		return errors.New("modelId cannot be empty")
	}
	return ic.BaseClassSettings.Validate()
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) getDefaultModel() string {
	return DefaultModelID
}

func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, ic.getDefaultModel())
}
