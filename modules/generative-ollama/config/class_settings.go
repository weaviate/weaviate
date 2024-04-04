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
	modelIDProperty     = "modelId"
)

const (
	DefaultApiEndpoint = "http://localhost:11434"
	DefaultModelID     = "llama2"
)

var availableOllamaModels = []string{
	DefaultModelID,
	"mistral",
	"dolphin-phi",
	"phi",
	"neural-chat",
	"starling-lm",
	"codellama",
	"llama2-uncensored",
	"llama2:13b",
	"llama2:70b",
	"orca-mini",
	"vicuna",
	"llava",
	"gemma:2b",
	"gemma:7b",
}

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
	model := ic.ModelID()
	if model == "" || !contains(availableOllamaModels, model) {
		return errors.Errorf("wrong Ollama model name, available model names are: %v", availableOllamaModels)
	}
	return nil
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, name, defaultValue)
}

func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, DefaultModelID)
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
