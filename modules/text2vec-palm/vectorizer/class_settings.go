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
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	apiEndpointProperty = "apiEndpoint"
	projectIDProperty   = "projectId"
	modelIDProperty     = "modelId"
	titleProperty       = "titleProperty"
)

const (
	DefaultVectorizeClassName     = false
	DefaultPropertyIndexed        = true
	DefaultVectorizePropertyName  = false
	DefaultApiEndpoint            = "us-central1-aiplatform.googleapis.com"
	DefaultModelID                = "textembedding-gecko@001"
	DefaulGenerativeAIApiEndpoint = "generativelanguage.googleapis.com"
	DefaulGenerativeAIModelID     = "embedding-gecko-001"
)

var availablePalmModels = []string{
	DefaultModelID,
	"textembedding-gecko@latest",
	"textembedding-gecko-multilingual@latest",
	"textembedding-gecko@003",
	"textembedding-gecko@002",
	"textembedding-gecko-multilingual@001",
	"textembedding-gecko@001",
}

var availableGenerativeAIModels = []string{
	DefaulGenerativeAIModelID,
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	var errorMessages []string
	if err := ic.BaseClassSettings.Validate(class); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	apiEndpoint := ic.ApiEndpoint()
	model := ic.ModelID()
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		if model != "" && !ic.validatePalmSetting(model, availableGenerativeAIModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available Generative AI model names are: %v", modelIDProperty, availableGenerativeAIModels))
		}
	} else {
		projectID := ic.ProjectID()
		if projectID == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", projectIDProperty))
		}
		if model != "" && !ic.validatePalmSetting(model, availablePalmModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available model names are: %v", modelIDProperty, availablePalmModels))
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validatePalmSetting(value string, availableValues []string) bool {
	for i := range availableValues {
		if value == availableValues[i] {
			return true
		}
	}
	return false
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaulGenerativeAIApiEndpoint {
		return DefaulGenerativeAIModelID
	}
	return DefaultModelID
}

// PaLM params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(projectIDProperty, "")
}

func (ic *classSettings) ModelID() string {
	return ic.getStringProperty(modelIDProperty, ic.getDefaultModel(ic.ApiEndpoint()))
}

func (ic *classSettings) TitleProperty() string {
	return ic.getStringProperty(titleProperty, "")
}
