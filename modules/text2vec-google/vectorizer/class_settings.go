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
	"slices"
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
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultApiEndpoint           = "us-central1-aiplatform.googleapis.com"
	DefaultModelID               = "textembedding-gecko@001"
	DefaultAIStudioEndpoint      = "generativelanguage.googleapis.com"
	DefaulAIStudioModelID        = "embedding-001"
)

var availableGoogleModels = []string{
	DefaultModelID,
	"textembedding-gecko@latest",
	"textembedding-gecko-multilingual@latest",
	"textembedding-gecko@003",
	"textembedding-gecko@002",
	"textembedding-gecko-multilingual@001",
	"textembedding-gecko@001",
	"text-embedding-preview-0409",
	"text-multilingual-embedding-preview-0409",
	"gemini-embedding-001",
	"text-embedding-005",
	"text-multilingual-embedding-002",
}

var availableGenerativeAIModels = []string{
	DefaulAIStudioModelID,
	"text-embedding-004",
	"gemini-embedding-001",
	"text-embedding-005",
	"text-multilingual-embedding-002",
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:               cfg,
		BaseClassSettings: *basesettings.NewBaseClassSettingsWithAltNames(cfg, false, "text2vec-google", []string{"text2vec-palm"}, []string{modelIDProperty}),
	}
}

func (ic *classSettings) Validate(class *models.Class) error {
	var errorMessages []string
	if err := ic.BaseClassSettings.Validate(class); err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	apiEndpoint := ic.ApiEndpoint()
	model := ic.ModelID()
	if apiEndpoint == DefaultAIStudioEndpoint {
		if model != "" && !ic.validateGoogleSetting(model, availableGenerativeAIModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available AI Studio model names are: %v", modelIDProperty, availableGenerativeAIModels))
		}
	} else {
		projectID := ic.ProjectID()
		if projectID == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%s cannot be empty", projectIDProperty))
		}
		if model != "" && !ic.validateGoogleSetting(model, availableGoogleModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %s available model names are: %v", modelIDProperty, availableGoogleModels))
		}
	}

	if len(errorMessages) > 0 {
		return fmt.Errorf("%s", strings.Join(errorMessages, ", "))
	}

	return nil
}

func (ic *classSettings) validateGoogleSetting(value string, availableValues []string) bool {
	return slices.Contains(availableValues, value)
}

func (ic *classSettings) getStringProperty(name, defaultValue string) string {
	return ic.BaseClassSettings.GetPropertyAsString(name, defaultValue)
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaultAIStudioEndpoint {
		return DefaulAIStudioModelID
	}
	return DefaultModelID
}

// Google params
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
