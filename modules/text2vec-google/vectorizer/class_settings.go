//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	modelProperty       = "model"
	titleProperty       = "titleProperty"
	dimensionsProperty  = "dimensions"
	taskTypeProperty    = "taskType"
)

const (
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	DefaultApiEndpoint           = "us-central1-aiplatform.googleapis.com"
	DefaultModel                 = "gemini-embedding-001"
	DefaultAIStudioEndpoint      = "generativelanguage.googleapis.com"
	DefaulAIStudioModel          = "gemini-embedding-001"
	DefaultTaskType              = "RETRIEVAL_QUERY"
)

const GoogleCloudProjectEnv = "GOOGLE_CLOUD_PROJECT"

// default dimensions are set to 768 bc of being backward compatible with earlier models
// textembedding-gecko@001 and embedding-001 that were default ones
var DefaultDimensions int64 = 768

var defaultModelDimensions = map[string]*int64{
	"gemini-embedding-001": &DefaultDimensions,
}

var availableGoogleModels = []string{
	"textembedding-gecko@001",
	"textembedding-gecko@latest",
	"textembedding-gecko-multilingual@latest",
	"textembedding-gecko@003",
	"textembedding-gecko@002",
	"textembedding-gecko-multilingual@001",
	"textembedding-gecko@001",
	"text-embedding-preview-0409",
	"text-multilingual-embedding-preview-0409",
	DefaultModel,
	"text-embedding-005",
	"text-multilingual-embedding-002",
}

var availableGenerativeAIModels = []string{
	"embedding-001",
	"text-embedding-004",
	DefaulAIStudioModel,
	"text-embedding-005",
	"text-multilingual-embedding-002",
}

var availableTaskTypes = []string{
	DefaultTaskType,
	"QUESTION_ANSWERING",
	"FACT_VERIFICATION",
	"CODE_RETRIEVAL_QUERY",
	"CLASSIFICATION",
	"CLUSTERING",
	"SEMANTIC_SIMILARITY",
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
	model := ic.Model()
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

	if !slices.Contains(availableTaskTypes, ic.TaskType()) {
		errorMessages = append(errorMessages, fmt.Sprintf("wrong taskType supported task types are: %v", availableTaskTypes))
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
		return DefaulAIStudioModel
	}
	return DefaultModel
}

// Google params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(apiEndpointProperty, DefaultApiEndpoint)
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(projectIDProperty, os.Getenv(GoogleCloudProjectEnv, ""))
}

func (ic *classSettings) Model() string {
	if model := ic.getStringProperty(modelProperty, ""); model != "" {
		return model
	}
	return ic.getStringProperty(modelIDProperty, ic.getDefaultModel(ic.ApiEndpoint()))
}

func (ic *classSettings) TitleProperty() string {
	return ic.getStringProperty(titleProperty, "")
}

func (ic *classSettings) Dimensions() *int64 {
	return ic.GetPropertyAsInt64(dimensionsProperty, defaultModelDimensions[ic.Model()])
}

func (ic *classSettings) TaskType() string {
	return ic.getStringProperty(taskTypeProperty, DefaultTaskType)
}
