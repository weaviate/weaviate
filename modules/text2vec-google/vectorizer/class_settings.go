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
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
	DefaultApiEndpoint           = "us-central1-aiplatform.googleapis.com"
	DefaultModel                 = "gemini-embedding-001"
	DefaultAIStudioEndpoint      = "generativelanguage.googleapis.com"
	DefaulAIStudioModel          = "gemini-embedding-001"
	DefaultTaskType              = "RETRIEVAL_QUERY"

	// Parameter keys for accessing the Parameters map
	ParamApiEndpoint = "ApiEndpoint"
	ParamProjectID   = "ProjectID"
	ParamModel       = "Model"
	ParamTitle       = "TitleProperty"
	ParamDimensions  = "Dimensions"
	ParamTaskType    = "TaskType"
)

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

// Parameters defines all configuration parameters for text2vec-google
var Parameters = map[string]basesettings.ParameterDef{
	ParamApiEndpoint: {
		JSONKey:      "apiEndpoint",
		DefaultValue: DefaultApiEndpoint,
		Description:  "Google API endpoint",
		Required:     false,
	},
	ParamProjectID: {
		JSONKey:      "projectId",
		DefaultValue: "",
		Description:  "Google Cloud project ID; only required for non-AI Studio endpoints",
		Required:     false,
	},
	ParamModel: {
		JSONKey:       "model",
		AlternateKeys: []string{"modelId"},
		DefaultValue:  DefaultModel,
		Description:   "Google embedding model name; modelId is an alternate key for backward compatibility",
		Required:      false,
	},
	ParamTitle: {
		JSONKey:      "titleProperty",
		DefaultValue: "",
		Description:  "Title property for document embedding",
		Required:     false,
	},
	ParamDimensions: {
		JSONKey:      "dimensions",
		DefaultValue: nil,
		Description:  "Number of dimensions for the embedding",
		Required:     false,
	},
	ParamTaskType: {
		JSONKey:      "taskType",
		DefaultValue: DefaultTaskType,
		Description:  "Task type for the embedding",
		Required:     false,
	},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		cfg:               cfg,
		BaseClassSettings: *basesettings.NewBaseClassSettingsWithAltNames(cfg, LowerCaseInput, "text2vec-google", []string{"text2vec-palm"}, []string{"modelId"}),
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
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %v parameter, available AI Studio model names are: %v", Parameters[ParamModel].JSONKey, availableGenerativeAIModels))
		}
	} else {
		projectID := ic.ProjectID()
		if projectID == "" {
			errorMessages = append(errorMessages, fmt.Sprintf("%v cannot be empty", Parameters[ParamProjectID].JSONKey))
		}
		if model != "" && !ic.validateGoogleSetting(model, availableGoogleModels) {
			errorMessages = append(errorMessages, fmt.Sprintf("wrong %v parameter, available model names are: %v", Parameters[ParamModel].JSONKey, availableGoogleModels))
		}
	}

	if !slices.Contains(availableTaskTypes, ic.TaskType()) {
		errorMessages = append(errorMessages, fmt.Sprintf("wrong %v parameter, supported task types are: %v", Parameters[ParamTaskType].JSONKey, availableTaskTypes))
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

// getPropertyWithAlternates tries the primary JSONKey first, then falls back to AlternateKeys
func (ic *classSettings) getPropertyWithAlternates(paramKey string, defaultValue interface{}) interface{} {
	param := Parameters[paramKey]

	// Try primary key first
	if val := ic.getStringProperty(param.JSONKey, ""); val != "" {
		return val
	}

	// Fall back to alternate keys
	for _, altKey := range param.AlternateKeys {
		if val := ic.getStringProperty(altKey, ""); val != "" {
			return val
		}
	}

	return defaultValue
}

func (ic *classSettings) getDefaultModel(apiEndpoint string) string {
	if apiEndpoint == DefaultAIStudioEndpoint {
		return DefaulAIStudioModel
	}
	return DefaultModel
}

// Google params
func (ic *classSettings) ApiEndpoint() string {
	return ic.getStringProperty(Parameters[ParamApiEndpoint].JSONKey, DefaultApiEndpoint)
}

func (ic *classSettings) ProjectID() string {
	return ic.getStringProperty(Parameters[ParamProjectID].JSONKey, "")
}

func (ic *classSettings) Model() string {
	// Use dynamic default based on endpoint
	defaultModel := ic.getDefaultModel(ic.ApiEndpoint())
	return ic.getPropertyWithAlternates(ParamModel, defaultModel).(string)
}

func (ic *classSettings) TitleProperty() string {
	return ic.getStringProperty(Parameters[ParamTitle].JSONKey, "")
}

func (ic *classSettings) Dimensions() *int64 {
	return ic.GetPropertyAsInt64(Parameters[ParamDimensions].JSONKey, defaultModelDimensions[ic.Model()])
}

func (ic *classSettings) TaskType() string {
	return ic.getStringProperty(Parameters[ParamTaskType].JSONKey, DefaultTaskType)
}
